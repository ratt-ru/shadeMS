#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk


import matplotlib
matplotlib.use('agg')


from collections import OrderedDict as odict
import colorcet
import dask.dataframe as dd
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
from optparse import OptionParser
import pandas as pd
import pylab
import sys
import time
import xarray as xa
import xarrayms as xms


def get_chan_freqs(myms):
    spw_tab = xms.xds_from_table(myms+'::SPECTRAL_WINDOW',columns=['CHAN_FREQ'])
    chan_freqs = spw_tab[0].CHAN_FREQ
    return chan_freqs


def get_field_names(myms):
    field_tab = xms.xds_from_table(myms+'::FIELD',columns=['NAME','SOURCE_ID'])
    field_ids = field_tab[0].SOURCE_ID.values
    field_names = field_tab[0].NAME.values
    return field_ids,field_names


def freq_to_wavel(ff):
    c = 299792458.0 # m/s
    return c/ff


def make_plot(data,xmin,xmax,ymin,ymax,xlabel,ylabel,title,pngname,bgcol,fontsize,figx=24,figy=12):

    def match(artist):
        return artist.__module__ == 'matplotlib.text'

    fig = pylab.figure(figsize=(figx,figy))
    ax = fig.add_subplot(111,facecolor=bgcol)
    ax.imshow(X=data,extent=[xmin,xmax,ymin,ymax],aspect='auto',origin='upper')
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(fontsize)
    fig.savefig(pngname,bbox_inches='tight')
    return pngname


def now():
    # stamp = time.strftime('[%Y-%m-%d %H:%M:%S]: ')
    # msg = '\033[92m'+stamp+'\033[0m' # time in green
    stamp = time.strftime(' [%H:%M:%S] ')
    msg = stamp+' '
    return msg


def stamp():
    return str(time.time()).replace('.','')


def blank():
    print('%s' % now())


def fullname(shortname):
    fullnames = [('a','Amplitude',''),
        ('p','Phase','[rad]'),
        ('r','Real',''),
        ('i','Imaginary',''),
        ('t','Time','[s]'),
        ('c','Channel',''),
        ('f','Frequency','[Hz]'),
        ('uv','uv-distance','[wavelengths]'),
        ('u','u','[wavelengths]'),
        ('v','v','[wavelengths]')]
    for xx in fullnames:
        if xx[0] == shortname:
            fullname = xx[1]
            units = xx[2]
    return fullname,units


def main():


    clock_start = time.time()


    # Command line options

    parser = OptionParser(usage='%prog [options] ms')
    parser.add_option('--xaxis',dest='xaxis',help='[t]ime (default), [f]requency, [c]hannels, [u], [uv]distance, [r]eal, [a]mplitude',default='t')
    parser.add_option('--yaxis',dest='yaxis',help='[a]mplitude (default), [p]hase, [r]eal, [i]maginary, [v]',default='a')
    parser.add_option('--col',dest='col',help='Measurement Set column to plot (default = DATA)',default='DATA')
    parser.add_option('--field',dest='myfields',help='Field ID(s) to plot (comma separated list, default = all)',default='all')
    parser.add_option('--spw',dest='myspws',help='Spectral windows (DDIDs) to plot (comma separated list, default = all)',default='all')
#    parser.add_option('--scan',dest='myscans',help='Scan numbers to plot (comma separated list, default = all)',default='all')
    parser.add_option('--corr',dest='corr',help='Correlation index to plot (default = 0)',default=0)
    parser.add_option('--noflags',dest='noflags',help='Plot flagged data (default = False)',action='store_true',default=False)
    parser.add_option('--noconj',dest='noconj',help='Do not show conjugate points in u,v plots (default = plot conjugates)',action='store_true',default=False)
    parser.add_option('--xmin',dest='xmin',help='Minimum x-axis value (default = data min)',default='')
    parser.add_option('--xmax',dest='xmax',help='Maximum x-axis value (default = data max)',default='')
    parser.add_option('--ymin',dest='ymin',help='Minimum y-axis value (default = data min)',default='')
    parser.add_option('--ymax',dest='ymax',help='Maximum y-axis value (default = data max)',default='')
    parser.add_option('--xcanvas',dest='xcanvas',help='Canvas x-size in pixels (default = 1280)',default=1280)
    parser.add_option('--ycanvas',dest='ycanvas',help='Canvas y-size in pixels (default = 800)',default=900)
    parser.add_option('--norm',dest='normalize',help='Pixel scale normalization: eq_hist (default), cbrt, log, linear',default='eq_hist')
    parser.add_option('--cmap',dest='mycmap',help='Colorcet map to use (default = bkr)',default='bkr')
    parser.add_option('--bgcol',dest='bgcol',help='RGB hex code for background colour (default = FFFFFF)',default='FFFFFF')
    parser.add_option('--fontsize',dest='fontsize',help='Font size for all text elements (default = 20)',default=20)
    parser.add_option('--png',dest='pngname',help='PNG name (default = something verbose)',default='')
    parser.add_option('--prefix',dest='prefix',help='Prefix for default PNG name (default = plot)',default='plot')
    parser.add_option('--stamp',dest='dostamp',help='Add timestamp to default PNG name',action='store_true',default=False)


    # Assign inputs

    (options,args) = parser.parse_args()
    xaxis = (options.xaxis).lower()
    yaxis = (options.yaxis).lower()
    col = options.col.upper()
    myfields = options.myfields
    corr = int(options.corr)
    myspws = options.myspws
#    myscans = options.myscans
    noflags = options.noflags
    noconj = options.noconj
    xmin = options.xmin
    xmax = options.xmax
    ymin = options.ymin
    ymax = options.ymax
    xcanvas = int(options.xcanvas)
    ycanvas = int(options.ycanvas)
    normalize = options.normalize
    mycmap = options.mycmap
    bgcol = '#'+options.bgcol.lstrip('#')
    fontsize = options.fontsize
    pngname = options.pngname
    prefix = options.prefix
    dostamp = options.dostamp


    # Trap no MS

    if len(args) != 1:
        print('Please specify a Measurement Set to plot')
        sys.exit()
    else:
        myms = args[0].rstrip('/')

    # Check for allowed axes

    allowed = ['a','p','r','i','t','f','c','uv','u','v']
    if xaxis not in allowed or yaxis not in allowed:
        print('Please check requested axes')
        sys.exit()


    xfullname,xunits = fullname(xaxis)
    yfullname,yunits = fullname(yaxis)


    print('%sPlotting %s vs %s' % (now(),yfullname,xfullname))
    print('%sCorrelation index %s' % (now(),str(corr)))


    # Get MS metadata

    chan_freqs = get_chan_freqs(myms)
    field_ids,field_names = get_field_names(myms)


    # Sort out field selection(s)

    if myfields == 'all':
        fields = field_ids  
        # fields = []
        # for group in msdata:
        #   fields.append(group.FIELD_ID)
        # fields = numpy.unique(fields)
    else:
        fields = list(map(int, myfields.split(',')))

    blank()
    print('%sFIELD_ID   NAME' % now())
    for i in fields:
        print('%s%-10s %-16s' % (now(),i,field_names[i]))


    # Sort out SPW selection(s)

    if myspws == 'all':
        spws = numpy.arange(len(chan_freqs))
    else:
        spws = list(map(int, myspws.split(',')))

    blank()
    print('%sSPW_ID     NCHAN ' % now())
    for i in spws:
        nchan = len(chan_freqs.values[i])
        print('%s%-10s %-16s' % (now(),i,nchan))

    blank()


    # Construct TaQL string based on FIELD and SPW selections

    field_taq = []
    for fld in fields:
        field_taq.append('FIELD_ID=='+str(fld))

    spw_taq = []
    for spw in spws:
        spw_taq.append('DATA_DESC_ID=='+str(spw))

    mytaql = '('+' || '.join(field_taq)+') && ('+' || '.join(spw_taq)+')'


    # Read the selected data

    print('%sReading %s' % (now(),myms))
    print('%s%s column' % (now(),col))

    msdata = xms.xds_from_ms(myms,columns=[col,'TIME','FLAG','FIELD_ID','UVW'],taql_where=mytaql)


    # Replace xarray data with a,p,r,i in situ

    print('%sRearranging the deck chairs' % now())

    for i in range(0,len(msdata)):
        msdata[i] = msdata[i].rename({col:'VISDATA'})


    # Initialise arrays for plot data

    ydata = numpy.array(())
    xdata = numpy.array(())
    flags = numpy.array(())
    t0 = False


    # Get plot data into a pair of numpy arrays

    for group in msdata:
        nrows = group.VISDATA.values.shape[0]
        nchan = group.VISDATA.values.shape[1]
        fld = group.FIELD_ID
        ddid = group.DATA_DESC_ID
        if fld in fields and ddid in spws:
            chans = chan_freqs.values[ddid]
            flags = numpy.append(flags,group.FLAG.values[:,:,corr])


            if xaxis == 'uv' or xaxis == 'u' or yaxis == 'v':
                uu = group.UVW.values[:,0]
                vv = group.UVW.values[:,1]
                chans_wavel = freq_to_wavel(chans)
                uu_wavel = numpy.ravel(uu / numpy.transpose(numpy.array([chans_wavel,]*len(uu))))
                vv_wavel = numpy.ravel(vv / numpy.transpose(numpy.array([chans_wavel,]*len(vv))))
                uvdist_wavel = ((uu_wavel**2.0)+(vv_wavel**2.0))**0.5


            if yaxis == 'a':
                ydata = numpy.append(ydata,numpy.abs(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'p':
                ydata = numpy.append(ydata,numpy.angle(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'r':
                ydata = numpy.append(ydata,numpy.real(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'i':
                ydata = numpy.append(ydata,numpy.imag(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'v':
                ydata = uu_wavel

            if xaxis == 'f':
                xdata = numpy.append(xdata,numpy.tile(chans,nrows))
            elif xaxis == 'c':
                xdata = numpy.append(xdata,numpy.tile(numpy.arange(nchan),nrows))
            elif xaxis == 't':
                # Add t = t - t[0] and make it relative
                xdata = numpy.append(xdata,numpy.repeat(group.TIME.values,nchan))
                t0 = xdata[0]
                xdata = xdata - t0
            elif xaxis == 'uv':
                xdata = uvdist_wavel
            elif xaxis == 'r':
                xdata = numpy.append(xdata,numpy.real(group.VISDATA.values[:,:,corr]))
            elif xaxis == 'u':
                xdata = vv_wavel
            elif xaxis == 'a':
                xdata = numpy.append(xdata,numpy.abs(group.VISDATA.values[:,:,corr]))



    # Drop flagged data if required

    if not noflags:

        bool_flags = list(map(bool,flags))

        masked_ydata = numpy.ma.masked_array(data=ydata,mask=bool_flags)
        masked_xdata = numpy.ma.masked_array(data=xdata,mask=bool_flags)

        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()


    # Plot the conjugate points for a u,v plot if requested
    # This is done at this stage so we don't have to worry about the flags

    if not noconj and xaxis == 'u' and yaxis == 'v':
        xdata = numpy.append(xdata,xdata*-1.0)
        ydata = numpy.append(ydata,ydata*-1.0)


    # Drop data out of plot range(s)

    if xmin != '':
        xmin = float(xmin)
        masked_xdata = numpy.ma.masked_less(xdata,xmin)
        masked_ydata = numpy.ma.masked_array(data=ydata,mask=masked_xdata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if xmax != '':
        xmax = float(xmax)
        masked_xdata = numpy.ma.masked_greater(xdata,xmax)
        masked_ydata = numpy.ma.masked_array(data=ydata,mask=masked_xdata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if ymin != '':
        ymin = float(ymin)
        masked_ydata = numpy.ma.masked_less(ydata,ymin)
        masked_xdata = numpy.ma.masked_array(data=xdata,mask=masked_ydata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if ymax != '':
        ymax = float(ymax)
        masked_ydata = numpy.ma.masked_greater(ydata,ymax)
        masked_xdata = numpy.ma.masked_array(data=xdata,mask=masked_ydata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()


    # Put plotdata into pandas data frame
    # This should be possible with xarray directly, but for freq plots we need a corner turn

    print('%sMaking Pandas dataframe' % now())

    dists = {'plotdata': pd.DataFrame(odict([(xaxis,xdata),(yaxis,ydata)]))}
    df = pd.concat(dists,ignore_index=True)


    # Run datashader on the pandas df

    print('%sRunning datashader' % now())

    canvas = ds.Canvas(xcanvas,ycanvas)
    agg = canvas.points(df,xaxis,yaxis)
    img = hd.shade(hv.Image(agg),cmap=getattr(colorcet,mycmap),normalization=normalize)
        

    # Set plot limits based on data extent or user values for axis labels

    if ymin == '':
        ymin = numpy.min(agg.coords[yaxis].values)
    else:
        ymin = float(ymin)
    if ymax == '':
        ymax = numpy.max(agg.coords[yaxis].values)
    else:
        ymax = float(ymax)
    if xmin == '':
        xmin = numpy.min(agg.coords[xaxis].values)
    else:
        xmin = float(xmin)
    if xmax == '':
        xmax = numpy.max(agg.coords[xaxis].values)
    else:
        xmax = float(xmax)


    # Setup plot labels and PNG name

    ylabel = yfullname+' '+yunits
    xlabel = xfullname+' '+xunits 
    if t0:
        xlabel += ' relative to '+str(t0)
    title = myms+' '+col+' (correlation '+str(corr)+')'
    if pngname == '':
        pngname = prefix+'_'+myms.split('/')[-1]+'_'+col+'_'
        pngname += 'SPW-'+myspws.replace(',','-')+'_FIELD-'+myfields.replace(',','-')+'_'
#        pngname += 'SCAN-'+myscans.replace(',','-')+'_'
        pngname += yfullname+'_vs_'+xfullname+'_'+'corr'+str(corr)
        if dostamp:
            pngname += '_'+stamp()
        pngname += '.png'    


    # Render the plot

    print('%sRendering plot' % now())

    make_plot(img.data,
        xmin,
        xmax,
        ymin,
        ymax,
        xlabel,
        ylabel,
        title,
        pngname,
        bgcol,
        fontsize,
        figx=xcanvas/60,
        figy=ycanvas/60)


    # Stop the clock

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start),2))

    print ('%sDone. Elapsed time: %s seconds.' % (now(),elapsed))



if __name__ == "__main__":


    main()
