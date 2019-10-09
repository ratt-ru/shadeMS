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


def make_plot(data,xmin,xmax,ymin,ymax,xlabel,ylabel,title,pngname,figx=24,figy=12,doblack=False):
    fig = pylab.figure(figsize=(figx,figy))
    if doblack:
        ax = fig.add_subplot(111,facecolor='black')
    else:
        ax = fig.add_subplot(111)
    ax.imshow(X=data,extent=[xmin,xmax,ymin,ymax],aspect='auto',origin='upper')
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.savefig(pngname,bbox_inches='tight')
    return pngname


def now():
    stamp = time.strftime('[%Y-%m-%d %H:%M:%S]: ')
    # msg = '\033[92m'+stamp+'\033[0m' # time in green
    msg = stamp+' '
    return msg


def blank():
    print('%s' % now())


def fullname(shortname):
    fullnames = [('a','Amplitude'),
        ('p','Phase'),
        ('r','Real'),
        ('i','Imaginary'),
        ('t','Time'),
        ('c','Channel'),
        ('f','Frequency'),
        ('uv','uv-distance')]
    for xx in fullnames:
        if xx[0] == shortname:
            fullname = xx[1]
    return fullname


def main():


    clock_start = time.time()


    # Command line options

    parser = OptionParser(usage='%prog [options] ms')
    parser.add_option('--xaxis',dest='xaxis',help='[t] (default), [f]requency, [c]hannels, [uv]distance, [r]eal',default='t')
    parser.add_option('--yaxis',dest='yaxis',help='[a]mplitude (default), [p]hase, [r]eal, [i]maginary',default='a')
    parser.add_option('--col',dest='col',help='Measurement Set column to plot (default = DATA)',default='DATA')
    parser.add_option('--field',dest='myfields',help='Field ID(s) to plot (comma separated list, default = all)',default='all')
    parser.add_option('--spws',dest='myspws',help='Spectral windows (DDIDs) to plot (comma separated list, default = all)',default='all')
    parser.add_option('--corr',dest='corr',help='Correlation index to plot (default = 0)',default=0)
    parser.add_option('--noflags',dest='noflags',help='Plot flagged data (default = False)',action='store_true',default=False)
    parser.add_option('--xmin',dest='xmin',help='Minimum x-axis value (default = data min)',default='')
    parser.add_option('--xmax',dest='xmax',help='Maximum x-axis value (default = data max)',default='')
    parser.add_option('--ymin',dest='ymin',help='Minimum y-axis value (default = data min)',default='')
    parser.add_option('--ymax',dest='ymax',help='Maximum y-axis value (default = data max)',default='')
    parser.add_option('--xcanvas',dest='xcanvas',help='Canvas x-size in pixels (default = 1280)',default=1280)
    parser.add_option('--ycanvas',dest='ycanvas',help='Canvas y-size in pixels (default = 800)',default=800)
    parser.add_option('--norm',dest='normalize',help='Pixel scale normalization: eq_hist (default), cbrt, log, linear',default='eq_hist')
    parser.add_option('--cmap',dest='mycmap',help='Colorcet map to use (default = bkr)',default='bkr')
    parser.add_option('--doblack',dest='doblack',help='Set plot background to black (default = False)',action='store_true',default=False)
    parser.add_option('--png',dest='pngname',help='PNG name (default = something very verbose)',default='')


    # Assign inputs

    (options,args) = parser.parse_args()
    xaxis = (options.xaxis).lower()
    yaxis = (options.yaxis).lower()
    col = options.col.upper()
    myfields = options.myfields
    corr = int(options.corr)
    myspws = options.myspws
    noflags = options.noflags
    xmin = options.xmin
    xmax = options.xmax
    ymin = options.ymin
    ymax = options.ymax
    xcanvas = int(options.xcanvas)
    ycanvas = int(options.ycanvas)
    normalize = options.normalize
    mycmap = options.mycmap
    doblack = options.doblack
    pngname = options.pngname


    # Trap no MS

    if len(args) != 1:
        print('Please specify a Measurement Set to plot')
        sys.exit()
    else:
        myms = args[0].rstrip('/')

    # Check for allowed axes

    allowed = ['a','p','r','i','t','f','c','uv']
    if xaxis not in allowed or yaxis not in allowed:
        print('Please check requested axes')
        sys.exit()

    print('%sPlotting %s vs %s' % (now(),fullname(yaxis),fullname(xaxis)))
    print('%sCorrelation index %s' % (now(),str(corr)))


    # Get MS data

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


    # Get plot data into a pair of numpy arrays

    for group in msdata:
        nrows = group.VISDATA.values.shape[0]
        nchan = group.VISDATA.values.shape[1]
        fld = group.FIELD_ID
        ddid = group.DATA_DESC_ID
        if fld in fields and ddid in spws:
            chans = chan_freqs.values[ddid]
            flags = numpy.append(flags,group.FLAG.values[:,:,corr])

            if yaxis == 'a':
                ydata = numpy.append(ydata,numpy.abs(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'p':
                ydata = numpy.append(ydata,numpy.angle(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'r':
                ydata = numpy.append(ydata,numpy.real(group.VISDATA.values[:,:,corr]))
            elif yaxis == 'i':
                ydata = numpy.append(ydata,numpy.imag(group.VISDATA.values[:,:,corr]))

            if xaxis == 'f':
                xdata = numpy.append(xdata,numpy.tile(chans,nrows))
            elif xaxis == 'c':
                xdata = numpy.append(xdata,numpy.tile(numpy.arange(nchan),nrows))
            elif xaxis == 't':
                xdata = numpy.append(xdata,numpy.repeat(group.TIME.values,nchan))
            elif xaxis == 'uv':
                uu = group.UVW.values[:,0]
                vv = group.UVW.values[:,1]
                uvdist = ((uu**2.0)+(vv**2.0))**0.5
                xdata = numpy.append(xdata,numpy.repeat(uvdist,nchan))
            elif xaxis == 'r':
                xdata = numpy.append(xdata,numpy.real(group.VISDATA.values[:,:,corr]))


    # Drop flagged data if required

    if not noflags:

        bool_flags = list(map(bool,flags))

        masked_ydata = numpy.ma.masked_array(data=ydata,mask=bool_flags)
        masked_xdata = numpy.ma.masked_array(data=xdata,mask=bool_flags)

        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()


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
    #img = tf.set_background(tf.shade(agg, cmap=colorcet.dimgray,how='log'),"black")
        

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

    ylabel = fullname(yaxis)
    xlabel = fullname(xaxis) # Add t = t - t[0] and make it relative
    title = myms+' '+col+' (correlation '+str(corr)+')'
    if pngname == '':
        pngname = 'plot_'+myms.split('/')[-1]+'_'+col+'_'
        pngname += 'SPW-'+myspws.replace(',','-')+'_FIELD-'+myfields.replace(',','-')+'_'
        pngname += fullname(yaxis)+'_vs_'+fullname(xaxis)+'_'+'corr'+str(corr)+'.png'    


    # Render the plot

    print('%sRendering plot' % now())

    make_plot(img.data,xmin,xmax,ymin,ymax,xlabel,ylabel,title,pngname,figx=xcanvas/60,figy=ycanvas/60,doblack=doblack)


    # Stop the clock

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start),2))

    print ('%sDone. Elapsed time: %s seconds.' % (now(),elapsed))



if __name__ == "__main__":


    main()
