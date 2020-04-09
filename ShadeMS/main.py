#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk


import matplotlib
matplotlib.use('agg')


from argparse import ArgumentParser
import ShadeMS
from ShadeMS import shadeMS as sms
import time
import numpy
import daskms as xms
#import dask.dataframe as dd
import pkg_resources


log = ShadeMS.log


try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"


def main(argv):


    clock_start = time.time()


    # ---------------------------------------------------------------------------------------------------------------------------------------------
    # Setup command line options


    parser = ArgumentParser(description='Rapid Measurement Set plotting with dask-ms and datashader. Version {0:s}'.format(__version__))


    parser.add_argument('ms', 
                      help='Measurement set')
    parser.add_argument("-v", "--version", action='version',
                      version='{:s} version {:s}'.format(parser.prog, __version__))


    data_opts = parser.add_argument_group('Data selection')
    data_opts.add_argument('--xaxis', dest='xaxis',
                      help='[t]ime (default), [f]requency, [c]hannels, [u], [uv]distance, [r]eal, [a]mplitude', default='t')
    data_opts.add_argument('--yaxis', dest='yaxis',
                      help='[a]mplitude (default), [p]hase, [r]eal, [i]maginary, [v]', default='a')
    data_opts.add_argument('--col', dest='col',
                      help='Measurement Set column to plot (default = DATA)', default='DATA')
    data_opts.add_argument('--p', dest='antenna1',
                      help='Antenna 1 (comma-separated list, default = all)', default='all')
    data_opts.add_argument('--q', dest='antenna2',
                      help='Antenna 2 (comma-separated list, default = all)', default='all')
    data_opts.add_argument('--spw', dest='myspws',
                      help='Spectral windows (DDIDs) to plot (comma-separated list, default = all)', default='all')
    data_opts.add_argument('--field', dest='myfields',
                      help='Field ID(s) to plot (comma-separated list, default = all)', default='all')
    data_opts.add_argument('--scan', dest='myscans',
                      help='Scans to plot (comma-separated list, default = all)', default='all')    
    data_opts.add_argument('--corr', dest='corr',
                      help='Correlation index to plot (default = 0)', default=0)


    figure_opts = parser.add_argument_group('Plot settings')
    figure_opts.add_argument('--iterate', dest='iterate',
                      help='Set to p, q, spw, field or scan to produce a plot per selection or MS content (default = do not iterate)', default='')
    figure_opts.add_argument('--noflags', dest='noflags',
                      help='Enable to include flagged data', action='store_true', default=False)
    figure_opts.add_argument('--noconj', dest='noconj',
                      help='Do not show conjugate points in u,v plots (default = plot conjugates)', action='store_true', default=False)
    figure_opts.add_argument('--xmin', dest='xmin',
                      help='Minimum x-axis value (default = data min)', default='')
    figure_opts.add_argument('--xmax', dest='xmax',
                      help='Maximum x-axis value (default = data max)', default='')
    figure_opts.add_argument('--ymin', dest='ymin',
                      help='Minimum y-axis value (default = data min)', default='')
    figure_opts.add_argument('--ymax', dest='ymax',
                      help='Maximum y-axis value (default = data max)', default='')
    figure_opts.add_argument('--xcanvas', dest='xcanvas',
                      help='Canvas x-size in pixels (default = 1280)', default=1280)
    figure_opts.add_argument('--ycanvas', dest='ycanvas',
                      help='Canvas y-size in pixels (default = 800)', default=900)
    figure_opts.add_argument('--norm', dest='normalize',
                      help='Pixel scale normalization: eq_hist (default), cbrt, log, linear', default='eq_hist')
    figure_opts.add_argument('--cmap', dest='mycmap',
                      help='Colorcet map to use (default = bkr)', default='bkr')
    figure_opts.add_argument('--bgcol', dest='bgcol',
                      help='RGB hex code for background colour (default = FFFFFF)', default='FFFFFF')
    figure_opts.add_argument('--fontsize', dest='fontsize',
                      help='Font size for all text elements (default = 20)', default=20)


    output_opts = parser.add_argument_group('Output')
    output_opts.add_argument('--png', dest='pngname',
                      help='PNG name (default = something verbose)', default='')
    output_opts.add_argument('--stamp', dest='dostamp',
                      help='Enable to add timestamp to default PNG name', action='store_true', default=False)


    options = parser.parse_args(argv)

    xaxis = options.xaxis.lower()
    yaxis = options.yaxis.lower()
    col = options.col.upper()
    p = options.antenna1
    q = options.antenna2
    myspws = options.myspws
    myfields = options.myfields
    myscans = options.myscans
    corr = int(options.corr)

    iterate = options.iterate.lower()
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
    dostamp = options.dostamp

    myms = options.ms.rstrip('/')


    # ---------------------------------------------------------------------------------------------------------------------------------------------
    # Check for allowed axis selections and get long forms


    allowed_axes = ['a', 'p', 'r', 'i', 't', 'c', 'f', 'uv', 'u', 'v']
    allowed_iterators = ['','p','q','spw','field','scan']

    if xaxis not in allowed_axes:
        raise ValueError('--xaxis setting "%s" is unknown, please check inputs.' % xaxis)

    if yaxis not in allowed_axes:
        raise ValueError('--yaxis setting "%s" is unknown, please check inputs.' % yaxis)

    if iterate not in allowed_iterators:
        raise ValueError('--iterate setting "%s" is unknown, please check inputs.' % iterate)

    xfullname, xunits = sms.fullname(xaxis)
    yfullname, yunits = sms.fullname(yaxis)


    # ---------------------------------------------------------------------------------------------------------------------------------------------

    # Default group cols
    group_cols = ['FIELD_ID', 'DATA_DESC_ID']


    # # Sort out SPW selection

    chan_freqs = sms.get_chan_freqs(myms)
    # if myspws == 'all':
    #     spws = numpy.arange(len(chan_freqs))
    # else:
    #     spws = list(map(int, myspws.split(',')))    


    # # Sort out field selection

    # field_ids, field_names = sms.get_field_names(myms)
    # if myfields == 'all':
    #     fields = field_ids
    # else:
    #     fields = list(map(int, myfields.split(',')))


    # # Sort out scan selection

    # if myscans != 'all':
    #     group_cols.append('SCAN_NUMBER')
    #     scans = list(map(int, myscans.split(',')))
    # else:
    #     scans = sms.get_scan_numbers(myms)


    # Construct TaQL string based on FIELD, SPW and SCAN selections
    
    mytaql = []

    if myfields != 'all' and iterate != 'field':
        field_taql = []
        fields = list(map(int, myfields.split(',')))
        for fld in fields:
            field_taql.append('FIELD_ID=='+str(fld))
        mytaql.append(('('+' || '.join(field_taql)+')'))
    else:
        fields, field_names = sms.get_field_names(myms) 

    if myspws != 'all' and iterate != 'spw':
        spw_taql = []
        spws = list(map(int, myspws.split(',')))    
        for spw in spws:
            spw_taql.append('DATA_DESC_ID=='+str(spw))
        mytaql.append(('('+' || '.join(spw_taql)+')'))
    else:
        spws = numpy.arange(len(chan_freqs)) 

    if myscans != 'all' and iterate != 'scan':
        scan_taql = []
        group_cols.append('SCAN_NUMBER')
        scans = list(map(int, myscans.split(',')))
        for scan in scans:
            scan_taql.append('SCAN_NUMBER=='+str(scan))
        mytaql.append(('('+' || '.join(scan_taql)+')'))

    if mytaql:
        mytaql = ' && '.join(mytaql)
    else:
        mytaql = ''

#    mytaql = '('+' || '.join(field_taq)+') && ('+' || '.join(spw_taq)+')'

    print(mytaql)

    # log.info('Plotting %s vs %s' % (yfullname, xfullname))
    # log.info('Correlation index %d' % corr)
    # sms.blank()
    # log.info('FIELD_ID   NAME')
    # for i in fields:
    #     log.info('%-10s %-16s' % (i, field_names[i]))
    # sms.blank()
    # log.info('SPW_ID     NCHAN ')
    # for i in spws:
    #     nchan = len(chan_freqs.values[i])
    #     log.info('%-10s %-16s' % (i, nchan))
    # sms.blank()

    # Read the selected data

    log.info('Reading %s' % (myms))
    log.info('%s column' % (col))
    
    xdata,ydata = sms.getxydata(myms,col,group_cols,mytaql,chan_freqs,xaxis,yaxis,p,q,spws,fields,corr,noflags,noconj)

    img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(xdata,ydata,xaxis,yaxis,
                    xcanvas,ycanvas,xmin,xmax,ymin,ymax,mycmap,normalize) 




    # Setup plot labels and PNG name

    ylabel = yfullname+' '+yunits
    xlabel = xfullname+' '+xunits
    title = myms+' '+col+' (correlation '+str(corr)+')'
    if pngname == '':
        pngname = 'plot_'+myms.split('/')[-1]+'_'+col+'_'
        pngname += 'SPW-' + myspws.replace(',', '-')+ \
            '_FIELD-'+myfields.replace(',', '-')+\
            '_SCAN-'+myscans.replace(',', '-')+'_'
        pngname += yfullname+'_vs_'+xfullname+'_'+'corr'+str(corr)
        if dostamp:
            pngname += '_'+sms.stamp()
        pngname += '.png'

    # Render the plot

    log.info('Rendering plot')

    sms.make_plot(img_data,
              data_xmin,
              data_xmax,
              data_ymin,
              data_ymax,
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
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Done. Elapsed time: %s seconds.' % (elapsed))
