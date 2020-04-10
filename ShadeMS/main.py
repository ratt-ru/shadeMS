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
    data_opts.add_argument('--ant', dest='myants',
                      help='Antenna to plot (comma-separated list, default = all)', default='all')
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
                      help='Set to ant, q, spw, field or scan to produce a plot per selection or MS content (default = do not iterate)', default='')
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
    myants = options.myants
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

    log.info('Measurement Set is %s' % myms)
    log.info('Plotting: %s column' % col)
    log.info('--------- correlation product %d' %corr)

    group_cols = ['FIELD_ID', 'DATA_DESC_ID']
    chan_freqs = sms.get_chan_freqs(myms)

    
    mytaql = []

    if myants != 'all': 
        ant_taql = []
        group_cols.append('ANTENNA1')
        ants = list(map(int, myants.split(',')))
        if iterate != 'ant':
            for ant in ants:
                ant_taql.append('ANTENNA1=='+str(ant)+' || ANTENNA2=='+str(ant))
            mytaql.append(('('+' || '.join(ant_taql)+')'))
            log.info('--------- antenna(s): %s' % ants)
    else:
        log.info('--------- all antennas')


    if myfields != 'all': 
        field_taql = []
        fields = list(map(int, myfields.split(',')))
        if iterate != 'field':
            for fld in fields:
                field_taql.append('FIELD_ID=='+str(fld))
            mytaql.append(('('+' || '.join(field_taql)+')'))
            log.info('--------- field(s): %s' % fields)
    else:
        fields, field_names = sms.get_field_names(myms)
        log.info('--------- all fields')


    if myspws != 'all': 
        spw_taql = []
        spws = list(map(int, myspws.split(',')))    
        if iterate != 'spw':
            for spw in spws:
                spw_taql.append('DATA_DESC_ID=='+str(spw))
            mytaql.append(('('+' || '.join(spw_taql)+')'))
            log.info('--------- SPW(s): %s' % fields)
    else:
        spws = numpy.arange(len(chan_freqs))
        log.info('--------- all SPWs') 


    if myscans != 'all':
        scan_taql = []
        group_cols.append('SCAN_NUMBER')
        scans = list(map(int, myscans.split(',')))
        if iterate != 'scan':
            for scan in scans:
                scan_taql.append('SCAN_NUMBER=='+str(scan))
            mytaql.append(('('+' || '.join(scan_taql)+')'))
            log.info('--------- scan(s): %s' %scans)
    else:
        log.info('--------- all scans')


    if mytaql:
        mytaql = ' && '.join(mytaql)
    else:
        mytaql = ''

    print(mytaql)

    if iterate == '':

        xdata,ydata = sms.getxydata(myms, col,group_cols, mytaql, chan_freqs, xaxis, yaxis,
                        spws,fields,corr,noflags,noconj)

        img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(xdata, ydata, xaxis, yaxis,
                        xcanvas, ycanvas, xmin, xmax, ymin, ymax, mycmap, normalize) 

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

        sms.make_plot(img_data,data_xmin,data_xmax,data_ymin,data_ymax,xmin,
                        xmax,ymin,ymax,xlabel,ylabel,title,pngname,bgcol,fontsize,
                        figx=xcanvas/60,figy=ycanvas/60)

    elif iterate == 'field':

        log.info('Iterating over fields (%d in total)' % len(fields))

        for field in fields:

            taql_i = mytaql+' (FIELD_ID=='+str(field)+')'

            print(taql_i)

            xdata,ydata = sms.getxydata(myms, col,group_cols, taql_i, chan_freqs, xaxis, yaxis,
                            spws,fields,corr,noflags,noconj)

            img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(xdata, ydata, xaxis, yaxis,
                            xcanvas, ycanvas, xmin, xmax, ymin, ymax, mycmap, normalize) 
            ylabel = yfullname+' '+yunits
            xlabel = xfullname+' '+xunits
            title = myms+' '+col+' (correlation '+str(corr)+')'
            pngname = 'plot_'+str(field)+'.png'

            sms.make_plot(img_data,data_xmin,data_xmax,data_ymin,data_ymax,xmin,
                            xmax,ymin,ymax,xlabel,ylabel,title,pngname,bgcol,fontsize,
                            figx=xcanvas/60,figy=ycanvas/60)
    # Stop the clock

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Done. Elapsed time: %s seconds.' % (elapsed))
