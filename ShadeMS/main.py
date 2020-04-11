#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk


import matplotlib
matplotlib.use('agg')


import daskms as xms
import numpy
import os
import pkg_resources
import ShadeMS
import time


from argparse import ArgumentParser
from ShadeMS import shadeMS as sms


log = ShadeMS.log


try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"


def main(argv):


    clock_start = time.time()


    # ---------------------------------------------------------------------------------------------------------------------------------------------


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
    data_opts.add_argument('--antenna', dest='myants',
                      help='Antenna(s) to plot (comma-separated list, default = all)', default='all')
    # data_opts.add_argument('--q', dest='antenna2',
    #                   help='Antenna 2 (comma-separated list, default = all)', default='all')
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
                      help='Set to antenna, spw, field or scan to produce a plot per selection or MS content (default = do not iterate)', default='none')
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
                      help='PNG name, without extension, iterations will be added (default = something verbose)', default='')
    output_opts.add_argument('--dest', dest='destdir',
                      help='Destination path for output PNGs (will be created if not present, default = CWD)', default='')
    output_opts.add_argument('--stamp', dest='dostamp',
                      help='Enable to add timestamp to default PNG name', action='store_true', default=False)


    options = parser.parse_args(argv)

    xaxis = options.xaxis.lower()
    yaxis = options.yaxis.lower()
    col = options.col.upper()
    myants = options.myants
#    q = options.antenna2
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
    destdir = options.destdir
    dostamp = options.dostamp

    myms = options.ms.rstrip('/')


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    allowed_axes = ['a', 'p', 'r', 'i', 't', 'c', 'f', 'uv', 'u', 'v']
    allowed_iterators = ['none','antenna','spw','field','scan']

    if xaxis not in allowed_axes:
        raise ValueError('--xaxis setting "%s" is unknown, please check inputs.' % xaxis)

    if yaxis not in allowed_axes:
        raise ValueError('--yaxis setting "%s" is unknown, please check inputs.' % yaxis)

    if iterate not in allowed_iterators:
        raise ValueError('--iterate setting "%s" is unknown, please check inputs.' % iterate)

    xfullname, xunits = sms.fullname(xaxis)
    yfullname, yunits = sms.fullname(yaxis)

    ylabel = yfullname+' '+yunits
    xlabel = xfullname+' '+xunits


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    sms.blank()
    log.info('Measurement Set  : %s' % myms)


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    if destdir != '':
        log.info('Measurement Set  : %s' % myms)
        log.info('Output directory : %s' % destdir)
        if not os.path.isdir(destdir):
            os.mkdir(destdir)
            log.info('                 : Created')
        else:
            log.info('                 : Found')


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    sms.blank()
    log.info('Plotting         : %s vs %s' % (yfullname, xfullname))
    log.info('MS column        : %s ' % col)
    log.info('Correlation      : %d' % corr)

    group_cols = ['FIELD_ID', 'DATA_DESC_ID']
    chan_freqs = sms.get_chan_freqs(myms)

    
    mytaql = []

    if myants != 'all': 
        ant_taql = []
        group_cols.append('ANTENNA1')
        ants = list(map(int, myants.split(',')))
        log.info('Antenna(s)       : %s' % ants)
        if iterate != 'antenna':
            for ant in ants:
                ant_taql.append('ANTENNA1=='+str(ant)+' || ANTENNA2=='+str(ant)+')')
            mytaql.append(('('+' || '.join(ant_taql)+')'))
    else:
        ants = sms.get_antennas(myms)
        log.info('Antenna(s)       : all')


    fields, field_names = sms.get_field_names(myms)
    if myfields != 'all': 
        field_taql = []
        fields = list(map(int, myfields.split(',')))
        log.info('Field(s)         : %s' % fields)
        if iterate != 'field':
            for fld in fields:
                field_taql.append('FIELD_ID=='+str(fld))
            mytaql.append(('('+' || '.join(field_taql)+')'))
    else:
        log.info('Field(s)         : all')


    if myspws != 'all': 
        spw_taql = []
        spws = list(map(int, myspws.split(',')))    
        log.info('SPW(s)           : %s' % spws)
        if iterate != 'spw':
            for spw in spws:
                spw_taql.append('DATA_DESC_ID=='+str(spw))
            mytaql.append(('('+' || '.join(spw_taql)+')'))
    else:
        spws = numpy.arange(len(chan_freqs))
        log.info('SPW(s)           : all')


    if myscans != 'all':
        scan_taql = []
        group_cols.append('SCAN_NUMBER')
        scans = list(map(int, myscans.split(',')))
        log.info('Scan(s)          : %s' % scans)
        if iterate != 'scan':
            for scan in scans:
                scan_taql.append('SCAN_NUMBER=='+str(scan))
            mytaql.append(('('+' || '.join(scan_taql)+')'))
    else:
        scans = sms.get_scan_numbers(myms)
        log.info('Scan(s)          : all')


    if mytaql:
        mytaql = ' && '.join(mytaql)
    else:
        mytaql = ''

    sms.blank()

    if iterate == 'none':

        xdata,ydata = sms.getxydata(myms, col,group_cols, mytaql, chan_freqs, xaxis, yaxis,
                        spws,fields,corr,noflags,noconj)

        img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(xdata, ydata, xaxis, yaxis,
                        xcanvas, ycanvas, xmin, xmax, ymin, ymax, mycmap, normalize) 

        if pngname == '':
            pngname = sms.generate_pngname(myms,col,corr,xfullname,yfullname,
                        myants,ants,myspws,spws,myfields,fields,myscans,scans,
                        iterate,-1,dostamp)
        else:
            pngname = pngname+'.png'

        title = myms+' '+col+' (correlation '+str(corr)+')'

        sms.make_plot(img_data,data_xmin,data_xmax,data_ymin,data_ymax,xmin,
                        xmax,ymin,ymax,xlabel,ylabel,title,pngname,bgcol,fontsize,
                        figx=xcanvas/60,figy=ycanvas/60)

        log.info('                 : %s' % pngname)


    else:

        iters = []

        if iterate == 'antenna':
            iterate_over = ants
            if 'ANTENNA1' not in group_cols:
                group_cols.append('ANTENNA1')
#            group_cols.append('ANTENNA2')
            for i in iterate_over:
                iter_taql = 'ANTENNA1=='+str(i)+' || ANTENNA2=='+str(i)
                iter_info = 'Antenna '+str(i)
                iters.append((iter_taql,iter_info,i))
            log.info('Iterating over   : antennas (%d in total)' % len(ants))

        elif iterate == 'field':
            iterate_over = fields
            for i in iterate_over:
                iter_taql = 'FIELD_ID=='+str(i)
                iter_info = '(Field '+str(i)+', '+field_names[i]+')'
                iters.append((iter_taql,iter_info,i))
            log.info('Iterating over   : fields (%d in total)' % len(fields))

        elif iterate == 'spw':
            iterate_over = spws
            for i in iterate_over:
                iter_taql = 'DATA_DESC_ID=='+str(i)
                iter_info = '(SPW '+str(i)+')'
                iters.append((iter_taql,iter_info,i))
            log.info('Iterating over   : SPWs (%d in total)' % len(spws))

        elif iterate == 'scan':
            iterate_over = scans
            if 'SCAN_NUMBER' not in group_cols:
                group_cols.append('SCAN_NUMBER')
            for i in iterate_over:
                iter_taql = 'SCAN_NUMBER=='+str(i)
                iter_info = '(Scan '+str(i)+')'
                iters.append((iter_taql,iter_info,i))
            log.info('Iterating over   : scans (%d in total)' % len(scans))

        sms.blank()

        count = 1

        for ii in iters:

            clock_start_iter = time.time()

            taql_i = ii[0]
            info_i = ii[1]
            i = ii[2]

            log.info('Iteration        : %d / %d %s' % (count,len(iterate_over),info_i))

            if mytaql != '':
                taql_i = mytaql+' && '+taql_i

            xdata,ydata,doplot = sms.getxydata(myms, col,group_cols, taql_i, chan_freqs, xaxis, yaxis,
                            spws,fields,corr,noflags,noconj)

            if doplot:

                img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(xdata, ydata, xaxis, yaxis,
                                xcanvas, ycanvas, xmin, xmax, ymin, ymax, mycmap, normalize) 

                title = sms.generate_title(myms,col,corr,xfullname,yfullname,
                                myants,ants,myspws,spws,myfields,fields,myscans,scans,
                                iterate,i)

                if pngname == '':
                    pngname_i = sms.generate_pngname(myms,col,corr,xfullname,yfullname,
                                myants,ants,myspws,spws,myfields,fields,myscans,scans,
                                iterate,i,dostamp)
                else:
                    pngname_i = pngname+'_'+iterate.upper()+'-'+str(i)+'.png'


                sms.make_plot(img_data,data_xmin,data_xmax,data_ymin,data_ymax,xmin,
                                xmax,ymin,ymax,xlabel,ylabel,title,pngname_i,bgcol,fontsize,
                                figx=xcanvas/60,figy=ycanvas/60)

                log.info('                 : %s' % pngname_i)

            else:

                log.info('                 : No data returned for this selection')



            clock_stop_iter = time.time()
            elapsed_iter = str(round((clock_stop_iter-clock_start_iter), 2))
            log.info('Time             : %s seconds' % (elapsed_iter))
            sms.blank()

            count += 1

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Total time       : %s seconds' % (elapsed))
    log.info('Finished')
    sms.blank()