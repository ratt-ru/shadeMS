# -*- coding: future_fstrings -*-
# ian.heywood@physics.ox.ac.uk


import matplotlib
matplotlib.use('agg')


import numpy
import datetime
import os
import pkg_resources
import ShadeMS
import time
import logging
import itertools
import re


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
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enable debugging output")

    data_opts = parser.add_argument_group('Data selection')
    data_opts.add_argument('-x', '--xaxis', dest='xaxis', action="append",
                      help="""X axis of plot. Use [t]ime, [f]requency, [c]hannels, [u], [v], [uv]distance, [r]eal, [i]mag,
                      [a]mplitude, [p]hase. For multiple plots, use comma-separated list, or
                      specify multiple times for multiple plots.""")
                      
    data_opts.add_argument('-y', '--yaxis', dest='yaxis', action="append",
                      help='Y axis to plot. Must be given the same number of times as --xaxis.')

    data_opts.add_argument('-c', '--col', dest='col', action="append", default=[],
                      help="""Name of visibility column (default is DATA), if needed. You can also employ
                      'D-M', 'C-M', 'D/M', 'C/M' for various combinations of data, corrected and model. Can use multiple
                      times, or use comma-separated list, for multiple plots (or else specify it just once).
                      """)

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
                      help='Correlations to plot, use indices or labels (comma-separated list, default is 0)',
                      default="0")


    figure_opts = parser.add_argument_group('Plot settings')
    figure_opts.add_argument('--iter-field', action="store_true",
                      help='Separate plots per field (default is to combine in one plot)')
    figure_opts.add_argument('--iter-antenna', action="store_true",
                      help='Separate plots per antenna (default is to combine in one plot)')
    figure_opts.add_argument('--iter-spw', action="store_true",
                      help='Separate plots per spw (default is to combine in one plot)')
    figure_opts.add_argument('--iter-scan', action="store_true",
                      help='Separate plots per scan (default is to combine in one plot)')
    figure_opts.add_argument('--iter-corr', action="store_true",
                      help='Separate plots per correlation (default is to combine in one plot)')
    figure_opts.add_argument('--noflags',
                      help='Enable to include flagged data', action='store_true')
    figure_opts.add_argument('--noconj',
                      help='Do not show conjugate points in u,v plots (default = plot conjugates)', action='store_true')
    figure_opts.add_argument('--xmin', action='append',
                      help="""Minimum x-axis value (default = data min). Use multiple times for multiple plots, but 
                      note that the clipping is the same per axis across all plots, so only the last applicable
                      setting will be used.""")
    figure_opts.add_argument('--xmax', action='append',
                      help='Maximum x-axis value (default = data max)')
    figure_opts.add_argument('--ymin', action='append',
                      help='Minimum y-axis value (default = data min)')
    figure_opts.add_argument('--ymax', action='append',
                      help='Maximum y-axis value (default = data max)')

    figure_opts.add_argument('--xcanvas', type=int,
                      help='Canvas x-size in pixels (default = %(default)s)', default=1280)
    figure_opts.add_argument('--ycanvas', type=int,
                      help='Canvas y-size in pixels (default = %(default)s)', default=900)
    figure_opts.add_argument('--norm', dest='normalize', choices=['eq_hist', 'cbrt', 'log', 'linear'],
                      help='Pixel scale normalization (default = %(default)s)', default='eq_hist')
    figure_opts.add_argument('--cmap', dest='mycmap',
                      help='Colorcet map to use (default = %(default)s)', default='bkr')
    figure_opts.add_argument('--bgcol', dest='bgcol',
                      help='RGB hex code for background colour (default = FFFFFF)', default='FFFFFF')
    figure_opts.add_argument('--fontsize', dest='fontsize',
                      help='Font size for all text elements (default = 20)', default=20)


    output_opts = parser.add_argument_group('Output')
    # can also use "plot-{msbase}-{column}-{corr}-{xfullname}-vs-{yfullname}", let's expand on this later
    output_opts.add_argument('--png', dest='pngname',
                             default="plot-{ms}{_field}{_Spw}{_Scan}{_Ant}{_corr}-{yname}_vs_{xname}.png",
                      help='template for output png files, default "%(default)s"')
    output_opts.add_argument('--title',
                             default="{ms}{_field}{_Spw}{_Scan}{_Ant}{_corr}",
                      help='template for plot titles, default "%(default)s"')
    output_opts.add_argument('--xlabel',
                             default="{xname}{_xunit}",
                      help='template for X axis labels, default "%(default)s"')
    output_opts.add_argument('--ylabel',
                             default="{yname}{_yunit}",
                             help='template for X axis labels, default "%(default)s"')

    options = parser.parse_args(argv)

    myants = options.myants
#    q = options.antenna2
    myspws = options.myspws
    myfields = options.myfields
    myscans = options.myscans
    mycorrs = options.corr

    noflags = options.noflags
    noconj = options.noconj
    xmin = options.xmin
    xmax = options.xmax
    ymin = options.ymin
    ymax = options.ymax
    xcanvas = options.xcanvas
    ycanvas = options.ycanvas
    normalize = options.normalize
    mycmap = options.mycmap
    bgcol = '#'+options.bgcol.lstrip('#')
    fontsize = options.fontsize

    myms = options.ms.rstrip('/')

    if options.debug:
        ShadeMS.log_console_handler.setLevel(logging.DEBUG)

    # figure our list of plots to make

    xaxes = list(itertools.chain(*[opt.split(",") for opt in options.xaxis]))
    yaxes = list(itertools.chain(*[opt.split(",") for opt in options.yaxis]))
    log.debug(f"plot axes are {xaxes} {yaxes}")
    if len(xaxes) != len(yaxes):
        parser.error("--xaxis and --yaxis must be given the same number of times")

    def get_conformal_list(name, force_type=None):
        """
        For all other settings, returns list same length as xaxes, or throws error if no conformance.
        Can also impose a type such as float (returning None for an empty string)
        """
        optlist = getattr(options, name, None)
        if not optlist:
            return [None]*len(xaxes)
        # stick all lists together
        elems = list(itertools.chain(*[opt.split(",") for opt in optlist]))
        if len(elems) > 1 and len(elems) != len(xaxes):
            parser.error(f"--{name} must be given the same number of times as --xaxis, or else just once")
        # convert type
        if force_type:
            elems = [force_type(x) if x else None for x in elems]
        if len(elems) != len(xaxes):
            elems = [elems[0]]*len(xaxes)
        return elems

    # get list of columns and plot limites of the same length
    if not options.col:
        options.col = ["DATA"]
    columns = get_conformal_list('col')
    xmin = get_conformal_list('xmin', float)
    xmax = get_conformal_list('xmax', float)
    ymin = get_conformal_list('ymin', float)
    ymax = get_conformal_list('ymax', float)

    all_plots = list(zip(xaxes, yaxes, columns))

    all_axes = set(xaxes) | set(yaxes)

    # form dicts of min/max per axis
    axis_min = { axis:value for axis, value in zip(xaxes, xmin) if value is not None}
    axis_max = { axis:value for axis, value in zip(xaxes, xmax) if value is not None}
    axis_min.update({ axis:value for axis, value in zip(yaxes, ymin) if value is not None})
    axis_max.update({ axis:value for axis, value in zip(yaxes, ymax) if value is not None})

    log.debug(f"all plots are {all_plots}")

    sms.blank()
    log.info('Measurement Set  : %s' % myms)

    # log.info('Plotting         : %s vs %s' % (yfullname, xfullname))
    # log.info('MS column        : %s ' % col)
    # log.info('Correlation      : %d' % corr)

    group_cols = ['FIELD_ID', 'DATA_DESC_ID']
    chan_freqs = sms.get_chan_freqs(myms)

    mytaql = []

    if myants != 'all': 
        ant_taql = []
        # group_cols.append('ANTENNA1')
        ants = list(map(int, myants.split(',')))
        log.info('Antenna(s)       : %s' % ants)
        if options.iter_antenna:
            raise NotImplementedError("iteration over antennas not currently supported")
            # for ant in ants:
            #     ant_taql.append('(ANTENNA1=='+str(ant)+' || ANTENNA2=='+str(ant)+')')
            # mytaql.append(('('+' || '.join(ant_taql)+')'))
    else:
        ants = sms.get_antennas(myms)
        log.info('Antenna(s)       : all')

    fields, field_names = sms.get_field_names(myms)
    if myfields != 'all': 
        field_taql = []
        fields = list(map(int, myfields.split(',')))
        log.info('Field(s)         : %s' % fields)
        if options.iter_field:
            for fld in fields:
                field_taql.append('FIELD_ID=='+str(fld))
            mytaql.append(('('+' || '.join(field_taql)+')'))
    else:
        log.info('Field(s)         : all {}'.format(" ".join(field_names)))


    if myspws != 'all': 
        spw_taql = []
        spws = list(map(int, myspws.split(',')))    
        log.info('SPW(s)           : %s' % spws)
        if options.iter_spws:
            for spw in spws:
                spw_taql.append('DATA_DESC_ID=='+str(spw))
            mytaql.append(('('+' || '.join(spw_taql)+')'))
    else:
        spws = numpy.arange(len(chan_freqs))
        log.info(f'SPW(s)           : all {spws}')


    if myscans != 'all':
        scan_taql = []
        group_cols.append('SCAN_NUMBER')
        scans = list(map(int, myscans.split(',')))
        log.info('Scan(s)          : %s' % scans)
        if options.iter_scan:
            for scan in scans:
                scan_taql.append('SCAN_NUMBER=='+str(scan))
            mytaql.append(('('+' || '.join(scan_taql)+')'))
    else:
        if options.iter_scan:
            group_cols.append('SCAN_NUMBER')
        scans = sms.get_scan_numbers(myms)
        log.info('Scan(s)          : all')

    if mytaql:
        mytaql = ' && '.join(mytaql)
    else:
        mytaql = ''

    ms_corr_list = sms.get_correlations(myms)
    log.debug(f"correlations in MS are {ms_corr_list}")
    corr_map = { corr:i for i, corr in enumerate(ms_corr_list)}
    corrs = []
    for corr in mycorrs.upper().split(','):
        if re.fullmatch("\d+", corr):
            corrs.append(int(corr))
        elif corr in corr_map:
            corrs.append(corr_map[corr])
        else:
            parser.error("unknown corrrelation --corr {corr}")

    sms.blank()

    log.debug(f"taql is {mytaql}, group_cols is {group_cols}")

    dataframes, axis_col_labels, np = \
        sms.getxydata(myms, group_cols, mytaql, chan_freqs, all_plots,
                      spws=spws, fields=fields, corrs=corrs, noflags=noflags, noconj=noconj,
                      iter_field=options.iter_field, iter_spw=options.iter_spw,
                      iter_scan=options.iter_scan, iter_corr=options.iter_corr,
                      axis_min=axis_min, axis_max=axis_max)

    log.info("                 : rendering {} dataframes with {:.3g} points".format(len(dataframes), np))

    ## each dataframe is an instance of the axes being iterated over -- on top of that, we need to iterate over plot types

    # dictionary of substitutions for filename and title
    keys = {}
    keys['ms'] = os.path.basename(os.path.splitext(myms.rstrip("/"))[0])
    keys['timestamp'] = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    # dictionary of titles for these identifiers
    titles = dict(field="", field_num="field", scan="scan", corr="", spw="spw", antenna="ant", icorr="corr")

    keys['field_num'] = list(fields) if myfields != 'all' else ''
    keys['field'] = [field_names[fld] for fld in fields] if myfields != 'all' else ''
    keys['scan'] = list(scans) if myscans != 'all' else ''
    keys['ant'] = list(ants) if myants != 'all' else ''
    keys['spw'] = list(spws) if myspws != 'all' else ''
    keys['icorr'] = list(corrs) if mycorrs != 'all' else ''
    keys['corr'] = [ms_corr_list[i] for i in corrs] if mycorrs != 'all' else ''

    def generate_string_from_keys(template, keys, listsep=" ", titlesep=" ", prefix=" "):
        """Converts list of keys into a string suitable for plot titles or filenames.
        listsep is used to separate list elements: " " for plot titles, "_" for filenames.
        titlesep is used to separate names from values: " " for plot titles, "_" for filenames
        prefix is used to prefix {_key} substitutions: ", " for titles, "-" for filenames
        """
        full_keys = keys.copy()
        # convert lists to strings, add Capitalized keys with titles
        for key, value in keys.items():
            capkey = key.title()
            if value is '':
                full_keys[capkey] = ''
            else:
                if type(value) is list:                               # e.g. scan=[1,2] becomes scan="1 2"
                    full_keys[key] = value = listsep.join(map(str, value))
                if key in titles:                                     # e.g. Scan="scan 1 2"
                    full_keys[capkey] = f"{titles[key]}{titlesep}{value}"
                else:
                    full_keys[capkey] = value
        # add _keys which supply prefixes
        full_keys.update({f"_{key}": (f"{prefix}{value}" if value else '') for key, value in full_keys.items()})
        # finally, format
        return template.format(**full_keys)

    for (fld, spw, scan, antenna, corr), df in dataframes.items():
        # update keys to be substituted into title and filename
        if fld is not None:
            keys['field_num'] = fld
            keys['field'] = field_names[fld]
        if spw is not None:
            keys['spw'] = spw
        if scan is not None:
            keys['scan'] = scan
        if antenna is not None:
            keys['ant'] = antenna
        if corr is not None:
            keys['icorr'] = corr
            keys['corr'] = ms_corr_list[corr]

        # now loop over plot types
        for xaxis, yaxis, col in all_plots:
            keys.update(column=col, xaxis=xaxis, yaxis=yaxis,
                        xname=sms.mappers[xaxis].fullname, yname=sms.mappers[yaxis].fullname,
                        xunit=sms.mappers[xaxis].unit, yunit=sms.mappers[yaxis].unit)

            log.debug(f"rendering DS canvas  for {keys}")

            img_data, data_xmin, data_xmax, data_ymin, data_ymax = sms.run_datashader(df,
                axis_col_labels[xaxis, col], axis_col_labels[yaxis, col],
                xcanvas, ycanvas, mycmap, normalize)

            pngname = generate_string_from_keys(options.pngname, keys, "_", "_", "-")
            title   = generate_string_from_keys(options.title, keys, " ", " ", ", ")
            xlabel  = generate_string_from_keys(options.xlabel, keys, " ", " ", ", ")
            ylabel  = generate_string_from_keys(options.ylabel, keys, " ", " ", ", ")

            log.debug(f"rendering plot")

            # make output directory, if needed
            dirname = os.path.dirname(pngname)
            if dirname and not os.path.exists(dirname):
                os.mkdir(dirname)
                log.info(f'                 : created output directory {dirname}')

            sms.make_plot(img_data, data_xmin, data_xmax, data_ymin, data_ymax,
                          axis_min.get(xaxis), axis_max.get(xaxis),
                          axis_min.get(yaxis), axis_max.get(yaxis),
                          xlabel, ylabel, title,
                          pngname, bgcol, fontsize,
                          figx=xcanvas/60, figy=ycanvas/60)

            log.info(f'                 : wrote {pngname}')

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Total time       : %s seconds' % (elapsed))
    log.info('Finished')
    sms.blank()
