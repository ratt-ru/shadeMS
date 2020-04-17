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
import sys
import colorcet
from concurrent.futures import ThreadPoolExecutor

from argparse import ArgumentParser
from ShadeMS import shadeMS as sms


log = ShadeMS.log


try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"


# set default number of renderers to half the available cores
DEFAULT_NUM_RENDERS = max(1, len(os.sched_getaffinity(0))//2)

DEFAULT_CNUM = 16

def main(argv):


    clock_start = time.time()

    # default # of CPUs


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    parser = ArgumentParser(description='Rapid Measurement Set plotting with dask-ms and datashader. Version {0:s}'.format(__version__))


    parser.add_argument('ms', 
                      help='Measurement set')
    parser.add_argument("-v", "--version", action='version',
                      version='{:s} version {:s}'.format(parser.prog, __version__))

    data_opts = parser.add_argument_group('Data selection')
    data_opts.add_argument('-x', '--xaxis', dest='xaxis', action="append",
                      help="""X axis of plot. Use [t]ime, [f]requency, [c]hannels, [u], [v], [uv]distance, [r]eal, [i]mag,
                      [a]mplitude, [p]hase. For multiple plots, use comma-separated list, or
                      specify multiple times for multiple plots.""")
                      
    data_opts.add_argument('-y', '--yaxis', dest='yaxis', action="append",
                      help='Y axis to plot. Must be given the same number of times as --xaxis.')

    data_opts.add_argument('-c', '--color-by', action="append",
                      help='Color-by axis and/or column. Can be none, or given once, or given the same number of times as --xaxis.')

    data_opts.add_argument('-C', '--col', dest='col', action="append", default=[],
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


    figure_opts = parser.add_argument_group('Plot selection')
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
                      help="""Minimum x-axis value (default = data min). For multiple plots, you can give this 
                      multiple times, or use a comma-separated list, but note that the clipping is the same per axis 
                      across all plots, so only the last applicable setting will be used. The list may include empty
                      elements (or 'None') to not apply a clip.""")
    figure_opts.add_argument('--xmax', action='append',
                      help='Maximum x-axis value (default = data max)')
    figure_opts.add_argument('--ymin', action='append',
                      help='Minimum y-axis value (default = data min)')
    figure_opts.add_argument('--ymax', action='append',
                      help='Maximum y-axis value (default = data max)')
    figure_opts.add_argument('--cmin', action='append',
                      help='Minimum colouring value. Must be supplied for every non-discrete axis to be coloured by')
    figure_opts.add_argument('--cmax', action='append',
                      help='Maximum colouring value. Must be supplied for every non-discrete axis to be coloured by')
    figure_opts.add_argument('--cnum', action='append', type=int,
                      help=f'Number of steps used to discretize a continuous axis. Default is {DEFAULT_CNUM}.')

    figure_opts = parser.add_argument_group('Rendering settings')
    figure_opts.add_argument('--xcanvas', type=int,
                      help='Canvas x-size in pixels (default = %(default)s)', default=1280)
    figure_opts.add_argument('--ycanvas', type=int,
                      help='Canvas y-size in pixels (default = %(default)s)', default=900)
    figure_opts.add_argument('--norm', dest='normalize', choices=['eq_hist', 'cbrt', 'log', 'linear'],
                      help='Pixel scale normalization (default = %(default)s)', default='eq_hist')
    figure_opts.add_argument('--cmap',
                      help='Colorcet map used without --color-by  (default = %(default)s)', default='bkr')
    figure_opts.add_argument('--bmap',
                      help='Colorcet map used when coloring by a continuous axis (default = %(default)s)', default='bkr')
    figure_opts.add_argument('--dmap',
                      help='Colorcet map used when coloring by a discrete axis (default = %(default)s)', default='glasbey_dark')
    figure_opts.add_argument('--bgcol', dest='bgcol',
                      help='RGB hex code for background colour (default = FFFFFF)', default='FFFFFF')
    figure_opts.add_argument('--fontsize', dest='fontsize',
                      help='Font size for all text elements (default = 20)', default=20)


    output_opts = parser.add_argument_group('Output')
    # can also use "plot-{msbase}-{column}-{corr}-{xfullname}-vs-{yfullname}", let's expand on this later
    output_opts.add_argument('--png', dest='pngname',
                             default="plot-{ms}{_field}{_Spw}{_Scan}{_Ant}-{label}{_colorlabel}.png",
                      help='template for output png files, default "%(default)s"')
    output_opts.add_argument('--title',
                             default="{ms}{_field}{_Spw}{_Scan}{_Ant}{_title}{_Colortitle}",
                      help='template for plot titles, default "%(default)s"')
    output_opts.add_argument('--xlabel',
                             default="{xname}{_xunit}",
                      help='template for X axis labels, default "%(default)s"')
    output_opts.add_argument('--ylabel',
                             default="{yname}{_yunit}",
                             help='template for X axis labels, default "%(default)s"')

    perf_opts = parser.add_argument_group('Performance & tweaking')

    perf_opts.add_argument("-d", "--debug", action='store_true',
                            help="Enable debugging output")
    perf_opts.add_argument('-z', '--row-chunk-size', type=int, metavar="N", default=100000,
                           help="""row chunk size for dask-ms. Larger chunks may or may not be faster, but will
                            certainly use more RAM.""")
    perf_opts.add_argument('-j', '--num-parallel', type=int, metavar="N", default=DEFAULT_NUM_RENDERS,
                             help="""run up to N renderers in parallel (default = %(default)s). This is not necessarily 
                             faster, as they might all end up contending for disk I/O. This might also work against 
                             dask-ms's own intrinsic parallelism. You have been advised.""")

    options = parser.parse_args(argv)

    myants = options.myants
#    q = options.antenna2
    myspws = options.myspws
    myfields = options.myfields
    myscans = options.myscans
    mycorrs = options.corr

    noflags = options.noflags
    noconj = options.noconj
    xcanvas = options.xcanvas
    ycanvas = options.ycanvas
    normalize = options.normalize
    bgcol = '#'+options.bgcol.lstrip('#')
    fontsize = options.fontsize

    cmap = getattr(colorcet, options.cmap)
    bmap = getattr(colorcet, options.bmap)
    dmap = getattr(colorcet, options.dmap)

    myms = options.ms.rstrip('/')

    if options.debug:
        ShadeMS.log_console_handler.setLevel(logging.DEBUG)

    # figure our list of plots to make

    xaxes = list(itertools.chain(*[opt.split(",") for opt in options.xaxis]))
    yaxes = list(itertools.chain(*[opt.split(",") for opt in options.yaxis]))

    if len(xaxes) != len(yaxes):
        parser.error("--xaxis and --yaxis must be given the same number of times")

    def get_conformal_list(name, force_type=None, default=None):
        """
        For all other settings, returns list same length as xaxes, or throws error if no conformance.
        Can also impose a type such as float (returning None for an empty string)
        """
        optlist = getattr(options, name, None)
        if not optlist:
            return [default]*len(xaxes)
        # stick all lists together
        elems = list(itertools.chain(*[opt.split(",") for opt in optlist]))
        if len(elems) > 1 and len(elems) != len(xaxes):
            parser.error(f"--{name} must be given the same number of times as --xaxis, or else just once")
        # convert type
        if force_type:
            elems = [force_type(x) if x and x.lower() != "none" else None for x in elems]
        if len(elems) != len(xaxes):
            elems = [elems[0]]*len(xaxes)
        return elems

    # get list of columns and plot limites of the same length
    if not options.col:
        options.col = ["DATA"]
    columns = get_conformal_list('col')
    xmins = get_conformal_list('xmin', float)
    xmaxs = get_conformal_list('xmax', float)
    ymins = get_conformal_list('ymin', float)
    ymaxs = get_conformal_list('ymax', float)
    caxes = get_conformal_list('color_by')
    cmins = get_conformal_list('cmin', float)
    cmaxs = get_conformal_list('cmax', float)
    cnums = get_conformal_list('cnum', int, default=DEFAULT_CNUM)

    sms.blank()

    from .ms_info import MSInfo
    ms = sms.ms = MSInfo(myms, log=log)

    sms.blank()

    group_cols = ['FIELD_ID', 'DATA_DESC_ID']

    mytaql = []

    if myants != 'all':
        ants = ms.antenna.get_subset(myants)
        # group_cols.append('ANTENNA1')
        log.info(f"Antenna name(s)  : {' '.join(ants.names)}")
        if options.iter_antenna:
            raise NotImplementedError("iteration over antennas not currently supported")
        mytaql.append("||".join([f'ANTENNA1=={ant}||ANTENNA2=={ant}' for ant in ants.numbers]))
    else:
        ants = ms.antenna
        log.info('Antenna(s)       : all')

    if myfields != 'all':
        fields = ms.field.get_subset(myfields)
        log.info(f"Field(s)         : {' '.join(fields.names)}")
        # here for now, workaround for https://github.com/ska-sa/dask-ms/issues/100, should be inside if clause
        mytaql.append("||".join([f'FIELD_ID=={fld}' for fld in fields.numbers]))
    else:
        fields = ms.field
        log.info('Field(s)         : all')

    if myspws != 'all':
        spws = ms.spws.get_subset(myspws)
        log.info(f"SPW(s)           : {' '.join(spws.names)}")
        mytaql.append("||".join([f'DATA_DESC_ID=={ddid}' for ddid in spws.numbers]))
    else:
        spws = ms.spws
        log.info(f'SPW(s)           : all')

    if myscans != 'all':
        scans = ms.scan.get_subset(myscans)
        log.info(f"Scan(s)          : {' '.join(scans.names)}")
        mytaql.append("||".join([f'SCAN_NUMBER=={n}' for n in scans.numbers]))
    else:
        scans = ms.scan
        log.info('Scan(s)          : all')

    if options.iter_scan:
        group_cols.append('SCAN_NUMBER')

    mytaql = ' && '.join([f"({t})" for t in mytaql]) if mytaql else ''

    corrs = ms.corr.get_subset(mycorrs)

    sms.blank()

    # figure out list of plots to make
    all_plots = []

    # This will be True if any of the specified axes change with correlation
    have_corr_dependence = False

    # now go create definitions
    for xaxis, yaxis, default_column, caxis, xmin, xmax, ymin, ymax, cmin, cmax, cnum in \
            zip(xaxes, yaxes, columns, caxes, xmins, xmaxs, ymins, ymaxs, cmins, cmaxs, cnums):
        # get axis specs
        xspecs = sms.DataAxis.parse_datum_spec(xaxis, default_column)
        yspecs = sms.DataAxis.parse_datum_spec(yaxis, default_column)
        cspecs = sms.DataAxis.parse_datum_spec(caxis, default_column) if caxis else [None] * 4
        # parse axis specifications
        xfunction, xcolumn, xcorr, xitercorr = xspecs
        yfunction, ycolumn, ycorr, yitercorr = yspecs
        cfunction, ccolumn, ccorr, citercorr = cspecs
        # does anything here depend on correlation?
        datum_itercorr = (xitercorr or yitercorr or citercorr)
        if datum_itercorr:
            have_corr_dependence = True
        join_corr = datum_itercorr and not options.iter_corr

        # do we iterate over correlations to make separate plots now?
        if datum_itercorr and options.iter_corr:
            corr_list = corrs.numbers
        else:
            corr_list = [None]

        def describe_corr(corrvalue):
            """Returns list of correlation labels corresponding to this corr setting"""
            if corrvalue is None:
                return ms.corr.names
            elif corrvalue is False:
                return []
            else:
                return [ms.corr.names[corrvalue]]

        for corr in corr_list:
            plot_xcorr = corr if xcorr is None else xcorr  # False if no corr in datum, None if all, else set to iterant or to fixed value
            plot_ycorr = corr if ycorr is None else ycorr
            plot_ccorr = corr if ccorr is None else ccorr
            xmap = sms.DataAxis.register(xfunction, xcolumn, plot_xcorr, (xmin, xmax))
            ymap = sms.DataAxis.register(yfunction, ycolumn, plot_ycorr, (ymin, ymax))
            cmap = cfunction and sms.DataAxis.register(cfunction, ccolumn, plot_ccorr, (cmin, cmax), cnum)

            # figure out plot properties -- basically construct a descriptive name and label
            # looks complicated, but we're just trying to figure out what to put in the plot title...
            props = dict()
            titles = []
            labels = []
            # start with column and correlation(s)
            if ycolumn and not ymap.mapper.column:   # only put column if not fixed by mapper
                titles.append(ycolumn)
                labels.append(sms.col_to_label(ycolumn))
            titles += describe_corr(plot_ycorr)
            labels += describe_corr(plot_ycorr)
            titles += [ymap.mapper.fullname, "vs"]
            if ymap.function:
                labels.append(ymap.function)
            # add x column/corrs, if different
            if xcolumn and (xcolumn != ycolumn or not xmap.function) and not xmap.mapper.column:
                titles.append(xcolumn)
                labels.append(sms.col_to_label(xcolumn))
            if plot_xcorr != plot_ycorr:
                titles += describe_corr(plot_xcorr)
                labels += describe_corr(plot_xcorr)
            titles += [xmap.mapper.fullname]
            if xmap.function:
                labels.append(xmap.function)
            props['title'] = " ".join(titles)
            props['label'] = "_".join(labels)
            # build up color-by label
            if cfunction:
                titles, labels = [], []
                if ccolumn and (ccolumn != xcolumn or ccolumn != ycolumn) and not cmap.mapper.column:
                    titles.append(ccolumn)
                    labels.append(sms.col_to_label(ccolumn))
                if plot_ccorr and (plot_ccorr != plot_xcorr or plot_ccorr != plot_ycorr):
                    titles += describe_corr(plot_ccorr)
                    labels += describe_corr(plot_ccorr)
                titles += [cmap.mapper.fullname]
                if cmap.function:
                    labels.append(cmap.function)
                if not cmap.discretized_delta:
                    if not cmap.discretized_labels or len(cmap.discretized_labels) > cmap.nlevels:
                        titles.append(f"(modulo {cmap.nlevels})")
                props['color_title'] = " ".join(titles)
                props['color_label'] = "_".join(labels)
            else:
                props['color_title'] = props['color_label'] = ''

            all_plots.append((props, xmap, ymap, cmap))
            log.debug(f"adding plot for {props['title']}")

    join_corrs = not options.iter_corr and len(corrs) > 1 and have_corr_dependence

    log.info('                 : you have asked for {} plots employing {} unique datums'.format(len(all_plots),
                                                                                                len(sms.DataAxis.all_axes)))
    if not len(all_plots):
        sys.exit(0)

    log.debug(f"taql is {mytaql}, group_cols is {group_cols}, join corrs is {join_corrs}")

    dataframes, np = \
        sms.get_plot_data(myms, group_cols, mytaql, ms.chan_freqs,
                      spws=spws, fields=fields, corrs=corrs, noflags=noflags, noconj=noconj,
                      iter_field=options.iter_field, iter_spw=options.iter_spw,
                      iter_scan=options.iter_scan,
                      join_corrs=join_corrs,
                      row_chunk_size=options.row_chunk_size)

    log.info(f": rendering {len(dataframes)} dataframes with {np:.3g} points into {len(all_plots)} plot types")

    ## each dataframe is an instance of the axes being iterated over -- on top of that, we need to iterate over plot types

    # dictionary of substitutions for filename and title
    keys = {}
    keys['ms'] = os.path.basename(os.path.splitext(myms.rstrip("/"))[0])
    keys['timestamp'] = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    # dictionary of titles for these identifiers
    titles = dict(field="", field_num="field", scan="scan", spw="spw", antenna="ant", colortitle="coloured by")

    keys['field_num'] = fields.numbers if myfields != 'all' else ''
    keys['field'] = fields.names if myfields != 'all' else ''
    keys['scan'] = scans.names if myscans != 'all' else ''
    keys['ant'] = ants.names if myants != 'all' else ''
    keys['spw'] = spws.names if myspws != 'all' else ''

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
            if value == '':
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

    jobs = []
    if options.num_parallel > 1:
        executor = ThreadPoolExecutor(options.num_parallel)
    else:
        executor = None

    def render_single_plot(df, xdatum, ydatum, cdatum, pngname, title, xlabel, ylabel):
        """Renders a single plot. Make this a function since we might call it in parallel"""
        log.info(f": rendering {pngname}")

        sms.create_plot(df, xdatum, ydatum, cdatum, xcanvas, ycanvas,
                        cmap, bmap, dmap, normalize,
                        xlabel, ylabel, title,
                        pngname, bgcol, fontsize,
                        figx=xcanvas / 60, figy=ycanvas / 60)

        log.info(f'                 : wrote {pngname}')

    for (fld, spw, scan, antenna), df in dataframes.items():
        # update keys to be substituted into title and filename
        if fld is not None:
            keys['field_num'] = fld
            keys['field'] = fields[fld]
        if spw is not None:
            keys['spw'] = spw
        if scan is not None:
            keys['scan'] = scan
        if antenna is not None:
            keys['ant'] = antenna

        # now loop over plot types
        for props, xmap, ymap, cmap in all_plots:
            keys.update(title=props['title'], label=props['label'],
                        colortitle=props['color_title'], colorlabel=props['color_label'],
                        xname=xmap.mapper.fullname, yname=ymap.mapper.fullname,
                        xunit=xmap.mapper.unit, yunit=ymap.mapper.unit)

            pngname = generate_string_from_keys(options.pngname, keys, "_", "_", "-")
            title   = generate_string_from_keys(options.title, keys, " ", " ", ", ")
            xlabel  = generate_string_from_keys(options.xlabel, keys, " ", " ", ", ")
            ylabel  = generate_string_from_keys(options.ylabel, keys, " ", " ", ", ")

            # make output directory, if needed
            dirname = os.path.dirname(pngname)
            if dirname and not os.path.exists(dirname):
                os.mkdir(dirname)
                log.info(f'                 : created output directory {dirname}')

            if executor is None or len(all_plots) < 2:
                render_single_plot(df, xmap, ymap, cmap, pngname, title, xlabel, ylabel)
            else:
                log.info(f'                 : submitting job for {pngname}')
                jobs.append(executor.submit(render_single_plot, df, xmap, ymap, cmap, pngname, title, xlabel, ylabel))

    # wait for jobs to finish
    if executor:
        log.info('                 : waiting for {} jobs to complete'.format(len(jobs)))
        for job in jobs:
            job.result()

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Total time       : %s seconds' % (elapsed))
    log.info('Finished')
    sms.blank()
