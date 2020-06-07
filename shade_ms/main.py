# -*- coding: future_fstrings -*-

import matplotlib
matplotlib.use('agg')

import datetime
import os
import pkg_resources
import shade_ms
import time
import logging
import itertools
import re
import sys
import colorcet
import dask.diagnostics
from contextlib import contextmanager
import json

import argparse

from . import data_plots, data_mappers

from .data_mappers import DataAxis, col_to_label
from .ms_info import MSInfo

from shade_ms import log, blank

try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"


# empty context manager for use when profiling is off
@contextmanager
def nullcontext(enter_result=None):
    yield enter_result

# set default number of renderers to half the available cores
DEFAULT_NUM_RENDERS = max(1, len(os.sched_getaffinity(0))//2)

DEFAULT_CNUM = 16

def main(argv):


    clock_start = time.time()

    # default # of CPUs


    # ---------------------------------------------------------------------------------------------------------------------------------------------


    parser = argparse.ArgumentParser(description='Rapid Measurement Set plotting with dask-ms and datashader. Version {0:s}'.format(__version__))


    parser.add_argument('ms', 
                      help='Measurement set')
    parser.add_argument("-v", "--version", action='version',
                      version='{:s} version {:s}'.format(parser.prog, __version__))

    group_opts = parser.add_argument_group('Plot types and data sources')

    group_opts.add_argument('-x', '--xaxis', dest='xaxis', action="append",
                      help="""X axis of plot, e.g. "amp:CORRECTED_DATA" This recognizes all column names (also CHAN, FREQ, 
                      CORR, ROW, WAVEL, U, V, W, UV), and, for complex columns, keywords such as 'amp', 'phase', 'real', 'imag'. You can also 
                      specify correlations, e.g. 'DATA:phase:XX', and do two-column arithmetic with "+-*/", e.g. 
                      'DATA-MODEL_DATA:amp'. Correlations may be specified by label, number, or as a Stokes parameter.
                      The order of specifiers does not matter.
                      """)

    group_opts.add_argument('-y', '--yaxis', dest='yaxis', action="append",
                      help="""Y axis to plot. Must be given the same number of times as --xaxis. Note that X/Y can
                      employ different columns and correlations.""")

    group_opts.add_argument('-a', '--aaxis', action="append",
                      help="""Intensity axis. Can be none, or given once, or given the same number of times as --xaxis.
                      If none, plot intensity (a.k.a. alpha channel) is proportional to density of points. Otherwise,
                      a reduction function (see --ared below) is applied to the given values, and the result is used
                      to determine intensity.
                      """)

    group_opts.add_argument('--ared', action="append",
                      help="""Alpha axis reduction function. Recognized reductions are count, any, sum, min, max,
                      mean, std, first, last, mode. Default is mean.""")

    group_opts.add_argument('-c', '--colour-by', action="append",
                      help="""Colour axis. Can be none, or given once, or given the same number of times as --xaxis.
                      All columns and variations listed under --xaxis are available for colouring by.""")

    group_opts.add_argument('-C', '--col', metavar="COLUMN", dest='col', action="append", default=[],
                      help="""Name of visibility column (default is DATA), if needed. This is used if
                      the axis specifications do not explicitly include a column. For multiple plots,
                      this can be given multiple times, or as a comma-separated list. Two-column arithmetic is recognized.
                      """)

    group_opts.add_argument('--noflags',
                      help='Enable to ignore flags. Default is to omit flagged data.', action='store_true')
    group_opts.add_argument('--noconj',
                      help='Do not show conjugate points in u,v plots (default = plot conjugates).', action='store_true')

    group_opts = parser.add_argument_group('Plot axes setup')

    group_opts.add_argument('--xmin', action='append',
                      help="""Minimum x-axis value (default = data min). For multiple plots, you can give this 
                      multiple times, or use a comma-separated list, but note that the clipping is the same per axis 
                      across all plots, so only the last applicable setting will be used. The list may include empty
                      elements (or 'None') to not apply a clip.""")
    group_opts.add_argument('--xmax', action='append',
                      help='Maximum x-axis value (default = data max).')
    group_opts.add_argument('--ymin', action='append',
                      help='Minimum y-axis value (default = data min).')
    group_opts.add_argument('--ymax', action='append',
                      help='Maximum y-axis value (default = data max).')
    group_opts.add_argument('--cmin', action='append',
                      help='Minimum colouring value. Must be supplied for every non-discrete axis to be coloured by.')
    group_opts.add_argument('--cmax', action='append',
                      help='Maximum colouring value. Must be supplied for every non-discrete axis to be coloured by.')
    group_opts.add_argument('--cnum', action='append',
                      help=f'Number of steps used to discretize a continuous axis. Default is {DEFAULT_CNUM}.')

    group_opts.add_argument('--xlim-load', action='store_true',
                      help=f'Load x-axis limits from limits file, if available.')
    group_opts.add_argument('--ylim-load', action='store_true',
                      help=f'Load y-axis limits from limits file, if available.')
    group_opts.add_argument('--clim-load', action='store_true',
                      help=f'Load colour axis limits from limits file, if available.')
    group_opts.add_argument('--lim-file', default="{ms}-minmax-cache.json",
                            help="""Name of limits file to save/load. '{ms}' will be substituted for MS base name.
                                    Default is '%(default)s'.""")
    group_opts.add_argument('--no-lim-save', action="store_false", dest="lim_save",
                            help="""Do not save auto-computed limits to limits file. Default is to save.""")
    group_opts.add_argument('--lim-save-reset', action="store_true",
                            help="""Reset limits file when saving. Default adds to existing file.""")

    group_opts = parser.add_argument_group('Options for multiple plots or combined plots')

    group_opts.add_argument('--iter-field', action="store_true",
                      help='Separate plots per field (default is to combine in one plot)')
    group_opts.add_argument('--iter-antenna', action="store_true",
                      help='Separate plots per antenna (default is to combine in one plot)')
    group_opts.add_argument('--iter-spw', action="store_true",
                      help='Separate plots per spw (default is to combine in one plot)')
    group_opts.add_argument('--iter-scan', action="store_true",
                      help='Separate plots per scan (default is to combine in one plot)')
    group_opts.add_argument('--iter-corr', action="store_true",
                      help='Separate plots per correlation or Stokes (default is to combine in one plot)')
    group_opts.add_argument('--iter-ant', action="store_true",
                            help='Separate plots per antenna (default is to combine in one plot)')

    group_opts = parser.add_argument_group('Data subset selection')

    group_opts.add_argument('--ant', default='all',
                      help='Antennas to plot (comma-separated list of names, default = all)')
    group_opts.add_argument('--ant-num',
                      help='Antennas to plot (comma-separated list of numbers, or a [start]:[stop][:step] slice, overrides --ant)')
    group_opts.add_argument('--baseline', default='noauto',
                      help="Baselines to plot, as 'ant1-ant2' (comma-separated list, default of 'noauto' omits "
                           "auto-correlations, use 'all' to select all)")
    group_opts.add_argument('--spw', default='all',
                      help='Spectral windows (DDIDs) to plot (comma-separated list, default = all)')
    group_opts.add_argument('--field', default='all',
                      help='Field ID(s) to plot (comma-separated list, default = all)')
    group_opts.add_argument('--scan', default='all',
                      help='Scans to plot (comma-separated list, default = all)')
    group_opts.add_argument('--corr',  default='all',
                      help='Correlations or Stokes to plot, use indices or labels (comma-separated list, default = all)')
    group_opts.add_argument('--chan',
                      help='Channel slice, as [start]:[stop][:step], default is to plot all channels')

    group_opts = parser.add_argument_group('Rendering settings')

    group_opts.add_argument('-X', '--xcanvas', type=int,
                      help='Canvas x-size in pixels (default = %(default)s)', default=1280)
    group_opts.add_argument('-Y', '--ycanvas', type=int,
                      help='Canvas y-size in pixels (default = %(default)s)', default=900)
    group_opts.add_argument('--norm', choices=['auto', 'eq_hist', 'cbrt', 'log', 'linear'], default='auto',
                      help="Pixel scale normalization (default is 'log' when colouring, and 'eq_hist' when not)")
    group_opts.add_argument('--cmap', default='bkr',
                      help="""Colorcet map used without --colour-by  (default = %(default)s), see
                      https://colorcet.holoviz.org""")
    group_opts.add_argument('--bmap', default='bkr',
                      help='Colorcet map used when colouring by a continuous axis (default = %(default)s)')
    group_opts.add_argument('--dmap', default='glasbey_dark',
                      help='Colorcet map used when colouring by a discrete axis (default = %(default)s)')
    group_opts.add_argument('--min-alpha', default=40, type=int, metavar="0-255",
                      help="""Minimum alpha value used in rendering the canvas. Increase to saturate colour at
                      the expense of dynamic range. Default is %(default)s.""")
    group_opts.add_argument('--saturate-perc', default=95, type=int, metavar="0-100",
                      help="""Saturate colors so that the range [min-alpha, X] is mapped to [min-alpha, 255],
                              where X is the given percentile. Default is %(default)s.""")
    group_opts.add_argument('--saturate-alpha', default=None, type=int, metavar="0-255",
                      help="""Saturate colors as above, but with a fixed value of X. Overrides --saturate-perc.""")
    group_opts.add_argument('--spread-pix', type=int, default=0, metavar="PIX",
                      help="""Dynamically spread rendered pixels to this size""")
    group_opts.add_argument('--spread-thr', type=float, default=0.5, metavar="THR",
                      help="""Threshold parameter for spreading (0 to 1, default %(default)s)""")
    group_opts.add_argument('--bgcol', dest='bgcol',
                      help='RGB hex code for background colour (default = FFFFFF)', default='FFFFFF')
    group_opts.add_argument('--fontsize', dest='fontsize',
                      help='Font size for all text elements (default = 20)', default=16)

    group_opts = parser.add_argument_group('Output settings')

    # can also use "plot-{msbase}-{column}-{corr}-{xfullname}-vs-{yfullname}", let's expand on this later
    group_opts.add_argument('--dir',
                      help='Send all plots to this output directory')
    group_opts.add_argument('-s', '--suffix', help="suffix to be included in filenames, can include {options}")
    group_opts.add_argument('--png', dest='pngname',
                             default="plot-{ms}{_field}{_Spw}{_Scan}{_Ant}-{label}{_alphalabel}{_colorlabel}{_suffix}.png",
                      help='Template for output png files, default "%(default)s"')
    group_opts.add_argument('--title',
                             default="{ms}{_field}{_Spw}{_Scan}{_Ant}{_title}{_Alphatitle}{_Colortitle}",
                      help='Template for plot titles, default "%(default)s"')
    group_opts.add_argument('--xlabel',
                             default="{xname}{_xunit}",
                      help='Template for X axis labels, default "%(default)s"')
    group_opts.add_argument('--ylabel',
                             default="{yname}{_yunit}",
                             help='Template for X axis labels, default "%(default)s"')

    group_opts = parser.add_argument_group('Performance & tweaking')

    group_opts.add_argument("-d", "--debug", action='store_true',
                            help="Enable debugging output")
    group_opts.add_argument('-z', '--row-chunk-size', type=int, metavar="NROWS", default=5000,
                           help="""Row chunk size for dask-ms. Larger chunks may or may not be faster, but will
                            certainly use more RAM.""")
    group_opts.add_argument('-j', '--num-parallel', type=int, metavar="N", default=1,
                             help=f"""Run up to N renderers in parallel. Default is serial. Use -j0 to 
                             auto-set this to half the available cores ({DEFAULT_NUM_RENDERS} on this system).
                             This is not necessarily faster, as they might all end up contending for disk I/O. 
                             This might also work against dask-ms's own intrinsic parallelism. 
                             You have been advised.""")
    group_opts.add_argument("--profile", action='store_true', help="Enable dask profiling output")


    # various hidden performance-testing options
    data_mappers.add_options(group_opts)
    data_plots.add_options(group_opts)

    options = parser.parse_args(argv)

    cmap = getattr(colorcet, options.cmap, None)
    if cmap is None:
        parser.error(f"unknown --cmap {options.cmap}")
    bmap = getattr(colorcet, options.bmap, None)
    if bmap is None:
        parser.error(f"unknown --bmap {options.bmap}")
    dmap = getattr(colorcet, options.dmap, None)
    if dmap is None:
        parser.error(f"unknown --dmap {options.dmap}")

    options.ms = options.ms.rstrip('/')

    if options.debug:
        shade_ms.log_console_handler.setLevel(logging.DEBUG)

    # pass options to shade_ms
    data_mappers.set_options(options)
    data_plots.set_options(options)

    # figure our list of plots to make

    if not options.xaxis:
        xaxes = ['TIME'] # Default xaxis if none is specified
    else:
        xaxes = list(itertools.chain(*[opt.split(",") for opt in options.xaxis]))
    if not options.yaxis:
        yaxes = ['DATA:amp'] # Default yaxis if none is specified
    else:
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
    aaxes = get_conformal_list('aaxis')
    areds = get_conformal_list('ared', str, 'mean')
    caxes = get_conformal_list('colour_by')
    cmins = get_conformal_list('cmin', float)
    cmaxs = get_conformal_list('cmax', float)
    cnums = get_conformal_list('cnum', int, default=DEFAULT_CNUM)

    # check min/max
    if any([(a is None)^(b is None) for a, b in zip(xmins, xmaxs)]):
        parser.error("--xmin/--xmax must be either both set, or neither")
    if any([(a is None)^(b is None) for a, b in zip(ymins, ymaxs)]):
        parser.error("--xmin/--xmax must be either both set, or neither")
    if any([(a is None)^(b is None) for a, b in zip(ymins, ymaxs)]):
        parser.error("--cmin/--cmax must be either both set, or neither")

    # check chan slice
    def parse_slice_spec(spec, name):
        if spec:
            try:
                spec_elems = [int(x) if x else None for x in spec.split(":", 2)]
            except ValueError:
                parser.error(f"invalid selection --{name} {spec}")
            return slice(*spec_elems), spec_elems
        else:
            return slice(None), []

    chanslice, chanslice_spec = parse_slice_spec(options.chan, name="chan")

    log.info(" ".join(sys.argv))

    blank()

    ms = MSInfo(options.ms, log=log)

    blank()
    log.info(": Data selected for plotting:")

    group_cols = ['FIELD_ID', 'DATA_DESC_ID']

    mytaql = []

    class Subset(object):
        pass
    subset = Subset()

    if options.ant != 'all' or options.ant_num:
        if options.ant_num:
            ant_subset = set()
            for spec in options.ant_num.split(","):
                if re.fullmatch(r"\d+", spec):
                    ant_subset.add(int(spec))
                else:
                    ant_subset.update(ms.all_antenna.numbers[parse_slice_spec(spec, "ant-num")[0]])
            subset.ant = ms.antenna.get_subset(sorted(ant_subset))
        else:
            subset.ant = ms.antenna.get_subset(options.ant)
        log.info(f"Antenna name(s)  : {' '.join(subset.ant.names)}")
        mytaql.append("||".join([f'ANTENNA1=={ant}||ANTENNA2=={ant}' for ant in subset.ant.numbers]))
    else:
        subset.ant = ms.antenna
        log.info('Antenna(s)       : all')

    if options.iter_antenna:
        raise NotImplementedError("iteration over antennas not currently supported")

    if options.baseline == 'all':
        log.info('Baseline(s)      : all')
        subset.baseline = ms.baseline
    elif options.baseline == 'noauto':
        log.info('Baseline(s)      : all except autocorrelations')
        subset.baseline = ms.all_baseline.get_subset([i for i in ms.baseline.numbers if ms.baseline_lengths[i]!=0])
        mytaql.append("ANTENNA1!=ANTENNA2")
    else:
        bls = set()
        a1a2 = set()
        for blspec in options.baseline.split(","):
            match = re.fullmatch(r"(\w+)-(\w+)", blspec)
            ant1 = match and ms.antenna[match.group(1)]
            ant2 = match and ms.antenna[match.group(2)]
            if ant1 is None or ant2 is None:
                raise ValueError("invalid baseline '{blspec}'")
            a1a2.add((ant1, ant2))
            bls.add(ms.baseline_number(ant1, ant2))
        # group_cols.append('ANTENNA1')
        subset.baseline = ms.all_baseline.get_subset(sorted(bls))
        log.info(f"Baseline(s)      : {' '.join(subset.baseline.names)}")
        mytaql.append("||".join([f'(ANTENNA1=={ant1}&&ANTENNA2=={ant2})||(ANTENNA1=={ant2}&&ANTENNA2=={ant1})'
                                 for ant1, ant2 in a1a2]))

    if options.field != 'all':
        subset.field = ms.field.get_subset(options.field)
        log.info(f"Field(s)         : {' '.join(subset.field.names)}")
        # here for now, workaround for https://github.com/ska-sa/dask-ms/issues/100, should be inside if clause
        mytaql.append("||".join([f'FIELD_ID=={fld}' for fld in subset.field.numbers]))
    else:
        subset.field = ms.field
        log.info('Field(s)         : all')

    if options.spw != 'all':
        subset.spw = ms.spw.get_subset(options.spw)
        log.info(f"SPW(s)           : {' '.join(subset.spw.names)}")
        mytaql.append("||".join([f'DATA_DESC_ID=={ddid}' for ddid in subset.spw.numbers]))
    else:
        subset.spw = ms.spw
        log.info(f'SPW(s)           : all')

    if options.scan != 'all':
        subset.scan = ms.scan.get_subset(options.scan)
        log.info(f"Scan(s)          : {' '.join(subset.scan.names)}")
        mytaql.append("||".join([f'SCAN_NUMBER=={n}' for n in subset.scan.numbers]))
    else:
        subset.scan = ms.scan
        log.info('Scan(s)          : all')
    if options.iter_scan:
        group_cols.append('SCAN_NUMBER')

    if chanslice == slice(None):
        log.info('Channels         : all')
    else:
        log.info(f"Channels         : {':'.join(str(x) if x is not None else '' for x in chanslice_spec)}")

    mytaql = ' && '.join([f"({t})" for t in mytaql]) if mytaql else ''

    options.corr = options.corr.upper()
    if options.corr == "ALL":
        subset.corr = ms.corr
    else:
        # recognize shortcut when it's just Stokes indices, convert to list
        if re.fullmatch(r"[IQUV]+", options.corr):
            options.corr = ",".join(options.corr)
        subset.corr = ms.all_corr.get_subset(options.corr)
    log.info(f"Corr/Stokes      : {' '.join(subset.corr.names)}")

    blank()

    # check minmax cache
    msbase = os.path.splitext(os.path.basename(options.ms))[0]
    cache_file = options.lim_file.format(ms=msbase)
    if options.dir and not "/" in cache_file:
        cache_file = os.path.join(options.dir, cache_file)

    # try to load the minmax cache file
    if not os.path.exists(cache_file):
        minmax_cache = {}
    else:
        log.info(f"loading minmax cache from {cache_file}")
        try:
            minmax_cache = json.load(open(cache_file, "rt"))
            if type(minmax_cache) is not dict:
                raise TypeError("cache cotent is not a dict")
        except Exception as exc:
            log.error(f"error reading cache file: {exc}. Minmax cache will be reset.")
            minmax_cache = {}

    # figure out list of plots to make
    all_plots = []

    # This will be True if any of the specified axes change with correlation
    have_corr_dependence = False

    # now go create definitions
    for xaxis, yaxis, default_column, caxis, aaxis, ared, xmin, xmax, ymin, ymax, cmin, cmax, cnum in \
            zip(xaxes, yaxes, columns, caxes, aaxes, areds, xmins, xmaxs, ymins, ymaxs, cmins, cmaxs, cnums):
        # get axis specs
        xspecs = DataAxis.parse_datum_spec(xaxis, default_column, ms=ms)
        yspecs = DataAxis.parse_datum_spec(yaxis, default_column, ms=ms)
        aspecs = DataAxis.parse_datum_spec(aaxis, default_column, ms=ms) if aaxis else [None] * 4
        cspecs = DataAxis.parse_datum_spec(caxis, default_column, ms=ms) if caxis else [None] * 4
        # parse axis specifications
        xfunction, xcolumn, xcorr, xitercorr = xspecs
        yfunction, ycolumn, ycorr, yitercorr = yspecs
        afunction, acolumn, acorr, aitercorr = aspecs
        cfunction, ccolumn, ccorr, citercorr = cspecs
        # does anything here depend on correlation?
        datum_itercorr = (xitercorr or yitercorr or aitercorr or citercorr)
        if datum_itercorr:
            have_corr_dependence = True
        if "FLAG" in (xcolumn, ycolumn, acolumn, ccolumn) or "FLAG_ROW" in (xcolumn, ycolumn, acolumn, ccolumn):
            if not options.noflags:
                log.info(": plotting a flag column implies that flagged data will not be masked")
                options.noflags = True

        # do we iterate over correlations/Stokes to make separate plots now?
        if datum_itercorr and options.iter_corr:
            corr_list = subset.corr.numbers
        else:
            corr_list = [None]

        def describe_corr(corrvalue):
            """Returns list of correlation labels corresponding to this corr setting"""
            if corrvalue is None:
                return subset.corr.names
            elif corrvalue is False:
                return []
            else:
                return [ms.all_corr.names[corrvalue]]

        for corr in corr_list:
            plot_xcorr = corr if xcorr is None else xcorr  # False if no corr in datum, None if all, else set to iterant or to fixed value
            plot_ycorr = corr if ycorr is None else ycorr
            plot_acorr = corr if acorr is None else acorr
            plot_ccorr = corr if ccorr is None else ccorr
            xdatum = DataAxis.register(xfunction, xcolumn, plot_xcorr, ms=ms, minmax=(xmin, xmax), subset=subset,
                                       minmax_cache=minmax_cache if options.xlim_load else None)
            ydatum = DataAxis.register(yfunction, ycolumn, plot_ycorr, ms=ms, minmax=(ymin, ymax), subset=subset,
                                       minmax_cache=minmax_cache if options.ylim_load else None)
            adatum = afunction and DataAxis.register(afunction, acolumn, plot_acorr, ms=ms, subset=subset)
            cdatum = cfunction and DataAxis.register(cfunction, ccolumn, plot_ccorr, ms=ms,
                                                     minmax=(cmin, cmax), ncol=cnum, subset=subset,
                                                     minmax_cache=minmax_cache if options.clim_load else None)

            # figure out plot properties -- basically construct a descriptive name and label
            # looks complicated, but we're just trying to figure out what to put in the plot title...
            props = dict()
            titles = []
            labels = []
            # start with column and correlation(s)
            if ycolumn and not ydatum.mapper.column:   # only put column if not fixed by mapper
                titles.append(ycolumn)
                labels.append(col_to_label(ycolumn))
            titles += describe_corr(plot_ycorr)
            labels += describe_corr(plot_ycorr)
            if ydatum.mapper.fullname:
                titles += [ydatum.mapper.fullname]
            titles += ["vs"]
            if ydatum.function:
                labels.append(ydatum.function)
            # add x column/subset.corr, if different
            if xcolumn and (xcolumn != ycolumn or not xdatum.function) and not xdatum.mapper.column:
                titles.append(xcolumn)
                labels.append(col_to_label(xcolumn))
            if plot_xcorr != plot_ycorr:
                titles += describe_corr(plot_xcorr)
                labels += describe_corr(plot_xcorr)
            if xdatum.mapper.fullname:
                titles += [xdatum.mapper.fullname]
            if xdatum.function:
                labels.append(xdatum.function)
            props['title'] = " ".join(titles)
            props['label'] = "-".join(labels)
            # build up intensity label
            if afunction:
                titles, labels = [ared], [ared]
                if acolumn and (acolumn != xcolumn or acolumn != ycolumn) and adatum.mapper.column is None:
                    titles.append(acolumn)
                    labels.append(col_to_label(acolumn))
                if plot_acorr and (plot_acorr != plot_xcorr or plot_acorr != plot_ycorr):
                    titles += describe_corr(plot_acorr)
                    labels += describe_corr(plot_acorr)
                titles += [adatum.mapper.fullname]
                if adatum.function:
                    labels.append(adatum.function)
                props['alpha_title'] = " ".join(titles)
                props['alpha_label'] = "-".join(labels)
            else:
                props['alpha_title'] = props['alpha_label'] = ''
            # build up color-by label
            if cfunction:
                titles, labels = [], []
                if ccolumn and (ccolumn != xcolumn or ccolumn != ycolumn) and cdatum.mapper.column is None:
                    titles.append(ccolumn)
                    labels.append(col_to_label(ccolumn))
                if plot_ccorr and (plot_ccorr != plot_xcorr or plot_ccorr != plot_ycorr):
                    titles += describe_corr(plot_ccorr)
                    labels += describe_corr(plot_ccorr)
                if cdatum.mapper.fullname:
                    titles.append(cdatum.mapper.fullname)
                if cdatum.function:
                    labels.append(cdatum.function)
                props['color_title'] = " ".join(titles)
                props['color_label'] = "-".join(labels)
                props['color_modulo'] = f"(into {cdatum.nlevels} colours)"
            else:
                props['color_title'] = props['color_label'] = ''

            all_plots.append((props, xdatum, ydatum, adatum, ared, cdatum))
            log.debug(f"adding plot for {props['title']}")

    # reset minmax cache if requested
    if options.lim_save_reset:
        minmax_cache = {}

    join_corrs = not options.iter_corr and len(subset.corr) > 1 and have_corr_dependence

    log.info('                 : you have asked for {} plots employing {} unique datums'.format(len(all_plots),
                                                                                                len(DataAxis.all_axes)))
    if not len(all_plots):
        sys.exit(0)

    log.debug(f"taql is {mytaql}, group_cols is {group_cols}, join subset.corr is {join_corrs}")

    dataframes, np = \
        data_plots.get_plot_data(ms, group_cols, mytaql, ms.chan_freqs,
                                 chanslice=chanslice, subset=subset,
                                 noflags=options.noflags, noconj=options.noconj,
                                 iter_field=options.iter_field, iter_spw=options.iter_spw,
                                 iter_scan=options.iter_scan, iter_ant=options.iter_ant,
                                 join_corrs=join_corrs,
                                 row_chunk_size=options.row_chunk_size)

    log.info(f": rendering {len(dataframes)} dataframes with {np:.3g} points into {len(all_plots)} plot types")

    ## each dataframe is an instance of the axes being iterated over -- on top of that, we need to iterate over plot types

    # dictionary of substitutions for filename and title
    keys = {}
    keys['ms'] = os.path.basename(os.path.splitext(options.ms.rstrip("/"))[0])
    keys['timestamp'] = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    # dictionary of titles for these identifiers
    titles = dict(field="", field_num="field", scan="scan", spw="spw", antenna="ant", colortitle="coloured by")

    keys['field_num'] = subset.field.numbers if options.field != 'all' else ''
    keys['field'] = subset.field.names if options.field != 'all' else ''
    keys['scan'] = subset.scan.names if options.scan != 'all' else ''
    keys['ant'] = subset.ant.names if options.ant != 'all' else ''  ## TODO: also handle ant-num settings
    keys['spw'] = subset.spw.names if options.spw != 'all' else ''

    keys['suffix'] = suffix = options.suffix.format(**options.__dict__) if options.suffix else ''
    keys['_suffix'] = f".{suffix}" if suffix else ''

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
    executor = None

    def render_single_plot(df, xdatum, ydatum, adatum, ared, cdatum, pngname, title, xlabel, ylabel):
        """Renders a single plot. Make this a function since we might call it in parallel"""
        log.info(f": rendering {pngname}")
        normalize = options.norm
        if normalize == "auto":
            normalize = "log" if cdatum is not None else "eq_hist"
        if options.profile:
            context = dask.diagnostics.ResourceProfiler
        else:
            context = nullcontext
        with context() as profiler:
            result = data_plots.create_plot(df, xdatum, ydatum, adatum, ared, cdatum,
                                      cmap=cmap, bmap=bmap, dmap=dmap, normalize=normalize,
                                      min_alpha=options.min_alpha,
                                      saturate_alpha=options.saturate_alpha,
                                      saturate_percentile=options.saturate_perc,
                                      xlabel=xlabel, ylabel=ylabel, title=title, pngname=pngname,
                                      minmax_cache=minmax_cache,
                                      options=options)
        if result:
            log.info(f'                 : wrote {pngname}')
            if profiler is not None:
                profile_file = os.path.splitext(pngname)[0] + ".prof.html"
                dask.diagnostics.visualize(profiler, file_path=profile_file, show=False, save=True)
                log.info(f'                 : wrote profiler info to {profile_file}')


    for (fld, spw, scan, antenna), df in dataframes.items():
        # update keys to be substituted into title and filename
        if fld is not None:
            keys['field_num'] = fld
            keys['field'] = ms.field[fld]
        if spw is not None:
            keys['spw'] = spw
        if scan is not None:
            keys['scan'] = scan
        if antenna is not None:
            keys['ant'] = ms.all_antenna[antenna]

        # now loop over plot types
        for props, xdatum, ydatum, adatum, ared, cdatum in all_plots:
            keys.update(title=props['title'], label=props['label'],
                        alphatitle=props['alpha_title'], alphalabel=props['alpha_label'],
                        colortitle=props['color_title'],
                        colorlabel=props['color_label'],
                        xname=xdatum.fullname, yname=ydatum.fullname,
                        xunit=xdatum.mapper.unit, yunit=ydatum.mapper.unit)
            if cdatum is not None and cdatum.is_discrete and not cdatum.discretized_labels:
                keys['colortitle'] += ' '+props['color_modulo']

            pngname = generate_string_from_keys(options.pngname, keys, "_", "_", "-")
            title   = generate_string_from_keys(options.title, keys, " ", " ", ", ")
            xlabel  = generate_string_from_keys(options.xlabel, keys, " ", " ", ", ")
            ylabel  = generate_string_from_keys(options.ylabel, keys, " ", " ", ", ")

            if options.dir:
                pngname = os.path.join(options.dir, pngname)

            # make output directory, if needed
            dirname = os.path.dirname(pngname)
            if dirname and not os.path.exists(dirname):
                os.mkdir(dirname)
                log.info(f'                 : created output directory {dirname}')

            if options.num_parallel < 2 or len(all_plots) < 2:
                render_single_plot(df, xdatum, ydatum, adatum, ared, cdatum, pngname, title, xlabel, ylabel)
            else:
                from concurrent.futures import ThreadPoolExecutor
                executor = ThreadPoolExecutor(options.num_parallel)
                log.info(f'                 : submitting job for {pngname}')
                jobs.append(executor.submit(render_single_plot, df, xdatum, ydatum, adatum, ared, cdatum,
                                            pngname, title, xlabel, ylabel))

    # wait for jobs to finish
    if executor is not None:
        log.info(f'                 : waiting for {len(jobs)} jobs to complete')
        for job in jobs:
            job.result()

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Total time       : %s seconds' % (elapsed))

    if minmax_cache and options.lim_save:
        # ensure floats, because in64s and such cause errors
        minmax_cache = {axis: list(map(float, minmax)) for axis, minmax in minmax_cache.items()}

        with open(cache_file, "wt") as file:
            json.dump(minmax_cache, file, sort_keys=True, indent=4, separators=(',', ': '))
        log.info(f"Saved minmax cache to {cache_file} (disable with --no-lim-save)")

    log.info('Finished')
    blank()
