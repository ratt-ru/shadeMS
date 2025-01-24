# -*- coding: future_fstrings -*-

import argparse
import itertools
import numpy
from importlib.metadata import version, PackageNotFoundError

from . import DEFAULT_CNUM, DEFAULT_NUM_RENDERS

try:
    __version__ = version("shadems")
except PackageNotFoundError:
    __version__ = "dev"


def uppercase(str_):
    return str_.upper()


def rstrip_slash(str_):
    return str_.rstrip('/')


def cli():
    description = ("""Rapid Measurement Set plotting with dask-ms and datashader.
                   Version {0:s}"""
                   .format(__version__))
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("ms", type=rstrip_slash,  # remove / from ms by tab complete
        help="Measurement set")
    parser.add_argument("-v", "--version", action="version",
        version="{:s} version {:s}".format(parser.prog, __version__))

    group_opts = parser.add_argument_group("Plot types and data sources")
    group_opts.add_argument("-x", "--xaxis", action="append",
        help="""X axis of plot, e.g. "amp:CORRECTED_DATA"
             This recognizes all column names (also CHAN, FREQ, CORR, ROW, WAVEL, u, v, w, uv)
             and, for complex columns, keywords such as 'amp', 'phase', 'real', 'imag'.
             You can also specify correlations, e.g. 'DATA:phase:XX',
             and do two-column arithmetic with "+-*/", e.g. 'DATA-MODEL_DATA:amp'.
             Correlations may be specified by label, number, or as a Stokes parameter 
             (upper case is required to avoid ambiguity with baseline coordinates).
             The order of specifiers does not matter.""")
    group_opts.add_argument("-y", "--yaxis", action="append",
        help="""Y axis to plot.
             Must be given the same number of times as --xaxis.
             Note that X/Y can employ different columns and correlations.""")
    group_opts.add_argument("-a", "--aaxis", action="append",
        help="""Intensity axis.
             Can be none, or given once, or given the same number of times as --xaxis.
             If none, plot intensity (a.k.a. alpha channel) is proportional to density of points.
             Otherwise, a reduction function (see --ared below) is applied to the given values,
             and the result is used to determine intensity.
             All columns and variations listed under --xaxis are available for --aaxis.""")
    group_opts.add_argument("--ared", action="append",
        help="""Alpha axis reduction function.
             Recognized reductions are count, any, sum, min, max, mean, std, first, last, mode.
             Default is mean.""")

    group_opts.add_argument("-c", "--colour-by", action="append",
        help="""Colour (a.k.a. category) axis.
             Can be none, or given once, or given the same number of times as --xaxis.
             All columns and variations listed under --xaxis are available for colouring by.""")
    group_opts.add_argument("-C", "--col", metavar="COLUMN", dest="col", action="append", default=[],
        help="""Name of visibility column (default is DATA), if needed.
             This is used if the axis specifications do not explicitly include a column.
             For multiple plots, this can be given multiple times, or as a comma-separated list.
             Two-column arithmetic is recognized.""")

    group_opts.add_argument("--noflags", action="store_true",
        help="Enable to ignore flags. Default is to omit flagged data.")
    group_opts.add_argument("--noconj", action="store_true",
        help="Do not show conjugate points in u,v plots (default = plot conjugates).")

    group_opts = parser.add_argument_group("Plot axes setup")
    group_opts.add_argument("--xmin", action="append",
        help="""Minimum x-axis value (default = data min).
             For multiple plots, you can give this multiple times, or use a comma-separated list,
             but note that the clipping is the same per axis across all plots,
             so only the last applicable setting will be used.
             The list may include empty elements (or 'None') to not apply a clip.
             Default computes clips from data min/max.""")
    group_opts.add_argument("--xmax", action="append",
        help="Maximum x-axis value.")
    group_opts.add_argument("--ymin", action="append",
        help="Minimum y-axis value.")
    group_opts.add_argument("--ymax", action="append",
        help="Maximum y-axis value.")
    group_opts.add_argument("--amin", action="append",
        help="Minimum intensity-axis value.")
    group_opts.add_argument("--amax", action="append",
        help="Maximum intensity-axis value.")
    group_opts.add_argument("--cmin", action="append",
        help="Minimum value to be coloured by.")
    group_opts.add_argument("--cmax", action="append",
        help="Maximum value to be coloured by.")
    group_opts.add_argument("--cnum", action="append",
        help=f"""Number of steps used to discretize a continuous axis. Default is {DEFAULT_CNUM}.""")

    group_opts.add_argument("--xlim-load", action="store_true",
        help=f"Load x-axis limits from limits file, if available.")
    group_opts.add_argument("--ylim-load", action="store_true",
        help=f"Load y-axis limits from limits file, if available.")
    group_opts.add_argument("--clim-load", action="store_true",
        help=f"Load colour axis limits from limits file, if available.")
    group_opts.add_argument("--lim-file", default="{ms}-minmax-cache.json",
        help="""Name of limits file to save/load. '{ms}' will be substituted for MS base name.
             Default is '%(default)s'.""")
    group_opts.add_argument("--no-lim-save", action="store_false", dest="lim_save",
        help="""Do not save auto-computed limits to limits file. Default is to save.""")
    group_opts.add_argument("--lim-save-reset", action="store_true",
        help="""Reset limits file when saving. Default adds to existing file.""")

    group_opts = parser.add_argument_group("Options for multiple plots or combined plots")
    group_opts.add_argument("--iter-field", action="store_true",
        help="Separate plots per field (default is to combine in one plot)")
    group_opts.add_argument("--iter-spw", action="store_true",
        help="Separate plots per spw (default is to combine in one plot)")
    group_opts.add_argument("--iter-scan", action="store_true",
        help="Separate plots per scan (default is to combine in one plot)")
    group_opts.add_argument("--iter-corr", action="store_true",
        help="Separate plots per correlation or Stokes (default is to combine in one plot)")

    ex_group_opts = group_opts.add_mutually_exclusive_group()
    ex_group_opts.add_argument("--iter-ant", action="store_true",
        help="Separate plots per antenna (default is to combine in one plot)")
    ex_group_opts.add_argument("--iter-baseline", action="store_true",
        help="Separate plots per baseline (default is to combine in one plot)")

    group_opts = parser.add_argument_group("Data subset selection")
    ex_group_opts = group_opts.add_mutually_exclusive_group()
    ex_group_opts.add_argument("--ant", default="all",
        help="Antennas to plot (comma-separated list of names, default = all)")
    ex_group_opts.add_argument("--ant-num",
        help="""Antennas to plot (comma-separated list of numbers,
             or a [start]:[stop][:step] slice, mutually exclusive to --ant)""")
    group_opts.add_argument("--baseline", default="noautocorr",
        help="""Baselines to plot, as 'ant1-ant2' (comma-separated list:
             default of 'noautocorr' omits auto-correlations, 'autocorr' selects auto-correlations only, 'all' selects all baselines.
             Use 'ant1-*' to select all baselines to ant1.)""")
    group_opts.add_argument("--spw", default="all",
        help="""Spectral windows (DDIDs) to plot (comma-separated list, default = all)""")
    group_opts.add_argument("--field", default="all",
        help="Field ID(s) to plot (comma-separated list, default = all)")
    group_opts.add_argument("--scan", default="all",
        help="Scans to plot (comma-separated list, default = all)")
    group_opts.add_argument("--corr", default="all", type=uppercase,  # need this argument in upper case
        help="""Correlations or Stokes to plot, use indices or labels
             (comma-separated list, default = all)""")
    group_opts.add_argument("--chan",
        help="""Channel slice, as [start]:[stop][:step], default is to plot all channels""")

    group_opts = parser.add_argument_group("Rendering settings")
    group_opts.add_argument("-X", "--xcanvas", default=1280, type=int,
        help="Canvas x-size in pixels (default = %(default)s)")
    group_opts.add_argument("-Y", "--ycanvas", default=900, type=int,
        help="Canvas y-size in pixels (default = %(default)s)")
    group_opts.add_argument("--norm",
        choices=["auto", "eq_hist", "cbrt", "log", "linear"], default="auto",
        help="""Pixel scale normalization
             (default is 'log' with caxis, 'linear' with aaxis, and 'eq_hist' when neither is in use)""")
    group_opts.add_argument("--cmap", default="bkr",
        help="""Colorcet map used without --colour-by (default = %(default)s),
             see https://colorcet.holoviz.org""")
    group_opts.add_argument("--bmap", default="pride",
        help="""Colorcet map used when colouring by a continuous axis (default = %(default)s)""")
    group_opts.add_argument("--dmap", default="glasbey_dark",
        help="""Colorcet map used when colouring by a discrete axis (default = %(default)s)""")
    group_opts.add_argument("--dmap-preserve", action="store_true",
        help="""Preserve colour assignments in discrete axes even when discrete values are missing.""")
    group_opts.add_argument("--min-alpha", default=40, type=int, metavar="0-255",
        help="""Minimum alpha value used in rendering the canvas.
             Increase to saturate colour at the expense of dynamic range. Default is %(default)s.""")
    group_opts.add_argument("--saturate-perc", default=95, type=int, metavar="0-100",
        help="""Saturate colors so that the range [min-alpha, X] is mapped to [min-alpha, 255],
             where X is the given percentile. Default is %(default)s.""")
    group_opts.add_argument("--saturate-alpha", default=None, type=int, metavar="0-255",
        help="""Saturate colors as above, but with a fixed value of X. Overrides --saturate-perc.""")
    group_opts.add_argument("--spread-pix", type=int, default=0, metavar="PIX",
        help="Dynamically spread rendered pixels to this size")
    group_opts.add_argument("--spread-thr", type=float, default=0.5, metavar="THR",
        help="""Threshold parameter for spreading (0 to 1, default %(default)s)""")
    group_opts.add_argument("--bgcol", default="FFFFFF",
        help="RGB hex code for background colour (default = %(default)s)")
    group_opts.add_argument("--fontsize", default=16, type=float,
        help="Font size for all text elements (default = %(default)s)")

    group_opts.add_argument('--hline', type=str, metavar="Y[-[colour]]", 
                      help="Draw horizontal line(s) at given Y coordinate(s). You can append a matplotlib linestyle "
                           "(-, --, -., :) and/or a colour. You can also use a comma-separated list.")
    group_opts.add_argument('--vline', type=str, metavar="X[-[colour]]", 
                      help="Draw vertical line at given X coordinate(s). You can append a matplotlib linestyle "
                           "(-, --, -., :) and/or a colour. You can also use a comma-separated list.")
    group_opts.add_argument('-M', '--markup', type=str, action="append", nargs=2, metavar="func {args}", 
                      help="Add arbitrary matplotlib markup to plot. 'func' is a function known to matplotlib.Axes. "
                           "The {args} dict should be in YaML format.")

    group_opts = parser.add_argument_group("Output settings")
    # can also use "plot-{msbase}-{column}-{corr}-{xfullname}-vs-{yfullname}", let's expand on this later
    group_opts.add_argument("--dir",
        help="Send all plots to this output directory")
    group_opts.add_argument("-s", "--suffix",
        help="suffix to be included in filenames, can include {options}")
    group_opts.add_argument("--png", dest="pngname",
        default="plot-{ms}{_field}{_Spw}{_Scan}{_Ant}{_Baseline}-{label}{_alphalabel}{_colorlabel}{_suffix}.png",
        help='Template for output png files, default "%(default)s"')
    group_opts.add_argument("--title",
        default="{ms}{_field}{_Spw}{_Scan}{_Ant}{_Baseline}{_title}{_Alphatitle}{_Colortitle}",
        help='Template for plot titles, default "%(default)s"')
    group_opts.add_argument("--xlabel", default="{xname}{_xunit}",
        help='Template for X axis labels, default "%(default)s"')
    group_opts.add_argument("--ylabel", default="{yname}{_yunit}",
        help='Template for X axis labels, default "%(default)s"')

    optimization_opts = parser.add_argument_group("Performance & tweaking")
    optimization_opts.add_argument("-d", "--debug", action="store_true",
        help="Enable debugging output")
    optimization_opts.add_argument("-z", "--row-chunk-size", type=int, metavar="NROWS", default=5000,
        help="""Row chunk size for dask-ms.
             Larger chunks may or may not be faster, but will certainly use more RAM.""")
    optimization_opts.add_argument("-j", "--num-parallel", type=int, metavar="N", default=1,
        help=f"""Run up to N renderers in parallel.
             Default is serial.
             Use -j0 to auto-set this to half the available cores ({DEFAULT_NUM_RENDERS} on this system).
             This is not necessarily faster, as they might all end up contending for disk I/O.
             This might also work against dask-ms's own intrinsic parallelism.
             You have been advised.""")
    optimization_opts.add_argument("--profile", action="store_true",
        help="Enable dask profiling output")

    return parser, optimization_opts


# -- parser utility functions --
# flatten append lists generated by argparse.actions
flatten_list = lambda list_: list(itertools.chain(*[str_.split(',') for str_ in list_]))
unpack_axis = lambda opts_axis, default: [default] if not opts_axis else flatten_list(opts_axis)
# validate limits on axes, both min and max limits must be present
eval_none = lambda arr_1, arr_2: any([(a is None)^(b is None) for a, b in zip(arr_1, arr_2)])
# -- parser utility functions --


# -- dealing with input arguments to extract expected parameters --
def get_list(list_, type_=str, default=None, set_len=-1):
    """
        all parameter lists must be the same length as the expected number of plot
        and can also impose a type such as float (returning None for an empty string)

        parameters
        ----------
        list_: list
            appended list of input options
        type_: [optional] object
            desired variable type: e.g. int, float, str
            type_=str assumed if not specified
        default: [optional] object
            appropriate default value for variable
            default=None assumed if not specified
        set_len: [optional] int
            expected number of plots

        returns
        -------
        elems: list
            list of parameter options to plot,
            with a parameter for each plot to generate
    """
    # if empty list, set default
    if not list_:
        elems = [default]
    else:
        # stick all lists together
        elems = flatten_list(list_)
        # convert type
        elems = list(map(type_, elems))
    if len(elems) == 1 and set_len > 0:
        elems = [elems[0]]*set_len
    return elems


def parse_slice_spec(spec):
    """ check channel slice

        parameters
        ----------
        spec: str
            format [start]:[stop]:[step]

        returns
        -------
        slice: object
            python slice object
        elems: list
            slice element ([start, [stop, [step]]])
    """
    if spec:
        spec_elems = [int(x) if x else None for x in spec.split(":", 2)]
        return slice(*spec_elems), spec_elems
    else:
        return slice(None), []


def parse_plot_spec(parser, options):
    """
        unpacking the plotting input arguments into variables for plotting and
        making use the specified parameters makes sense for plotting

        parameters
        ----------
        parser: object
            raw ArgumentParser object
        options: object
            Namespace object containing parsed attributes

        returns
        -------
        xaxes: list
            xaxis parameter for each figure
        yaxes: list
            yaxis parameter for each figure
        columns: list
            name of visibility column per figure
        caxes: list
            color axis per figure (if specified, else None)
        aaxes: list
            blah, blah, blah, ...
        areds: list
            blah, blah, blah, ...
        xmins: list
            x-range minimum per figure (if specified, else None)
        xmaxs: list
            x-range maximum per figure (if specified, else None)
        ymins: list
            y-range minimum per figure (if specified, else None)
        ymaxs: list
            y-range maximum per figure (if specified, else None)
        amins: list
            blah, blah, blah, ...
        amaxs: list
            blah, blah, blah, ...
        cmins: list
            minimum colormap value per figure (if specified, else None)
        cmaxs: list
            maximum colormap value per figure (if specified, else None)
        cnums: list
            number of steps in colormap range
        chanslice: slice object (if channel selection is specified else All)
            easy selection of specified channels
    """

    # figure our list of plots to make
    # list of xaxis options to plot (default ['TIME'])
    xaxes = unpack_axis(options.xaxis, 'TIME')
    # list of yaxis associated with xaxes options (default['DATA:amp'])
    yaxes = unpack_axis(options.yaxis, 'DATA:amp')
    if len(xaxes) != len(yaxes):
        parser.error("--xaxis and --yaxis must be given the same number of times")

    # get list of columns and plot limits of the same length
    param_desc = {
                 'names': ('vars', 'opts', 'types', 'defaults'),
                 'formats': ('U15', 'U15', object, object)
                 }
    plot_params = numpy.array([
                              ('columns', 'col', str, ["DATA"]) ,
                              ('xmins', 'xmin', float, None),
                              ('xmaxs', 'xmax', float, None),
                              ('ymins', 'ymin', float, None),
                              ('ymaxs', 'ymax', float, None),
                              ('aaxes', 'aaxis', str, None),
                              ('amins', 'amin', float, None),
                              ('amaxs', 'amax', float, None),
                              ('areds', 'ared', str, 'mean'),
                              ('caxes', 'colour_by', str, None),
                              ('cmins', 'cmin', float, None),
                              ('cmaxs', 'cmax', float, None),
                              ('cnums', 'cnum', int, DEFAULT_CNUM),
                              ], dtype=param_desc)
    if not options.col:
        options.col = ["DATA"]
    # For all other settings, returns list same length as xaxes,
    # or throws error if no conformance.
    for param, name, dtype, default in zip(plot_params['vars'],
                                           plot_params['opts'],
                                           plot_params['types'],
                                           plot_params['defaults']):
        elems =  get_list(getattr(options, name, None),
                          type_=dtype,
                          default=default,
                          set_len=len(xaxes),
                          )
        if len(elems) > 1 and len(elems) != len(xaxes):
            parser.error(f"--{name} must be given the same number of times as --xaxis, or else just once")
        #variable is dynamically created and assigned
        globals()[param] = elems

    # check min/max
    if eval_none(xmins, xmaxs):
        parser.error("--xmin/--xmax must be either both set, or neither")
    if eval_none(ymins, ymaxs):
        parser.error("--ymin/--ymax must be either both set, or neither")
    if eval_none(cmins, cmaxs):
        parser.error("--cmin/--cmax must be either both set, or neither")
    if eval_none(amins, amaxs):
        parser.error("--amin/--amax must be either both set, or neither")

    return [xaxes, yaxes, columns, caxes, aaxes, areds,
            xmins, xmaxs, ymins, ymaxs, amins, amaxs, cmins, cmaxs, cnums]

# -fin-
