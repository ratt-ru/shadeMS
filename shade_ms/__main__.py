# -*- coding: future_fstrings -*-

import argparse
import pkg_resources

from . import DEFAULT_CNUM, DEFAULT_NUM_RENDERS

try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"


def uppercase(str_):
    return str_.upper()


def cli(argv):

    description=("""Rapid Measurement Set plotting with dask-ms and datashader.
                  Version {0:s}"""
                  .format(__version__))
    parser = argparse.ArgumentParser(
             description=description,
             )

    parser.add_argument(
        "ms",
        help="Measurement set",
        )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version="{:s} version {:s}".format(parser.prog, __version__),
        )

    group_opts = parser.add_argument_group("Plot types and data sources")
    group_opts.add_argument(
        "-x", "--xaxis",
        action="append",
        help="""X axis of plot, e.g. "amp:CORRECTED_DATA"
             This recognizes all column names (also CHAN, FREQ, CORR, ROW, WAVEL, U, V, W, UV)
             and, for complex columns, keywords such as 'amp', 'phase', 'real', 'imag'.
             You can also specify correlations, e.g. 'DATA:phase:XX',
             and do two-column arithmetic with "+-*/", e.g. 'DATA-MODEL_DATA:amp'.
             Correlations may be specified by label, number, or as a Stokes parameter.
             The order of specifiers does not matter.
             """,
        )
    group_opts.add_argument(
        "-y", "--yaxis",
        action="append",
        help="""Y axis to plot.
             Must be given the same number of times as --xaxis.
             Note that X/Y can employ different columns and correlations.
             """,
        )
    group_opts.add_argument(
        "-a", "--aaxis",
        action="append",
        help="""Intensity axis.
             Can be none, or given once, or given the same number of times as --xaxis.
             If none, plot intensity (a.k.a. alpha channel) is proportional to density of points.
             Otherwise, a reduction function (see --ared below) is applied to the given values,
             and the result is used to determine intensity.
             All columns and variations listed under --xaxis are available for --aaxis.
             """,
        )
    group_opts.add_argument(
        "--ared",
        action="append",
        help="""Alpha axis reduction function.
             Recognized reductions are count, any, sum, min, max,
             mean, std, first, last, mode. Default is mean.
             """,
        )

    group_opts.add_argument(
        "-c", "--colour-by",
        action="append",
        help="""Colour (a.k.a. category) axis.
             Can be none, or given once, or given the same number of times as --xaxis.
             All columns and variations listed under --xaxis are available for colouring by.
             """,
        )
    group_opts.add_argument(
        "-C", "--col",
        metavar="COLUMN",
        dest="col",
        action="append",
        default=[],
        help="""Name of visibility column (default is DATA), if needed.
             This is used if the axis specifications do not explicitly include a column.
             For multiple plots, this can be given multiple times, or as a comma-separated list.
             Two-column arithmetic is recognized.
             """,
        )

    group_opts.add_argument(
        "--noflags",
        action="store_true",
        help="Enable to ignore flags. Default is to omit flagged data.",
        )
    group_opts.add_argument(
        "--noconj",
        action="store_true",
        help="Do not show conjugate points in u,v plots (default = plot conjugates).",
        )

    group_opts = parser.add_argument_group("Plot axes setup")
    group_opts.add_argument(
        "--xmin",
        action="append",
        help="""Minimum x-axis value (default = data min).
             For multiple plots, you can give this multiple times,
             or use a comma-separated list,
             but note that the clipping is the same per axis across all plots,
             so only the last applicable setting will be used.
             The list may include empty elements (or 'None') to not apply a clip.
             Default computes clips from data min/max.
             """,
        )
    group_opts.add_argument(
        "--xmax",
        action="append",
        help="Maximum x-axis value.",
        )
    group_opts.add_argument(
        "--ymin",
        action="append",
        help="Minimum y-axis value.",
        )
    group_opts.add_argument(
        "--ymax",
        action="append",
        help="Maximum y-axis value.",
        )
    group_opts.add_argument(
        "--amin",
        action="append",
        help="Minimum intensity-axis value.",
        )
    group_opts.add_argument(
        "--amax",
        action="append",
        help="Maximum intensity-axis value.",
        )
    group_opts.add_argument(
        "--cmin",
        action="append",
        help="Minimum value to be coloured by.",
        )
    group_opts.add_argument(
        "--cmax",
        action="append",
        help="Maximum value to be coloured by.",
        )
    group_opts.add_argument(
        "--cnum",
        action="append",
        help=f"""Number of steps used to discretize a continuous axis.
             Default is {DEFAULT_CNUM}.
             """,
        )

    group_opts.add_argument(
        "--xlim-load",
        action="store_true",
        help=f"Load x-axis limits from limits file, if available.",
        )
    group_opts.add_argument(
        "--ylim-load",
        action="store_true",
        help=f"Load y-axis limits from limits file, if available.",
        )
    group_opts.add_argument(
        "--clim-load",
        action="store_true",
        help=f"Load colour axis limits from limits file, if available.",
        )
    group_opts.add_argument(
        "--lim-file",
        default="{ms}-minmax-cache.json",
        help="""Name of limits file to save/load. '{ms}' will be substituted for MS base name.
             Default is '%(default)s'.
             """,
        )
    group_opts.add_argument(
        "--no-lim-save",
        action="store_false",
        dest="lim_save",
        help="""Do not save auto-computed limits to limits file.
             Default is to save.
             """,
        )
    group_opts.add_argument(
        "--lim-save-reset",
        action="store_true",
        help="""Reset limits file when saving.
             Default adds to existing file.
             """,
        )

    group_opts = parser.add_argument_group(
        "Options for multiple plots or combined plots"
        )
    group_opts.add_argument(
        "--iter-field",
        action="store_true",
        help="Separate plots per field (default is to combine in one plot)",
        )
    group_opts.add_argument(
        "--iter-spw",
        action="store_true",
        help="Separate plots per spw (default is to combine in one plot)",
        )
    group_opts.add_argument(
        "--iter-scan",
        action="store_true",
        help="Separate plots per scan (default is to combine in one plot)",
        )
    group_opts.add_argument(
        "--iter-corr",
        action="store_true",
        help="Separate plots per correlation or Stokes (default is to combine in one plot)",
        )

    ex_group_opts = group_opts.add_mutually_exclusive_group()
    ex_group_opts.add_argument(
        "--iter-ant",
        action="store_true",
        help="Separate plots per antenna (default is to combine in one plot)",
        )
    ex_group_opts.add_argument(
        "--iter-baseline",
        action="store_true",
        help="Separate plots per baseline (default is to combine in one plot)",
        )

    group_opts = parser.add_argument_group("Data subset selection")
    group_opts.add_argument(
        "--ant",
        default="all",
        help="Antennas to plot (comma-separated list of names, default = all)",
        )
    group_opts.add_argument(
        "--ant-num",
        help="""Antennas to plot (comma-separated list of numbers,
             or a [start]:[stop][:step] slice, overrides --ant)
             """,
        )
    group_opts.add_argument(
        "--baseline",
        default="noauto",
        help="""Baselines to plot, as 'ant1-ant2'
             (comma-separated list,
             default of 'noauto' omits auto-correlations,
             use 'all' to select all)
             """,
        )
    group_opts.add_argument(
        "--spw",
        default="all",
        help="""Spectral windows (DDIDs) to plot
             (comma-separated list, default = all)
             """,
        )
    group_opts.add_argument(
        "--field",
        default="all",
        help="Field ID(s) to plot (comma-separated list, default = all)",
        )
    group_opts.add_argument(
        "--scan",
        default="all",
        help="Scans to plot (comma-separated list, default = all)",
        )
    group_opts.add_argument(
        "--corr",
        default="all",
        type=uppercase,  # need this argument in upper case
        help="""Correlations or Stokes to plot, use indices or labels
             (comma-separated list, default = all)
             """,
        )
    group_opts.add_argument(
        "--chan",
        help="""Channel slice, as [start]:[stop][:step],
             default is to plot all channels
             """,
        )

    group_opts = parser.add_argument_group("Rendering settings")
    group_opts.add_argument(
        "-X", "--xcanvas",
        default=1280,
        type=int,
        help="Canvas x-size in pixels (default = %(default)s)",
        )
    group_opts.add_argument(
        "-Y", "--ycanvas",
        default=900,
        type=int,
        help="Canvas y-size in pixels (default = %(default)s)",
        )
    group_opts.add_argument(
        "--norm",
        choices=["auto", "eq_hist", "cbrt", "log", "linear"],
        default="auto",
        help="""Pixel scale normalization
             (default is 'log' with caxis,
             'linear' with aaxis, and
             'eq_hist' when neither is in use)
             """,
        )
    group_opts.add_argument(
        "--cmap",
        default="bkr",
        help="""Colorcet map used without --colour-by
             (default = %(default)s),
             see https://colorcet.holoviz.org""",
        )
    group_opts.add_argument(
        "--bmap",
        default="pride",
        help="""Colorcet map used when colouring by a continuous axis
             (default = %(default)s)
             """,
        )
    group_opts.add_argument(
        "--dmap",
        default="glasbey_dark",
        help="""Colorcet map used when colouring by a discrete axis
             (default = %(default)s)
             """,
        )
    group_opts.add_argument(
        "--dmap-preserve",
        action="store_true",
        help="""Preserve colour assignments in discrete axes
             even when discrete values are missing.
             """,
        )
    group_opts.add_argument(
        "--min-alpha",
        default=40,
        type=int,
        metavar="0-255",
        help="""Minimum alpha value used in rendering the canvas.
             Increase to saturate colour at the expense of dynamic range.
             Default is %(default)s.
             """,
        )
    group_opts.add_argument(
        "--saturate-perc",
        default=95,
        type=int,
        metavar="0-100",
        help="""Saturate colors so that the
             range [min-alpha, X] is mapped to [min-alpha, 255],
             where X is the given percentile. Default is %(default)s.
             """,
        )
    group_opts.add_argument(
        "--saturate-alpha",
        default=None,
        type=int,
        metavar="0-255",
        help="""Saturate colors as above, but with a fixed value of X.
             Overrides --saturate-perc.
             """,
        )
    group_opts.add_argument(
        "--spread-pix",
        type=int,
        default=0,
        metavar="PIX",
        help="Dynamically spread rendered pixels to this size",
        )
    group_opts.add_argument(
        "--spread-thr",
        type=float,
        default=0.5,
        metavar="THR",
        help="""Threshold parameter for spreading
             (0 to 1, default %(default)s)
             """,
        )
    group_opts.add_argument(
        "--bgcol",
        default="FFFFFF",
        help="RGB hex code for background colour (default = FFFFFF)",
        )
    group_opts.add_argument(
        "--fontsize",
        default=16,
        help="Font size for all text elements (default = 20)",
        )

    group_opts = parser.add_argument_group("Output settings")
    # can also use "plot-{msbase}-{column}-{corr}-{xfullname}-vs-{yfullname}", let's expand on this later
    group_opts.add_argument(
        "--dir",
        help="Send all plots to this output directory"
        )
    group_opts.add_argument(
        "-s", "--suffix",
        help="suffix to be included in filenames, can include {options}",
        )
    group_opts.add_argument(
        "--png",
        dest="pngname",
        default="plot-{ms}{_field}{_Spw}{_Scan}{_Ant}{_Baseline}-{label}{_alphalabel}{_colorlabel}{_suffix}.png",
        help='Template for output png files, default "%(default)s"',
        )
    group_opts.add_argument(
        "--title",
        default="{ms}{_field}{_Spw}{_Scan}{_Ant}{_Baseline}{_title}{_Alphatitle}{_Colortitle}",
        help='Template for plot titles, default "%(default)s"',
        )
    group_opts.add_argument(
        "--xlabel",
        default="{xname}{_xunit}",
        help='Template for X axis labels, default "%(default)s"',
        )
    group_opts.add_argument(
        "--ylabel",
        default="{yname}{_yunit}",
        help='Template for X axis labels, default "%(default)s"',
        )

    opt_group_opts = parser.add_argument_group("Performance & tweaking")
    opt_group_opts.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debugging output"
        )
    opt_group_opts.add_argument(
        "-z", "--row-chunk-size",
        type=int,
        metavar="NROWS",
        default=5000,
        help="""Row chunk size for dask-ms.
             Larger chunks may or may not be faster,
             but will certainly use more RAM.
             """,
        )
    opt_group_opts.add_argument(
        "-j", "--num-parallel",
        type=int,
        metavar="N",
        default=1,
        help=f"""Run up to N renderers in parallel.
             Default is serial.
             Use -j0 to auto-set this to half the available cores
             ({DEFAULT_NUM_RENDERS} on this system).
             This is not necessarily faster, as they might all end up
             contending for disk I/O.
             This might also work against dask-ms's own intrinsic parallelism.
             You have been advised.
             """,
        )
    opt_group_opts.add_argument(
        "--profile",
        action="store_true",
        help="Enable dask profiling output"
        )

    options = parser.parse_args(argv)

    options.ms = options.ms.rstrip('/')

    return options, opt_group_opts

# -fin-
