# -*- coding: future_fstrings -*-

# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import daskms
import dask.array as da
import dask.dataframe as dask_df
import xarray
import holoviews as holoviews
import holoviews.operation.datashader
import datashader.transfer_functions
import datashader.reductions
from datashader.reductions import category_modulo, category_binning
import numpy as np
import pylab
import textwrap
import argparse
import matplotlib.cm
from shade_ms import log

from collections import OrderedDict
from . import data_mappers
from .data_mappers import DataAxis
from .dask_utils import dataframe_factory
# from .ds_ext import by_integers, by_span

USE_REDUCE_BY = False

def add_options(parser):
    parser.add_argument('--reduce-by',  action="store_true", help=argparse.SUPPRESS)

def set_options(options):
    global USE_REDUCE_BY
    USE_REDUCE_BY = options.reduce_by


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff

def get_plot_data(msinfo, group_cols, mytaql, chan_freqs,
                  chanslice, subset,
                  noflags, noconj,
                  iter_field, iter_spw, iter_scan, iter_ant,
                  join_corrs=False,
                  row_chunk_size=100000):

    ms_cols = {'ANTENNA1', 'ANTENNA2'}
    if not noflags:
        ms_cols.update({'FLAG', 'FLAG_ROW'})
    # get visibility columns
    for axis in DataAxis.all_axes.values():
        ms_cols.update(axis.columns)

    total_num_points = 0  # total number of points to plot

    # output dataframes, indexed by (field, spw, scan, antenna, correlation)
    # If any of these axes is not being iterated over, then the index is None
    output_dataframes = OrderedDict()

    if iter_ant:
        antenna_subsets = zip(subset.ant.numbers, subset.ant.names)
    else:
        antenna_subsets = [(None, None)]
    taql = mytaql

    for antenna, antname in antenna_subsets:
        if antenna is not None:
            taql = f"({mytaql})&&(ANTENNA1=={antenna} || ANTENNA2=={antenna})" if mytaql else \
                    f"(ANTENNA1=={antenna} || ANTENNA2=={antenna})"
        # get MS data
        msdata = daskms.xds_from_ms(msinfo.msname, columns=list(ms_cols), group_cols=group_cols, taql_where=taql,
                                    chunks=dict(row=row_chunk_size))
        nrow = sum([len(group.row) for group in msdata])
        if not nrow:
            continue

        if antenna is not None:
            log.info(f': Indexing sub-MS (antenna {antname}) and building dataframes ({nrow} rows, chunk size is {row_chunk_size})')
        else:
            log.info(f': Indexing MS and building dataframes ({nrow} rows, chunk size is {row_chunk_size})')

        # iterate over groups
        for group in msdata:
            ddid     =  group.DATA_DESC_ID  # always present
            fld      =  group.FIELD_ID # always present
            if fld not in subset.field or ddid not in subset.spw:
                log.debug(f"field {fld} ddid {ddid} not in selection, skipping")
                continue
            scan    = getattr(group, 'SCAN_NUMBER', None)  # will be present if iterating over scans

            # always read flags -- easier that way
            flag = group.FLAG if not noflags else None
            flag_row = group.FLAG_ROW if not noflags else None

            a1 = da.minimum(group.ANTENNA1.data, group.ANTENNA2.data)
            a2 = da.maximum(group.ANTENNA1.data, group.ANTENNA2.data)

            baselines = a1*len(msinfo.antenna) - a1*(a1-1)//2 + a2

            freqs = chan_freqs[ddid]
            chans = xarray.DataArray(range(len(freqs)), dims=("chan",))
            wavel = freq_to_wavel(freqs)
            extras = dict(chans=chans, freqs=freqs, wavel=wavel, rows=group.row, baselines=baselines)

            nchan = len(group.chan)
            if flag is not None:
                flag = flag[dict(chan=chanslice)]
                nchan = flag.shape[1]
            shape = (len(group.row), nchan)

            arrays = OrderedDict()
            shapes = OrderedDict()
            ddf = None
            num_points = 0  # counts number of new points generated

            for corr in subset.corr.numbers:
                # make dictionary of extra values for DataMappers
                extras['corr'] = corr
                # loop over datums to be computed
                for axis in DataAxis.all_axes.values():
                    value = arrays.get(axis.label)
                    # a datum was already computed?
                    if value is not None:
                        # if not joining correlations, then that's the only one we'll need, so continue
                        if not join_corrs:
                            continue
                        # joining correlations, and datum has a correlation dependence: compute another one
                        if axis.corr is None:
                            value = None
                    if value is None:
                        value = axis.get_value(group, corr, extras, flag=flag, flag_row=flag_row, chanslice=chanslice)
                        # print(axis.label, value.compute().min(), value.compute().max())
                        num_points = max(num_points, value.size)
                        if value.ndim == 0:
                            shapes[axis.label] = ()
                        elif value.ndim == 1:
                            timefreq_axis = axis.mapper.axis or 0
                            assert value.shape[0] == shape[timefreq_axis], \
                                   f"{axis.mapper.fullname}: size {value.shape[0]}, expected {shape[timefreq_axis]}"
                            shapes[axis.label] = ("row",) if timefreq_axis == 0 else ("chan",)
                        # else 2D value better match expected shape
                        else:
                            assert value.shape == shape, f"{axis.mapper.fullname}: shape {value.shape}, expected {shape}"
                            shapes[axis.label] = ("row", "chan")
                        arrays[axis.label] = value
                # any new data generated for this correlation? Make dataframe
                if num_points:
                    total_num_points += num_points
                    args = (v for pair in ((array, shapes[key]) for key, array in arrays.items()) for v in pair)
                    df1 = dataframe_factory(("row", "chan"), *args, columns=arrays.keys())
                    # if any axis needs to be conjugated, double up all of them
                    if not noconj and any([axis.conjugate for axis in DataAxis.all_axes.values()]):
                        args = (v for pair in ((-array if DataAxis[key].conjugate else array, shapes[key])
                                                for key, array in arrays.items()) for v in pair)
                        df2 = dataframe_factory(("row", "chan"), *args, columns=arrays.keys())
                        df1 = dask_df.concat([df1, df2], axis=0)
                    ddf = dask_df.concat([ddf, df1], axis=0) if ddf is not None else df1

            # now, are we iterating or concatenating? Make frame key accordingly
            dataframe_key = (fld if iter_field else None,
                             ddid if iter_spw else None,
                             scan if iter_scan else None,
                             antenna)

            # do we already have a frame for this key
            ddf0 = output_dataframes.get(dataframe_key)

            if ddf0 is None:
                log.debug(f"first frame for {dataframe_key}")
                output_dataframes[dataframe_key] = ddf
            else:
                log.debug(f"appending to frame for {dataframe_key}")
                output_dataframes[dataframe_key] = dask_df.concat([ddf0, ddf], axis=0)

    # convert discrete axes into categoricals
    if data_mappers.USE_COUNT_CAT:
        categorical_axes = [axis.label for axis in DataAxis.all_axes.values() if axis.nlevels]
        if categorical_axes:
            log.info(": counting colours")
            for key, ddf in list(output_dataframes.items()):
                output_dataframes[key] = ddf.categorize(categorical_axes)

    # print("===")
    # for ddf in output_dataframes.values():
    #     for axis in DataAxis.all_axes.values():
    #         value = ddf[axis.label].values.compute()
    #         print(axis.label, np.nanmin(value), np.nanmax(value))

    log.info(": complete")
    return output_dataframes, total_num_points

def compute_bounds(unknowns, bounds, ddf):
    """
    Given a list of axis with unknown bounds, computes missing bounds and updates the bounds dict
    """
    # setup function to compute min/max on every column for which we don't have a min/max
    r = ddf.map_partitions(lambda df:
            np.array([[(np.nanmin(df[axis].values).item() if bounds[axis][0] is None else bounds[axis][0]) for axis in unknowns]+
                      [(np.nanmax(df[axis].values).item() if bounds[axis][1] is None else bounds[axis][1]) for axis in unknowns]]),
        ).compute()

    # setup new bounds dict based on this
    for i, axis in enumerate(unknowns):
        minval = np.nanmin(r[:, i])
        maxval = np.nanmax(r[:, i + len(unknowns)])
        if not (np.isfinite(minval) and np.isfinite(maxval)):
            minval, maxval = -1.0, 1.0
        elif minval >= maxval:
            minval, maxval = minval-1, minval+1
        bounds[axis] = minval, maxval



def create_plot(ddf, xdatum, ydatum, adatum, ared, cdatum, cmap, bmap, dmap, normalize,
                xlabel, ylabel, title, pngname,
                options=None):

    figx = options.xcanvas / 60
    figy = options.ycanvas / 60
    bgcol = "#" + options.bgcol.lstrip("#")

    xaxis = xdatum.label
    yaxis = ydatum.label
    aaxis = adatum and adatum.label
    caxis = cdatum and cdatum.label

    color_key = color_mapping = color_labels = agg_alpha = raster_alpha = cmin = cdelta = None

    bounds = OrderedDict({xaxis: xdatum.minmax, yaxis: ydatum.minmax})
    if caxis:
        bounds[caxis] = cdatum.minmax

    unknown = [axis for (axis, minmax) in bounds.items() if minmax[0] is None or minmax[1] is None]

    if unknown:
        log.info(f": scanning axis min/max for {' '.join(unknown)}")
        compute_bounds(unknown, bounds, ddf)

    canvas = datashader.Canvas(options.xcanvas, options.ycanvas, x_range=bounds[xaxis], y_range=bounds[yaxis])

    if aaxis is not None:
        agg_alpha = getattr(datashader.reductions, ared, None)
        if agg_alpha is None:
            raise ValueError(f"unknown alpha reduction function {ared}")
        agg_alpha = agg_alpha(aaxis)
    ared = ared or 'count'

    if aaxis is not None:
        agg_alpha = getattr(datashader.reductions, ared, None)
        if agg_alpha is None:
            raise ValueError(f"unknown alpha reduction function {ared}")
        agg_alpha = agg_alpha(aaxis)
    ared = ared or 'count'

    if cdatum is not None:
        if agg_alpha is not None and not USE_REDUCE_BY:
            log.debug(f'rasterizing alpha channel using {ared}(aaxis)')
            raster_alpha = canvas.points(ddf, xaxis, yaxis, agg=agg_alpha)

        # aggregation applied to by()
        agg_by = agg_alpha if USE_REDUCE_BY and agg_alpha is not None else datashader.count()

        if data_mappers.USE_COUNT_CAT:
            color_bins = [int(x) for x in getattr(ddf.dtypes, caxis).categories]
            log.debug(f'colourizing using {caxis} categorical, {len(color_bins)} bins')
            category = caxis
        else:
            color_bins = list(range(cdatum.nlevels))
            if cdatum.is_discrete:
                log.debug(f'colourizing using {caxis} modulo {len(color_bins)}')
                category = category_modulo(caxis,  cdatum.nlevels)
            else:
                log.debug(f'colourizing using {caxis} with {len(color_bins)} bins')
                cmin = bounds[caxis][0]
                cdelta = (bounds[caxis][1] - cmin) / cdatum.nlevels
                category = category_binning(caxis, cmin, cdelta, cdatum.nlevels)

        raster = canvas.points(ddf, xaxis, yaxis, agg=datashader.by(category, agg_by))
        is_integer_raster = np.issubdtype(raster.dtype, np.integer)

        # the binning aggregator accumulates flagged points in an extra raster plane
        if isinstance(category, category_binning):
            if is_integer_raster:
                log.info(f": {raster[..., -1].data.sum():.3g} points were flagged ")
            raster = raster[...,:-1]

        if is_integer_raster:
            non_empty = np.array(raster.any(axis=(0, 1)))
        else:
            non_empty = ~(np.isnan(raster.data).all(axis=(0, 1)))
        if not non_empty.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None

        # # work around https://github.com/holoviz/datashader/issues/899
        # # Basically, 0 is treated as a nan and masked out in _colorize(), which is not correct for float reductions.
        # # Also, _colorize() does not normalize the totals somehow.
        # if np.issubdtype(raster.dtype, np.bool_):
        #     pass
        # elif np.issubdtype(raster.dtype, np.integer):
        #     ## TODO: unfinished business here
        #     ## normalizing the raster bleaches out all colours again (fucks with log scaling, I guess?)
        #     # int values: simply normalize to max total 1. Null values will be masked
        #     # raster = raster.astype(np.float32) / raster.sum(axis=2).max()
        #     pass
        # else:
        #     # float values: first rescale raster to [0.001, 1]. Not 0, because 0 is masked out in _colorize()
        #     maxval = np.nanmax(raster)
        #     offset = np.nanmin(raster)
        #     raster = .001 + .999*(raster - offset)/(maxval - offset)
        #     # replace NaNs with zeroes (because when we take the total, and 1 channel is present while others are missing...)
        #     raster.data[np.isnan(raster.data)] = 0
        #     # now rescale so that max total is 1
        #     raster /= raster.sum(axis=2).max()

        if cdatum.is_discrete:
            # discard empty bins
            non_empty = np.where(non_empty)[0]
            raster = raster[..., non_empty]
            # just use bin numbers to look up a color directly
            color_bins = [color_bins[i] for i in non_empty]
            color_key = [dmap[bin] for bin in color_bins]
            # the numbers may be out of order -- reorder for color bar purposes
            bin_color = sorted(zip(color_bins, color_key))
            color_mapping = [col for _, col in bin_color]
            if bounds[caxis][1] > cdatum.nlevels:
                color_labels = [f"+{bin}" for bin, _ in bin_color]
            else:
                if cdatum.discretized_labels and len(cdatum.discretized_labels) <= cdatum.nlevels:
                    color_labels = [cdatum.discretized_labels[bin] for bin, _ in bin_color]
                else:
                    color_labels = [f"{bin}" for bin, _ in bin_color]
            log.info(f": rendering using {len(color_bins)} colors (values {' '.join(color_labels)})")
        else:
            # color labels are bin centres
            bin_centers = [cmin + cdelta*(i+0.5) for i in color_bins]
            # map to colors pulled from 256 color map
            color_key = [bmap[(i*256)//cdatum.nlevels] for i in color_bins]
            color_labels = list(map(str, bin_centers))
            log.info(f": shading using {len(color_bins)} colors (bin centres are {' '.join(color_labels)})")

        if raster_alpha is not None:
            amin = adatum.minmax[0] if adatum.minmax[0] is not None else np.nanmin(raster_alpha)
            amax = adatum.minnax[1] if adatum.minmax[1] is not None else np.nanmax(raster_alpha)
            raster = raster*(raster_alpha-amin)/(amax-amin)
            raster[raster<0] = 0
            raster[raster>1] = 1
            log.info(f": adjusting alpha (alpha raster was {amin} to {amax})")
        img = datashader.transfer_functions.shade(raster, color_key=color_key, how=normalize)
    else:
        log.debug(f'rasterizing using {ared}')
        raster = canvas.points(ddf, xaxis, yaxis, agg=agg_alpha)
        if not raster.data.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None
        log.debug('shading')
        img = datashader.transfer_functions.shade(raster, cmap=cmap, how=normalize)

    if options.spread_pix:
        img = datashader.transfer_functions.dynspread(img, options.spread_thr, max_px=options.spread_pix)
        log.info(f": spreading ({options.spread_thr} {options.spread_pix})")
    rgb = holoviews.RGB(holoviews.operation.datashader.shade.uint32_to_uint8_xr(img))

    log.debug('done')

    # Set plot limits based on data extent or user values for axis labels

    xmin, xmax = bounds[xaxis]
    ymin, ymax = bounds[yaxis]

    log.debug('rendering image')

    def match(artist):
        return artist.__module__ == 'matplotlib.text'

    fig = pylab.figure(figsize=(figx, figy))
    ax = fig.add_subplot(111, facecolor=bgcol)
    ax.imshow(X=rgb.data, extent=[xmin, xmax, ymin, ymax],
              aspect='auto', origin='lower')
    ax.set_title("\n".join(textwrap.wrap(title, 90)), loc='center')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    # ax.plot(xmin,ymin,'.',alpha=0.0)
    # ax.plot(xmax,ymax,'.',alpha=0.0)

    dx, dy = xmax - xmin, ymax - ymin
    ax.set_xlim([xmin - dx/100, xmax + dx/100])
    ax.set_ylim([ymin - dy/100, ymax + dy/100])

    # set fontsize on everything rendered so far
    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(options.fontsize)

    # colorbar?
    if color_key:
        import matplotlib.colors
        # discrete axis
        if caxis is not None and cdatum.is_discrete:
            norm = matplotlib.colors.Normalize(-0.5, len(color_bins)-0.5)
            ticks = np.arange(len(color_bins))
            colormap = matplotlib.colors.ListedColormap(color_mapping)
        # discretized axis
        else:
            norm = matplotlib.colors.Normalize(*bounds[caxis])
            colormap = matplotlib.colors.ListedColormap(color_key)
            # auto-mark colorbar, since it represents a continuous range of values
            ticks = None

        cb = fig.colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=colormap), ax=ax, ticks=ticks)

        # adjust ticks for discrete axis
        if caxis is not None and cdatum.is_discrete:
            rot = 0
            # adjust fontsize for number of labels
            fs = max(options.fontsize*min(1, 32./len(color_labels)), 6)
            fontdict = dict(fontsize=fs)
            if max([len(lbl) for lbl in color_labels]) > 3 and len(color_labels) < 8:
                rot = 90
                fontdict['verticalalignment'] ='center'
            cb.ax.set_yticklabels(color_labels, rotation=rot, fontdict=fontdict)

    fig.savefig(pngname, bbox_inches='tight')

    pylab.close()

    return pngname

