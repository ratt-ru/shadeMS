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
import itertools
import matplotlib.cm
from shade_ms import log
import colorcet
import cmasher
import matplotlib.cm

from collections import OrderedDict
from . import data_mappers
from .data_mappers import DataAxis
from .dask_utils import dataframe_factory
# from .ds_ext import by_integers, by_span

def add_options(parser):
    pass


def set_options(options):
    pass


def get_colormap(cmap_name):
    cmap = getattr(colorcet, cmap_name, None)
    if cmap:
        log.info(f"using colourmap colorcet.{cmap_name}")
        return cmap
    cmap = getattr(cmasher, cmap_name, None)
    if cmap:
        log.info(f"using colourmap cmasher.{cmap_name}")
    else:
        cmap = getattr(matplotlib.cm, cmap_name, None)
        if cmap is None:
            raise ValueError(f"unknown colourmap {cmap_name}")
        log.info(f"using colourmap matplotplib.cm.{cmap_name}")
    return [ f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}" for r,g,b in cmap.colors ]


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff

def get_plot_data(msinfo, group_cols, mytaql, chan_freqs,
                  chanslice, subset,
                  noflags, noconj,
                  iter_field, iter_spw, iter_scan, iter_ant, iter_baseline,
                  join_corrs=False,
                  row_chunk_size=100000):

    ms_cols = {'ANTENNA1', 'ANTENNA2'}
    ms_cols.update(msinfo.indexing_columns.keys())
    if not noflags:
        ms_cols.update({'FLAG', 'FLAG_ROW'})
    # get visibility columns
    for axis in DataAxis.all_axes.values():
        ms_cols.update(axis.columns)

    total_num_points = 0  # total number of points to plot

    # output dataframes, indexed by (field, spw, scan, antenna_or_baseline)
    # If any of these axes is not being iterated over, then the index at that position is None
    output_dataframes = OrderedDict()

    # output subsets of indexing columns, indexed by same tuple
    output_subsets = OrderedDict()

    if iter_ant:
        antenna_subsets = zip(subset.ant.numbers, subset.ant.names)
    else:
        antenna_subsets = [(None, None)]
    taql = mytaql

    for antenna, antname in antenna_subsets:
        if antenna is not None:
            taql = f"({mytaql})&&(ANTENNA1=={antenna} || ANTENNA2=={antenna})" if mytaql else \
                    f"(ANTENNA1=={antenna} || ANTENNA2=={antenna})"
        # add baselines to group columns
        if iter_baseline:
            group_cols = list(group_cols) + ["ANTENNA1", "ANTENNA2"]

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
            if not len(group.row):
                continue
            ddid     =  group.DATA_DESC_ID  # always present
            fld      =  group.FIELD_ID      # always present
            if fld not in subset.field or ddid not in subset.spw:
                log.debug(f"field {fld} ddid {ddid} not in selection, skipping")
                continue
            scan = getattr(group, 'SCAN_NUMBER', None)  # will be present if iterating over scans
            if iter_baseline:
                ant1    = getattr(group, 'ANTENNA1', None)   # will be present if iterating over baselines
                ant2    = getattr(group, 'ANTENNA2', None)   # will be present if iterating over baselines
                baseline = msinfo.baseline_number(ant1, ant2)
            else:
                baseline = None

            # always read flags -- easier that way
            flag = group.FLAG if not noflags else None
            flag_row = group.FLAG_ROW if not noflags else None

            a1 = da.minimum(group.ANTENNA1.data, group.ANTENNA2.data)
            a2 = da.maximum(group.ANTENNA1.data, group.ANTENNA2.data)
            baselines = msinfo.baseline_number(a1, a2)

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

            # Make frame key -- data subset corresponds to this frame
            dataframe_key = (fld if iter_field else None,
                             ddid if iter_spw else None,
                             scan if iter_scan else None,
                             antenna if antenna is not None else baseline)

            # update subsets of MS indexing columns that we've seen for this dataframe
            output_subset1 = output_subsets.setdefault(dataframe_key,
                                                {column:set() for column in msinfo.indexing_columns.keys()})
            for column, _ in msinfo.indexing_columns.items():
                value = getattr(group, column)
                if np.isscalar(value):
                    output_subset1[column].add(value)
                else:
                    output_subset1[column].update(value.compute().data)

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
                        arr_shape = [(-arrays[axis.label] if axis.conjugate else arrays[axis.label], shapes[axis.label])
                                                for axis in DataAxis.all_axes.values()]
                        args = (v for pair in arr_shape  for v in pair)
                        df2 = dataframe_factory(("row", "chan"), *args, columns=arrays.keys())
                        df1 = dask_df.concat([df1, df2], axis=0)
                    ddf = dask_df.concat([ddf, df1], axis=0) if ddf is not None else df1

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
    return output_dataframes, output_subsets, total_num_points

def compute_bounds(unknowns, bounds, ddf):
    """
    Given a list of axis with unknown bounds, computes missing bounds and updates the bounds dict
    """
    # setup function to compute min/max on every column for which we don't have a min/max
    with np.errstate(all='ignore'):
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



def create_plot(ddf, index_subsets, xdatum, ydatum, adatum, ared, cdatum, cmap, bmap, dmap, normalize,
                xlabel, ylabel, title, pngname,
                min_alpha=40, saturate_percentile=None, saturate_alpha=None,
                minmax_cache=None,
                options=None):

    figx = options.xcanvas / 60
    figy = options.ycanvas / 60
    bgcol = "#" + options.bgcol.lstrip("#")

    xaxis = xdatum.label
    yaxis = ydatum.label
    aaxis = adatum and adatum.label
    caxis = cdatum and cdatum.label

    color_key = color_labels = color_minmax = agg_alpha = None

    # do we need to compute any axis min/max?
    bounds = OrderedDict({xaxis: xdatum.minmax, yaxis: ydatum.minmax})
    unknown = []
    for datum in xdatum, ydatum, cdatum:
        if datum is not None:
            bounds[datum.label] = datum.minmax
            if datum.minmax[0] is None or datum.minmax[1] is None:
                if datum.is_discrete and datum.subset_indices is not None:
                    bounds[datum.label] = 0, len(datum.subset_indices)-1
                else:
                    unknown.append(datum.label)

    if unknown:
        log.info(f": scanning axis min/max for {' '.join(unknown)}")
        compute_bounds(unknown, bounds, ddf)
        # populate cache
        if minmax_cache is not None:
            minmax_cache.update([(label, bounds[label]) for label in unknown])

    # adjust bounds for discrete axes
    canvas_sizes = []
    for datum, size in (xdatum, options.xcanvas), (ydatum, options.ycanvas):
        if datum.is_discrete:
            bounds[datum.label] = bounds[datum.label][0]-0.5, bounds[datum.label][1]+0.5
            size = int(bounds[datum.label][1]) - int(bounds[datum.label][0]) + 1
        canvas_sizes.append(size)

    # create rendering canvas.
    canvas = datashader.Canvas(canvas_sizes[0], canvas_sizes[1], x_range=bounds[xaxis], y_range=bounds[yaxis])

    if aaxis is not None:
        agg_alpha = getattr(datashader.reductions, ared, None) if ared else datashader.reductions.count
        if agg_alpha is None:
            raise ValueError(f"unknown alpha reduction function {ared}")
        agg_alpha = agg_alpha(aaxis)

    if cdatum is not None:
        # aggregation applied to by()
        agg_by = agg_alpha if agg_alpha else datashader.count()

        # figure out mapping from raster planes to colours
        # after this if-else block, category will be an aggregator instance yielding N categories,
        # color_key will be a list of N colors, and color_label will be a list of N textual labels

        if data_mappers.USE_COUNT_CAT:
            cats = getattr(ddf.dtypes, caxis).categories
            log.debug(f'colourizing using {caxis} categorical, {len(cats)} bins')
            category = caxis
            color_key = dmap[:len(cats)]
            color_labels = list(map(str, cats))
        else:
            if cdatum.is_discrete:
                # make dictionary from index to label, omitting values that are not in the MS subset to begin with
                if cdatum.discretized_labels:
                    active_subset = OrderedDict(enumerate(cdatum.discretized_labels))
                # else make up integer labels on the spot
                else:
                    active_subset = OrderedDict(enumerate(map(str, range(bounds[caxis][1]+1))))
                # Check if the subset needs to be refined, because it is known to be smaller for this dataframe
                if cdatum.columns[0] in index_subsets and len(cdatum.columns) == 1:
                    df_index_subset = index_subsets[cdatum.columns[0]]
                    if cdatum.subset_remapper is not None:
                        remapper = cdatum.subset_remapper.compute()
                        df_index_subset = set(remapper[x] for x in df_index_subset)
                    active_subset = OrderedDict((idx, active_subset[idx]) for idx in df_index_subset)
                    log.debug(f"subset of indices for this axis is a priori {list(active_subset.keys())}")
                # max known index
                max_index = max(active_subset.keys())
                num_colors = min(cdatum.nlevels, len(dmap))
                color_key = dmap[:num_colors]
                # if we have fewer indices than colour levels, and the max index is sensible, we'll aggregate to one
                # raster slice per index value directly
                if len(active_subset) <= num_colors and max_index < max(num_colors, 256):
                    num_colors = max_index+1
                    log.debug(f"aggregating directly into {max_index+1} categories")
                    category = category_modulo(caxis, max_index+1)
                    color_label_list = {idx: [value] for idx, value in active_subset.items()}
                else:
                    log.debug(f"aggregating modulo {num_colors} categories")
                    category = category_modulo(caxis, num_colors)
                    # each slice maps to, potentially, multiple labels from the subset
                    color_label_list = {i: [active_subset[idx] for idx in range(i, max_index+1, num_colors) if idx in active_subset]
                                        for i in range(num_colors)}
                    # and colors just come from the bottom of the colormap
                    color_dict = dict(enumerate(options.dmap[:num_colors]))
                # convert lists of color labels into strings
                color_labels = ['']*num_colors
                for i, labels in color_label_list.items():
                    if len(labels) < 3:
                        color_labels[i] = ",".join(labels)
                    else:
                        color_labels[i] = ",".join(labels[:2] + ["..."])
            # else we discretize a span of values
            else:
                num_colors = min(cdatum.nlevels, len(bmap))
                log.debug(f'colourizing using {caxis} with {num_colors} bins')
                cmin = bounds[caxis][0]
                cdelta = (bounds[caxis][1] - cmin) / num_colors
                category = category_binning(caxis, cmin, cdelta, num_colors)
                # color labels are bin centres
                bin_centers = [cmin + cdelta*(i+0.5) for i in range(num_colors)]
                # map to colors pulled from entire extent of color map
                color_key = [bmap[(i*len(bmap))//num_colors] for i in range(num_colors)]
                color_labels = [str(bin) for bin in bin_centers]
                log.info(f": aggregating using {num_colors} bins at {' '.join(color_labels)})")

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

        if cdatum.is_discrete and not data_mappers.USE_COUNT_CAT:
            # discard empty planes
            non_empty = np.where(non_empty)[0]
            raster = raster[..., non_empty]
            # compress colours to bottom of colormap, unless asked to preserve assignments
            if options.dmap_preserve:
                color_key = [color_key[bin] for bin in non_empty]
            else:
                color_key = color_key[:len(non_empty)]
            color_labels =  [color_labels[bin] for bin in non_empty]

        img = datashader.transfer_functions.shade(raster, color_key=color_key, how=normalize, min_alpha=min_alpha)
        # set color_minmax for colorbar
        color_minmax = bounds[caxis]
    else:
        log.debug(f'rasterizing using {ared}')
        raster = canvas.points(ddf, xaxis, yaxis, agg=agg_alpha)
        if not raster.data.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None
        # get min/max cor colorbar
        if aaxis:
            amin, amax = adatum.minmax
            color_minmax = (amin if amin is not None else np.nanmin(raster)), \
                           (amax if amax is not None else np.nanmax(raster))
            color_key = cmap
        log.debug('shading')
        img = datashader.transfer_functions.shade(raster, cmap=cmap, how=normalize, span=color_minmax, min_alpha=min_alpha)

    # resaturate if needed
    if saturate_alpha is not None or saturate_percentile is not None:
        # get alpha channel
        imgval = img.values
        alpha = (imgval >> 24)&255
        nulls = alpha<min_alpha
        alpha -= min_alpha
        if nulls.all():
            log.debug(f"alpha<min_alpha for entire plot -- all data below lower clip perhaps?")
        else:    
            #if percentile if specified, use that to override saturate_alpha
            if saturate_alpha is None:
                saturate_alpha = np.percentile(alpha[~nulls], saturate_percentile)
                log.debug(f"using saturation alpha {saturate_alpha} from {saturate_percentile}th percentile")
            else:
                log.debug(f"using explicit saturation alpha {saturate_alpha}")
            # rescale alpha from [min_alpha, saturation_alpha] to [min_alpha, 255]
            saturation_factor = (255. - min_alpha) / (saturate_alpha - min_alpha)
            alpha = min_alpha + alpha*saturation_factor
            alpha[nulls] = 0
            alpha[alpha>255] = 255
            imgval[:] = (imgval & 0xFFFFFF) | alpha.astype(np.uint32)<<24

    if options.spread_pix:
        img = datashader.transfer_functions.dynspread(img, options.spread_thr, max_px=options.spread_pix)
        log.info(f": spreading ({options.spread_thr} {options.spread_pix})")
    rgb = holoviews.RGB(holoviews.operation.datashader.shade.uint32_to_uint8_xr(img))

    log.debug('done')

    # Set plot limits based on data extent or user values for axis labels

    xmin, xmax = bounds[xaxis]
    ymin, ymax = bounds[yaxis]

    log.debug('rendering image')

    fig = pylab.figure(figsize=(figx, figy))
    ax = fig.add_subplot(111, facecolor=bgcol)
    ax.imshow(X=rgb.data, extent=[xmin, xmax, ymin, ymax],
              aspect='auto', origin='lower', interpolation='nearest')

    ax.set_title("\n".join(textwrap.wrap(title, 90)), loc='center', fontdict=dict(fontsize=options.fontsize))
    ax.set_xlabel(xlabel, fontdict=dict(fontsize=options.fontsize))
    ax.set_ylabel(ylabel, fontdict=dict(fontsize=options.fontsize))
    # ax.plot(xmin,ymin,'.',alpha=0.0)
    # ax.plot(xmax,ymax,'.',alpha=0.0)

    dx, dy = xmax - xmin, ymax - ymin
    ax.set_xlim([xmin - dx/100, xmax + dx/100])
    ax.set_ylim([ymin - dy/100, ymax + dy/100])

    def decimate_list(x, maxel):
        """Helper function to reduce a list to < given max number of elements, dividing it by decimal factors of 2 and 5"""
        factors = 2, 5, 10
        base = divisor = 1
        while len(x)//divisor > maxel:
            for fac in factors:
                divisor = fac*base
                if len(x)//divisor <= maxel:
                    break
            base *= 10
        return x[::divisor]

    ax.tick_params(labelsize=options.fontsize*0.66)

    # max # of tickmarks and labels to draw for discrete axes
    MAXLABELS = 64   # if we have up to this many labels, show them all
    MAXLABELS1 = 32  # if we have >MAXLABELS to show, then sparsify and get below this number
    MAXTICKS = 300   # if total number of points is within this range, draw them as minor tickmarks

    # do we have discrete labels to put on the axes?
    if xdatum.discretized_labels is not None:
        n = len(xdatum.discretized_labels)
        ticks_labels = list(enumerate(xdatum.discretized_labels))
        if n > MAXLABELS:
            ticks_labels = decimate_list(ticks_labels, MAXLABELS1)         # enforce max number of tick labels
        labels = [label for _, label in ticks_labels]
        rot = 90 if max([len(label) for label in xdatum.discretized_labels])*n > 60 else 0
        ax.set_xticks([x[0] for x in ticks_labels])
        ax.set_xticklabels(labels, rotation=rot)
        if len(ticks_labels) < n and n <= MAXTICKS:
            ax.set_xticks(range(n), minor=True)

    if ydatum.discretized_labels is not None:
        n = len(ydatum.discretized_labels)
        ticks_labels = list(enumerate(ydatum.discretized_labels))
        if n > MAXLABELS:
            ticks_labels = decimate_list(ticks_labels, MAXLABELS1)         # enforce max number of tick labels
        labels = [label for _, label in ticks_labels]
        ax.set_yticks([y[0] for y in ticks_labels])
        ax.set_yticklabels(labels)
        if len(ticks_labels) < n and n <= MAXTICKS:
            ax.set_yticks(range(n), minor=True)

    # colorbar?
    if color_minmax:
        import matplotlib.colors
        # discrete axis
        if caxis is not None and cdatum.is_discrete:
            norm = matplotlib.colors.Normalize(-0.5, len(color_key)-0.5)
            ticks = np.arange(len(color_key))
            colormap = matplotlib.colors.ListedColormap(color_key)
        # discretized axis
        else:
            norm = matplotlib.colors.Normalize(*color_minmax)
            colormap = matplotlib.colors.ListedColormap(color_key)
            # auto-mark colorbar, since it represents a continuous range of values
            ticks = None

        cb = fig.colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=colormap), ax=ax, ticks=ticks)

        # adjust ticks for discrete axis
        if caxis is not None and cdatum.is_discrete:
            rot = 0
            # adjust fontsize for number of labels
            fs = max(options.fontsize*min(0.8, 20./len(color_labels)), 6)
            fontdict = dict(fontsize=fs)
            if max([len(lbl) for lbl in color_labels]) > 3 and len(color_labels) < 8:
                rot = 90
                fontdict['verticalalignment'] ='center'
            cb.ax.set_yticklabels(color_labels, rotation=rot, fontdict=fontdict)
        else:
            cb.ax.tick_params(labelsize=options.fontsize*0.8)

    fig.savefig(pngname, bbox_inches='tight')

    pylab.close()

    return pngname

