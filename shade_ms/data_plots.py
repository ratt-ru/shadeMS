# -*- coding: future_fstrings -*-

# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import daskms
import dask.array as da
import dask.array.ma as dama
import dask.dataframe as dask_df
import datashape.coretypes
import xarray
import holoviews as holoviews
import holoviews.operation.datashader
import datashader.transfer_functions
import numpy
import pylab
import textwrap
import argparse
import matplotlib.cm
from shade_ms import log

from collections import OrderedDict
from . import data_mappers
from .data_mappers import DataAxis


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
                  iter_field, iter_spw, iter_scan,
                  join_corrs=False,
                  row_chunk_size=100000):

    ms_cols = {'FLAG', 'FLAG_ROW', 'ANTENNA1', 'ANTENNA2'}

    # get visibility columns
    for axis in DataAxis.all_axes.values():
        ms_cols.update(axis.columns)

    # get MS data
    msdata = daskms.xds_from_ms(msinfo.msname, columns=list(ms_cols), group_cols=group_cols, taql_where=mytaql,
                                chunks=dict(row=row_chunk_size))

    log.info(f': Indexing MS and building dataframes (chunk size is {row_chunk_size})')

    np = 0  # number of points to plot

    # output dataframes, indexed by (field, spw, scan, antenna, correlation)
    # If any of these axes is not being iterated over, then the index is None
    output_dataframes = OrderedDict()

    # # make prototype dataframe
    # import pandas
    #
    #

    # iterate over groups
    for group in msdata:
        ddid     =  group.DATA_DESC_ID  # always present
        fld      =  group.FIELD_ID # always present
        if fld not in subset.field or ddid not in subset.spw:
            log.debug(f"field {fld} ddid {ddid} not in selection, skipping")
            continue
        scan    = getattr(group, 'SCAN_NUMBER', None)  # will be present if iterating over scans

        # TODO: antenna iteration. None forces no iteration, for now
        antenna = None

        # always read flags -- easier that way
        flag = group.FLAG
        flag_row = group.FLAG_ROW
        if noflags:
            flag = da.zeros_like(flag)
            flag_row = da.zeros_like(flag_row)

        baselines = numpy.array([msinfo.baseline_numbering[p,q] for p,q in zip(group.ANTENNA1.values,
                                                                               group.ANTENNA2.values)])
        freqs = chan_freqs[ddid]
        chans = xarray.DataArray(range(len(freqs)), dims=("chan",))
        wavel = freq_to_wavel(freqs)
        extras = dict(chans=chans, freqs=freqs, wavel=wavel, rows=group.row, baselines=baselines)

        flag = flag[dict(chan=chanslice)]
        shape = flag.shape[:-1]

        datums = OrderedDict()

        for corr in subset.corr.numbers:
            # make dictionary of extra values for DataMappers
            extras['corr'] = corr
            # loop over datums to be computed
            for axis in DataAxis.all_axes.values():
                value = datums[axis.label][-1] if axis.label in datums else None
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
                    # reshape values of shape NTIME to (NTIME,1) and NFREQ to (1,NFREQ), and scalar to (NTIME,1)
                    if value.ndim == 1:
                        timefreq_axis = axis.mapper.axis or 0
                        assert value.shape[0] == shape[timefreq_axis], \
                               f"{axis.mapper.fullname}: size {value.shape[0]}, expected {shape[timefreq_axis]}"
                        shape1 = [1,1]
                        shape1[timefreq_axis] = value.shape[0]
                        value = value.reshape(shape1)
                        if timefreq_axis > 0:
                            value = da.broadcast_to(value, shape)
                        log.debug(f"axis {axis.mapper.fullname} has shape {value.shape}")
                    # else 2D value better match expected shape
                    else:
                        assert value.shape == shape, f"{axis.mapper.fullname}: shape {value.shape}, expected {shape}"
                datums.setdefault(axis.label, []).append(value)

        # if joining correlations, stick all elements together. Otherwise, we'd better have one per label
        if join_corrs:
            datums = OrderedDict({label: da.concatenate(arrs) for label, arrs in datums.items()})
        else:
            assert all([len(arrs) == 1 for arrs in datums.values()])
            datums = OrderedDict({label: arrs[0] for label, arrs in datums.items()})

        # broadcast to same shape, and unravel all datums
        datums = OrderedDict({ key: arr.ravel() for key, arr in zip(datums.keys(),
                                                                    da.broadcast_arrays(*datums.values()))})

        # if any axis needs to be conjugated, double up all of them
        if not noconj and any([axis.conjugate for axis in DataAxis.all_axes.values()]):
            for axis in DataAxis.all_axes.values():
                if axis.conjugate:
                    datums[axis.label] = da.concatenate([datums[axis.label], -datums[axis.label]])
                else:
                    datums[axis.label] = da.concatenate([datums[axis.label], datums[axis.label]])

        labels, values = list(datums.keys()), list(datums.values())
        np += values[0].size

        # now stack them all into a big dataframe
        rectype = [(axis.label, numpy.int32 if axis.nlevels else numpy.float32) for axis in DataAxis.all_axes.values()]
        recarr = da.empty_like(values[0], dtype=rectype)
        ddf = dask_df.from_array(recarr)
        for label, value in zip(labels, values):
            ddf[label] = value

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
            output_dataframes[dataframe_key] = ddf0.append(ddf)

    # convert discrete axes into categoricals
    if data_mappers.USE_COUNT_CAT:
        categorical_axes = [axis.label for axis in DataAxis.all_axes.values() if axis.nlevels]
        if categorical_axes:
            log.info(": counting colours")
            for key, ddf in list(output_dataframes.items()):
                output_dataframes[key] = ddf.categorize(categorical_axes)

    log.info(": complete")
    return output_dataframes, np

from datashader.utils import ngjit

try:
    import cudf
except ImportError:
    cudf = None

class count_integers(datashader.count_cat):
    """Aggregator. Counts of all elements in ``column``, grouped by value of another column. Like datashader.count_cat,
    but for normal columns."""
    _dshape = datashape.dshape(datashape.coretypes.int32)

    def __init__(self, column, modulo):
        datashader.count_cat.__init__(self, column)
        self.modulo = modulo
        self.codes = xarray.DataArray(list(range(self.modulo)))

    def validate(self, in_dshape):
        pass

    @property
    def inputs(self):
        return (datashader.reductions.extract(self.column), )

    @staticmethod
    @ngjit
    def _append(x, y, agg, field):
        agg[y, x, int(field)] += 1

    def out_dshape(self, input_dshape):
        return datashape.util.dshape(datashape.Record([(c, datashape.coretypes.int32) for c in range(self.modulo)]))

    def _build_finalize(self, dshape):
        def finalize(bases, cuda=False, **kwargs):
            dims = kwargs['dims'] + [self.column]
            coords = kwargs['coords']
            coords[self.column] = list(self.codes.values)
            return xarray.DataArray(bases[0], dims=dims, coords=coords)
        return finalize

class extract_multi(datashader.reductions.category_values):
    """Extract multiple columns from a dataframe as a numpy array of values. Like datashader.category_values,
    but with two columns."""
    def apply(self, df):
        if cudf and isinstance(df, cudf.DataFrame):
            import cupy
            cols = []
            for column in self.columns:
                nullval = numpy.nan if df[self.columns[1]].dtype.kind == 'f' else 0
                cols.append(df[self.columns[0]].to_gpu_array(fillna=nullval))
            return cupy.stack(cols, axis=-1)
        else:
            return numpy.stack([df[col].values for col in self.columns], axis=-1)

class by_integers(datashader.by):
    """Like datashader.by,
    but for normal columns."""
    def __init__(self, cat_column, reduction, modulo):
        super().__init__(cat_column, reduction)
        self.modulo = modulo

    def validate(self, in_dshape):
        if not self.cat_column in in_dshape.dict:
            raise ValueError("specified column not found")

        self.reduction.validate(in_dshape)

    def out_dshape(self, input_dshape):
        cats = list(range(self.modulo))
        red_shape = self.reduction.out_dshape(input_dshape)
        return datashape.util.dshape(datashape.Record([(c, red_shape) for c in cats]))

    @property
    def inputs(self):
        if self.val_column is not None:
            return (extract_multi(self.columns),)
        else:
            return (datashader.reductions.extract(self.columns[0]),)

    def _build_finalize(self, dshape):
        def finalize(bases, cuda=False, **kwargs):
            dims = kwargs['dims'] + [self.cat_column]

            coords = kwargs['coords']
            coords[self.cat_column] = list(range(self.modulo))
            return xarray.DataArray(bases[0], dims=dims, coords=coords)

        return finalize


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
    color_key = ncolors = color_mapping = color_labels = agg_alpha = raster_alpha = None

    xmin, xmax = xdatum.minmax
    ymin, ymax = ydatum.minmax

    canvas = datashader.Canvas(options.xcanvas, options.ycanvas,
                               x_range=[xmin, xmax] if xmin is not None else None,
                               y_range=[ymin, ymax] if ymin is not None else None)

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

        if data_mappers.USE_COUNT_CAT:
            color_bins = [int(x) for x in getattr(ddf.dtypes, caxis).categories]
            log.debug(f'colourizing with count_cat, {len(color_bins)} bins')
            if USE_REDUCE_BY and agg_alpha:
                agg = datashader.by(caxis, agg_alpha)
            else:
                agg = datashader.count_cat(caxis)
        else:
            color_bins = list(range(cdatum.nlevels))
            log.debug(f'colourizing with count_integer, {len(color_bins)} bins')
            if USE_REDUCE_BY and agg_alpha:
                agg = by_integers(caxis, agg_alpha, cdatum.nlevels)
            else:
                agg = count_integers(caxis, cdatum.nlevels)


        raster = canvas.points(ddf, xaxis, yaxis, agg=agg)
        non_empty = numpy.array(raster.any(axis=(0, 1)))
        if not non_empty.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None
        # true if axis is continuous discretized
        if cdatum.discretized_delta is not None:
            # color labels are bin centres
            bin_centers = [cdatum.discretized_bin_centers[i] for i in color_bins]
            # map to colors pulled from 256 color map
            color_key = [bmap[(i*256)//cdatum.nlevels] for i in color_bins]
            color_labels = list(map(str, bin_centers))
            log.info(f": shading using {len(color_bins)} colors (bin centres are {' '.join(color_labels)})")
        # else a discrete axis
        else:
            # discard empty bins
            non_empty = numpy.where(non_empty)[0]
            raster = raster[..., non_empty]
            # just use bin numbers to look up a color directly
            color_bins = [color_bins[i] for i in non_empty]
            color_key = [dmap[bin] for bin in color_bins]
            # the numbers may be out of order -- reorder for color bar purposes
            bin_color = sorted(zip(color_bins, color_key))
            if cdatum.discretized_labels and len(cdatum.discretized_labels) <= cdatum.nlevels:
                color_labels = [cdatum.discretized_labels[bin] for bin, _ in bin_color]
            else:
                color_labels = [str(bin) for bin, _ in bin_color]
            color_mapping = [col for _, col in bin_color]
            log.info(f": rendering using {len(color_bins)} colors (values {' '.join(color_labels)})")
        if raster_alpha is not None:
            amin, amax = numpy.nanmin(raster_alpha), numpy.nanmax(raster_alpha)
            raster = raster*(raster_alpha-amin)/(amax-amin)
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

    data_xmin = numpy.min(raster.coords[xaxis].values)
    data_xmax = numpy.max(raster.coords[xaxis].values)
    data_ymin = numpy.min(raster.coords[yaxis].values)
    data_ymax = numpy.max(raster.coords[yaxis].values)

    xmin = data_xmin if xmin is None else xdatum.minmax[0]
    xmax = data_xmax if xmax is None else xdatum.minmax[1]
    ymin = data_ymin if ymin is None else ydatum.minmax[0]
    ymax = data_ymax if ymax is None else ydatum.minmax[1]

    log.debug('rendering image')

    def match(artist):
        return artist.__module__ == 'matplotlib.text'

    fig = pylab.figure(figsize=(figx, figy))
    ax = fig.add_subplot(111, facecolor=bgcol)
    ax.imshow(X=rgb.data, extent=[data_xmin, data_xmax, data_ymin, data_ymax],
              aspect='auto', origin='lower')
    ax.set_title("\n".join(textwrap.wrap(title, 90)), loc='left')
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
        if color_mapping is not None:
            norm = matplotlib.colors.Normalize(-0.5, len(color_bins)-0.5)
            ticks = numpy.arange(len(color_bins))
            colormap = matplotlib.colors.ListedColormap(color_mapping)
        # discretized axis
        else:
            norm = matplotlib.colors.Normalize(cdatum.minmax[0], cdatum.minmax[1])
            colormap = matplotlib.colors.ListedColormap(color_key)
            # auto-mark colorbar, since it represents a continuous range of values
            ticks = None

        cb = fig.colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=colormap), ax=ax, ticks=ticks)

        # adjust ticks for discrete axis
        if color_mapping is not None:
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

