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
import math
import re
import argparse
import pylab
import matplotlib.cm
import ShadeMS

from collections import OrderedDict
from .ms_info import MSInfo

log = ShadeMS.log
ms = MSInfo()

def blank():
    log.info('------------------------------------------------------')

def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff

def col_to_label(col):
    """Replaces '-' and "/" in column names with palatable characters acceptable in filenames and identifiers"""
    return col.replace("-", "min").replace("/", "div")

USE_COUNT_CAT = 0
COUNT_DTYPE = numpy.int16

def add_options(parser):
    parser.add_argument('--count-cat', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--count-dtype', default='int16', help=argparse.SUPPRESS)

def set_options(options):
    global USE_COUNT_CAT, COUNT_DTYPE
    USE_COUNT_CAT = options.count_cat
    COUNT_DTYPE = getattr(numpy, options.count_dtype)

class DataMapper(object):
    """This class defines a mapping from a dask group to an array of real values to be plotted"""
    def __init__(self, fullname, unit, mapper, column=None, extras=[], conjugate=False, axis=None):
        """
        :param fullname:    full name of parameter (real, amplitude, etc.)
        :param unit:        unit string
        :param mapper:      function that maps column data to parameter
        :param column:      fix a column name (e.g. TIME, UVW. Not for visibility columns.)
        :param extras:      extra arguments needed by mapper (e.g. ["freqs", "wavel"])
        :param conjugate:   sets conjugation flag
        :param axis:        which axis the parameter represets (0 time, 1 freq), if 1-dimensional
        """
        assert mapper is not None
        self.fullname, self.unit, self.mapper, self.column, self.extras = fullname, unit, mapper, column, extras
        self.conjugate = conjugate
        self.axis = axis

_identity = lambda x:x

# this dict maps short axis names into full DataMapper objects
data_mappers = OrderedDict(
    _     = DataMapper("", "", _identity),
    amp   = DataMapper("Amplitude", "", abs),
    phase = DataMapper("Phase", "deg", lambda x:da.arctan2(da.imag(x), da.real(x))*180/math.pi),
    real  = DataMapper("Real", "", da.real),
    imag  = DataMapper("Imag", "", da.imag),
    TIME  = DataMapper("Time", "s", axis=0, column="TIME", mapper=_identity),
    ROW   = DataMapper("Row number", "", column=False, axis=0, extras=["rows"], mapper=lambda x,rows: rows),
    BASELINE = DataMapper("Baseline", "", column=False, axis=0, extras=["baselines"], mapper=lambda x,baselines: baselines),
    CORR  = DataMapper("Correlation", "", column=False, axis=0, extras=["corr"], mapper=lambda x,corr: corr),
    CHAN  = DataMapper("Channel", "", column=False, axis=1, extras=["chans"], mapper=lambda x,chans: chans),
    FREQ  = DataMapper("Frequency", "Hz", column=False, axis=1, extras=["freqs"], mapper=lambda x, freqs: freqs),
    WAVEL = DataMapper("Wavelength", "m", column=False, axis=1, extras=["wavel"], mapper=lambda x, wavel: wavel),
    UV    = DataMapper("uv-distance", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: da.sqrt((uvw[:,:2]**2).sum(axis=1))/wavel),
    U     = DataMapper("u", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: uvw[:, 0] / wavel,
                 conjugate=True),
    V     = DataMapper("v", "wavelengths", column="UVW", extras=["wavel"],
                 mapper=lambda uvw, wavel: uvw[:, 1] / wavel,
                 conjugate=True),
    W     = DataMapper("w", "wavelengths", column="UVW", extras=["wavel"],
                 mapper=lambda uvw, wavel: uvw[:, 2] / wavel,
                 conjugate=True),
)


class DataAxis(object):
    """
    Represents a data axis that can be plotted: a combination of column, function and correlation
    """
    # dict of all registered axes
    all_axes = OrderedDict()
    # set of labels (used to make unique labels)
    all_labels = set()

    @classmethod
    def is_legit_colspec(cls, colspec):
        match = re.fullmatch(r"(\w+)([*/+-])(\w+)", colspec)
        if match:
            return match.group(1) in ms.valid_columns and match.group(3) in ms.valid_columns
        else:
            return colspec in ms.valid_columns

    @classmethod
    def parse_datum_spec(cls, axis_spec, default_column=None):
        # figure out the axis specification
        function = column = corr = None
        specs = axis_spec.split(":", 2)
        for spec in specs:
            if spec is None:
                raise ValueError(f"invalid axis specification '{axis_spec}'")
            if spec in data_mappers:
                if function is not None:
                    raise ValueError(f"function specified twice in '{axis_spec}'")
                function = spec
            elif DataAxis.is_legit_colspec(spec):
                if column is not None:
                    raise ValueError(f"column specified twice in '{axis_spec}'")
                column = spec
            else:
                if re.fullmatch(r"\d+", spec):
                    corr1 = int(spec)
                else:
                    corr1 = ms.corr[spec]
                    if corr1 is None:
                        raise ValueError(f"invalid axis specification '{axis_spec}'")
                if corr is not None:
                    raise ValueError(f"correlation specified twice in '{axis_spec}'")
                corr = corr1
        # defaults?
        if function is None:
            if column or corr:
                function = "_"
            else:
                raise ValueError(f"invalid axis specification '{axis_spec}'")
        column = column or data_mappers[function].column or default_column
        has_corr_axis = column.endswith("DATA") or column.endswith("SPECTRUM") or function == "corr"
        if not has_corr_axis:
            if corr is not None:
                raise ValueError(f"'{axis_spec}': can't specify correlation when column is '{column}'")
            corr = False  # corr=False marks a datum without a correlation axis
        return function, column, corr, (has_corr_axis and corr is None)

    @classmethod
    def register(cls, function, column, corr, minmax=None, ncol=None):
        """
        Registers a data axis, which ultimately ends up as a column in the assembled dataframe.
        For multiple plots, we want to reuse the same information (assuming the same
        clipping limits, etc.) so we have a dictionary of axis definitions here.

        Columns selects a column to operate on.
        Function selects a mapping (see datamappers below).
        Corr selects a correlation (or a Stokes product such as I, Q,...)
        minmax sets axis clipping levels
        ncol discretizes the axis into N colours between min and max
        """
        # form up label
        label  = "{}_{}_{}".format(col_to_label(column or ''), function, corr)
        minmax = tuple(minmax) or (None, None)
        key = label, minmax, ncol
        # see if this axis definition already exists, else create new one
        if key in cls.all_axes:
            return cls.all_axes[key]
        else:
            label0, i = label, 0
            while label in cls.all_labels:
                i += 1
                label = f"{label}_{i}"
            cls.all_labels.add(label)
            axis = cls.all_axes[key] = DataAxis(column, function, corr, minmax, ncol, label)
            return axis

    def __init__(self, column, function, corr, minmax=None, ncol=None, label=None):
        """See register() class method above. Not called directly."""
        self.name = ":".join([str(x) for x in (function, column, corr, minmax, ncol) if x is not None ])
        self.function = function        # function to apply to column (see list of DataMappers below)
        self.corr     = corr if corr != "all" else None
        self.nlevels  = ncol
        self.minmax   = vmin, vmax = tuple(minmax) or (None, None)
        self.label    = label
        self._corr_reduce = None
        self.discretized_labels = None  # filled for corrs and fields and so

        # set up discretized continuous axis
        if self.nlevels and vmin is not None and vmax is not None:
            self.discretized_delta = delta = (vmax - vmin) / self.nlevels
            self.discretized_bin_centers = numpy.arange(vmin + delta/2, vmax, delta)
        else:
            self.discretized_delta = self.discretized_bin_centers = None

        self.mapper = data_mappers[function]

        # columns with labels?
        if function == 'corr':
            # special case of "corr" if corr is fixed: return constant value fixed here
            if corr is not None:
                self.mapper = DataMapper("Correlation", "", column=False, axis=-1, mapper=lambda x: corr)
            self.discretized_labels = ms.corr.names
        elif column == "FIELD_ID":
            self.discretized_labels = ms.field.names
        elif column == "ANTENNA1" or column == "ANTENNA2":
            self.discretized_labels = ms.all_antenna.names

        # if labels were set up, adjust nlevels (but only down, never up)
        if self.discretized_labels is not None:
            self.nlevels = min(len(self.discretized_labels), self.nlevels)

        if self.function == "_":
            self.function = ""
        self.conjugate = self.mapper.conjugate
        self.timefreq_axis = self.mapper.axis

        # setup columns
        self._ufunc = None
        self.columns = ()

        # does the mapper have no column (i.e. frequency)?
        if self.mapper.column is False:
            log.info(f'axis: {function}, range {self.minmax}, discretization {self.nlevels}')
        # does the mapper have a fixed column? This better be consistent
        elif self.mapper.column is not None:
            # if a mapper (such as "uv") implies a fixed column name, make sure it's consistent with what the user said
            if column and self.mapper.column != column:
                raise ValueError(f"'{function}' not applicable with column {column}")
            self.columns = (column,)
            log.info(f'axis: {function}({column}), range {self.minmax}, discretization {self.nlevels}')
        # else arbitrary column
        else:
            log.info(f'axis: {function}({column}), corr {self.corr}, range {self.minmax}, discretization {self.nlevels}')
            # check for column arithmetic
            match = re.fullmatch(r"(\w+)([*/+-])(\w+)", column)
            if match:
                self.columns = (match.group(1), match.group(3))
                # look up dask ufunc corresponding to arithmetic op
                self._ufunc = {'+': da.add, '*': da.multiply, '-': da.subtract, '/': da.divide}[match.group(2)]
            else:
                self.columns = (column,)

    def get_column_data(self, group):
        """Given a dask group, returns dask array corresponding to column setting"""
        if not self.columns:
            return None
        try:
            data = getattr(group, self.columns[0])
            if self._ufunc:
                return self._ufunc(data, getattr(group, self.columns[1]))
            return data
        except AttributeError:
            raise NameError("column {} not found in group".format(" or ".join(self.columns)))

    def get_value(self, group, corr, extras, flag, flag_row, chanslice):
        coldata = self.get_column_data(group)
        # correlation may be pre-set by plot type, or may be passed to us
        corr = self.corr if self.corr is not None else corr
        # apply correlation reduction
        if coldata is not None and coldata.ndim == 3:
            assert corr is not None
            # the mapper can't have a specific axis set
            if self.mapper.axis is not None:
                raise TypeError(f"{self.name}: unexpected column with ndim=3")
            coldata = ms.corr_data_mappers[corr](coldata)
        # apply mapping function
        coldata = self.mapper.mapper(coldata, **{name:extras[name] for name in self.mapper.extras })
        # scalar expanded to row vector
        if numpy.isscalar(coldata):
            coldata = da.full_like(flag_row, fill_value=coldata, dtype=type(coldata))
            flag = flag_row
        else:
            # apply channel slicing, if there's a channel axis in the array (and the array is a DataArray)
            if type(coldata) is xarray.DataArray and 'chan' in coldata.dims:
                coldata = coldata[dict(chan=chanslice)]
            # determine flags -- start with original flags
            if coldata.ndim == 2:
                flag = ms.corr_flag_mappers[corr](flag)
            elif coldata.ndim == 1:
                if not self.mapper.axis:
                    flag = flag_row
                elif self.mapper.axis == 1:
                    flag = da.zeros_like(coldata, bool)
            # shapes must now match
            if coldata.shape != flag.shape:
                raise TypeError(f"{self.name}: unexpected column shape")
        x0, x1 = self.minmax
        # discretize
        if self.nlevels:
            # minmax set? discretize over that
            if self.discretized_delta is not None:
                coldata = da.floor((coldata - self.minmax[0])/self.discretized_delta)
                coldata = da.minimum(da.maximum(coldata, 0), self.nlevels-1).astype(COUNT_DTYPE)
            else:
                if not numpy.issubdtype(coldata.dtype, numpy.integer):
                    raise TypeError(f"{self.name}: min/max must be set to colour by non-integer values")
                coldata = da.remainder(coldata, self.nlevels).astype(COUNT_DTYPE)
        # # else just apply clipping # NB no longer needed as we set the canvas correctly
        # else:
        #     if x0 is not None:
        #         flag = da.logical_or(flag, coldata<self.minmax[0])
        #     if x1 is not None:
        #         flag = da.logical_or(flag, coldata>self.minmax[1])
        # return masked array
        return dama.masked_array(coldata, flag)


def get_plot_data(msinfo, group_cols, mytaql, chan_freqs,
                  chanslice,
                  spws, fields, corrs, noflags, noconj,
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
        if fld not in fields or ddid not in spws:
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

        for corr in corrs.numbers:
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

        # from pandas.api.types import CategoricalDtype
        # for axis in DataAxis.all_axes.values():
        #     if axis.nlevels:
        #         cat_type = CategoricalDtype(categories=range(axis.nlevels), ordered=True)
        #         kw = {}
        #         kw[axis.label+"_"] = cat_type
        #         ddf.assign(**kw)
        #
        # ddf = dask_df.from_array(da.stack(values, axis=1), columns=labels)

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
    if USE_COUNT_CAT:
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
    """Count of all elements in ``column``, grouped by value.
    """
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

class count_cat(datashader.count_cat):
    """Redefine here just so we can print during debugging..."""
    @staticmethod
    @ngjit
    def _append(x, y, agg, field):
#        print(x,y,field)
        agg[y, x, int(field)] += 1


USE_COUNT_CAT = 0

def create_plot(ddf, xdatum, ydatum, cdatum, xcanvas,ycanvas, cmap, bmap, dmap, normalize,
                xlabel, ylabel, title, pngname, bgcol, fontsize, figx=24, figy=12):

    xaxis = xdatum.label
    yaxis = ydatum.label
    caxis = cdatum and cdatum.label
    color_key = ncolors = color_mapping = color_labels = None

    xmin, xmax = xdatum.minmax
    ymin, ymax = ydatum.minmax

    canvas = datashader.Canvas(xcanvas, ycanvas,
                               x_range=[xmin, xmax] if xmin is not None else None,
                               y_range=[ymin, ymax] if ymin is not None else None)

    if cdatum is not None:
        if USE_COUNT_CAT:
            color_bins = [int(x) for x in getattr(ddf.dtypes, caxis).categories]
            log.debug(f'making raster with count_cat, {len(color_bins)} bins')
            raster = canvas.points(ddf, xaxis, yaxis, agg=count_cat(caxis))
        else:
            color_bins = list(range(cdatum.nlevels))
            log.debug(f'making raster with count_integer, {len(color_bins)} bins')
            raster = canvas.points(ddf, xaxis, yaxis, agg=count_integers(caxis, cdatum.nlevels))
        if not raster.data.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None
        ncolors = len(color_bins)
        # true if axis is continuous discretized
        if cdatum.discretized_delta is not None:
            # color labels are bin centres
            bin_centers = [cdatum.discretized_bin_centers[i] for i in color_bins]
            # map to colors pulled from 256 color map
            color_key = [bmap[(i*256)//cdatum.nlevels] for i in color_bins]
            color_labels = list(map(str, bin_centers))
            log.info(f": shading using {ncolors} colors (bin centres are {' '.join(color_labels)})")
        # else a discrete axis
        else:
            # just use bin numbers to look up a color directly
            color_key = [dmap[i] for i in color_bins]
            # the numbers may be out of order -- reorder for color bar purposes
            bin_color = sorted(zip(color_bins, color_key))
            if cdatum.discretized_labels and len(cdatum.discretized_labels) <= cdatum.nlevels:
                color_labels = [cdatum.discretized_labels[bin] for bin, _ in bin_color]
            else:
                color_labels = [str(bin) for bin, _ in bin_color]
            color_mapping = [col for _, col in bin_color]
            log.info(f": shading using {ncolors} colors (values {' '.join(color_labels)})")
        img = datashader.transfer_functions.shade(raster, color_key=color_key, how=normalize)
        rgb = holoviews.RGB(holoviews.operation.datashader.shade.uint32_to_uint8_xr(img))
    else:
        log.debug('making raster')
        raster = canvas.points(ddf, xaxis, yaxis)
        if not raster.data.any():
            log.info(": no valid data in plot. Check your flags and/or plot limits.")
            return None
        log.debug('shading')
        img = datashader.transfer_functions.shade(raster, cmap=cmap, how=normalize)
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
    ax.set_title(title,loc='left')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    # ax.plot(xmin,ymin,'.',alpha=0.0)
    # ax.plot(xmax,ymax,'.',alpha=0.0)

    dx, dy = xmax - xmin, ymax - ymin
    ax.set_xlim([xmin - dx/100, xmax + dx/100])
    ax.set_ylim([ymin - dy/100, ymax + dy/100])

    # set fontsize on everything rendered so far
    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(fontsize)

    # colorbar?
    if color_key:
        import matplotlib.colors
        # discrete axis
        if color_mapping is not None:
            norm = matplotlib.colors.Normalize(-0.5, ncolors-0.5)
            ticks = numpy.arange(ncolors)
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
            fs = max(fontsize*min(1, 32./len(color_labels)), 6)
            fontdict = dict(fontsize=fs)
            if max([len(lbl) for lbl in color_labels]) > 3 and len(color_labels) < 8:
                rot = 90
                fontdict['verticalalignment'] ='center'
            cb.ax.set_yticklabels(color_labels, rotation=rot, fontdict=fontdict)

    fig.savefig(pngname, bbox_inches='tight')

    pylab.close()

    return pngname

