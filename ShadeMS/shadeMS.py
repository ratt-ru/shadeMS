# -*- coding: future_fstrings -*-

# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import daskms
import dask.array as da
import dask.array.ma as dama
import dask.dataframe as dask_df
import datashader
import holoviews as holoviews
import holoviews.operation.datashader
import datashader.transfer_functions
import numpy
import math
import re
import pylab
import matplotlib.cm
import ShadeMS
from collections import OrderedDict
from casacore.tables import table

from MSUtils.msutils import STOKES_TYPES

log = ShadeMS.log

def log_info(message, prefix=''):
    log.info(f'{prefix:16} : {message}')

def blank():
    log.info('------------------------------------------------------')


def get_chan_freqs(myms):
    spw_tab = daskms.xds_from_table(
        myms+'::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
    chan_freqs = spw_tab[0].CHAN_FREQ
    return chan_freqs


def get_field_names(myms):
    field_tab = daskms.xds_from_table(
        myms+'::FIELD', columns=['NAME','SOURCE_ID'])
    field_ids = field_tab[0].SOURCE_ID.values
    field_names = field_tab[0].NAME.values
    return field_ids, field_names


def get_scan_numbers(myms):
    tab = daskms.xds_from_table(
        myms, columns=['SCAN_NUMBER'])
    scan_numbers = numpy.unique(tab[0].SCAN_NUMBER.values)
    return scan_numbers.tolist()


def get_antennas(myms):
    tab = daskms.xds_from_table(
        myms, columns=['ANTENNA1','ANTENNA2'])
    ant1 = numpy.unique(tab[0].ANTENNA1.values)
    ant2 = numpy.unique(tab[0].ANTENNA2.values)
    ants = numpy.unique(numpy.concatenate((ant1,ant2)))
    # ant_tab = xmd.xds_from_table(
    #     myms+'::ANTENNA', columns=['NAME','STATION'])
    # names = ant_tab[0].NAME.values
    # stations = ant_tab[0].STATION.values
    return ants.tolist()

def get_correlations(myms):
    pol_tab = daskms.xds_from_table(
        myms+'::POLARIZATION', columns=['CORR_TYPE'])
    return [STOKES_TYPES[icorr] for icorr in pol_tab[0].CORR_TYPE.values[0]]


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff


def col_to_label(col):
    """Replaces '-' and "/" in column names with palatable characters acceptable in filenames and identifiers"""
    return col.replace("-", "min").replace("/", "div")


def init_ms(msname):
    tab = table(msname, ack=False)
    global valid_ms_columns
    valid_ms_columns = set(tab.colnames())

# set of valid MS columns
valid_ms_columns = set()

# Maps correlation -> callable that extracts that correlation from visibility data
# By default, populated with slicing functions for 0...3,
# but can also be extended with "I", "Q", etx.
corr_data_mappers = OrderedDict({i: lambda x,icorr=i:x[...,icorr] for i in range(4)})

# Maps correlation -> callable that extracts that correlation from flag data
corr_flag_mappers = OrderedDict({i: lambda x,icorr=i:x[...,icorr] for i in range(4)})



class DataMapper(object):
    """This class defines a mapping from a dask group to an array of real values to be plotted"""
    def __init__(self, fullname, unit, mapper=None, column=None, extras=[], conjugate=False, axis=None):
        """
        :param fullname:    full name of parameter (real, amplitude, etc.)
        :param unit:        unit string
        :param mapper:      function that maps column data to parameter
        :param column:      fix a column name (e.g. TIME, UVW. Not for visibility columns.)
        :param extras:      extra arguments needed by mapper (e.g. ["freqs", "wavel"])
        :param conjugate:   sets conjugation flag
        :param axis:        which axis the parameter represets (0 time, 1 freq), if 1-dimensional
        """
        self.fullname, self.unit, self.mapper, self.column, self.extras = fullname, unit, mapper, column, extras
        self.conjugate = conjugate
        self.axis = axis

# this dict maps short axis names into full DataMapper objects
data_mappers = OrderedDict(
    _=DataMapper("", "", lambda x:x),
    a=DataMapper("Amplitude", "", abs),
    p=DataMapper("Phase", "deg", lambda x:da.arctan2(da.imag(x), da.real(x))*180/math.pi),
    r=DataMapper("Real", "", da.real),
    i=DataMapper("Imag", "", da.imag),
    t=DataMapper("Time", "s", axis=0, column="TIME"),
    corr=DataMapper("Correlation", "", column=False, axis=0, extras=["corr"], mapper=lambda x,corr: corr),
    chan=DataMapper("Channel", "", column=False, axis=1, extras=["chans"], mapper=lambda x,chans: chans),
    freq=DataMapper("Frequency", "Hz", column=None, axis=1, extras=["freqs"], mapper=lambda x, freqs: freqs),
    uv=DataMapper("uv-distance", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: da.sqrt((uvw[:,:2]**2).sum(axis=1))/wavel),
    u=DataMapper("u", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: uvw[:, 0] / wavel,
                 conjugate=True),
    v=DataMapper("v", "wavelengths", column="UVW", extras=["wavel"],
                 mapper=lambda uvw, wavel: uvw[:, 1] / wavel,
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
    def parse_datum_spec(cls, axis_spec, default_column=None):
        # figure out the axis specification
        function = column = corr = None
        specs = axis_spec.split(":", 2)
        if len(specs) == 1:
            if specs[0] in valid_ms_columns:    # single column name, identity mapping
                function = '_'
                column = specs[0]
            elif specs[0] in data_mappers:      # single function name ('a', 'p', etc.), default column
                function = specs[0]
                column = data_mappers[function].column or default_column
        elif len(specs) == 2:                   # function:column
            if specs[0] in data_mappers and specs[1] in valid_ms_columns:
                function = specs[0]
                column = specs[1]
        elif len(specs) == 3:
            if specs[0] in data_mappers and specs[1] in valid_ms_columns:
                function = specs[0]
                column = specs[1]
                corr = int(specs[2])
        # better be set now!
        if function is None:
            raise ValueError(f"invalid axis specification '{axis_spec}'")
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
            log.info(f'defining new plot axis: {function} of {column} corr {corr}, clip {minmax}, discretization {ncol}')
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

        # set up discretized continuous axis
        if self.nlevels and vmin is not None and vmax is not None:
            self.discretized_delta = delta = (vmax - vmin) / self.nlevels
            self.discretized_bin_centers = numpy.arange(vmin + delta/2, vmax, delta)
        else:
            self.discretized_delta = self.discretized_bin_centers = None

        # special case of "corr" if corr is fixed: return constant value
        if function == 'corr' and corr is not None:
            self.mapper = DataMapper("Correlation", "", column=False, axis=-1, mapper=lambda x: corr)
        # else find mapper -- register() will have ensured that it is valid
        else:
            self.mapper = data_mappers[function]

        if self.function == "_":
            self.function = ""
        self.conjugate = self.mapper.conjugate
        self.timefreq_axis = self.mapper.axis

        # setup columns
        self._ufunc = None
        self.columns = ()

        # does the mapper have no column (i.e. frequency)?
        if self.mapper.column is False:
            pass
        # does the mapper have a fixed column? This better be consistent
        elif self.mapper.column is not None:
            # if a mapper (such as "uv") implies a fixed column name, make sure it's consistent with what the user said
            if column and self.mapper.column != column:
                raise ValueError(f"'{function}' not applicable with column {column}")
            self.columns = (column,)
        # else arbitrary column
        else:
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

    def get_value(self, group, corr, extras, flag, flag_row):
        coldata = self.get_column_data(group)
        # correlation may be pre-set by plot type, or may be passed to us
        corr = self.corr if self.corr is not None else corr
        # apply correlation reduction
        if coldata is not None and coldata.ndim == 3:
            assert corr is not None
            # the mapper can't have a specific axis set
            if self.mapper.axis is not None:
                raise TypeError(f"{self.name}: unexpected column with ndim=3")
            coldata = corr_data_mappers[corr](coldata)
        # apply mapping function
        coldata = self.mapper.mapper(coldata, **{name:extras[name] for name in self.mapper.extras })
        # scalar expanded to row vector
        if numpy.isscalar(coldata):
            coldata = da.full_like(flag_row, fill_value=coldata, dtype=type(coldata))
            flag = flag_row
        else:
            # determine flags -- start with original flags
            if coldata.ndim == 2:
                flag = corr_flag_mappers[corr](flag)
            elif coldata.ndim == 1:
                if not self.mapper.axis:
                    flag = flag_row
                elif self.mapper.axis == 1:
                    flag = da.zeros_like(coldata, bool)
            # shapes must now match
            if coldata.shape != flag.shape:
                raise TypeError(f"{self.name}: unexpected column shape")
        x0, x1 = self.minmax
        # apply clipping
        if x0 is not None:
            flag = da.logical_or(flag, coldata<self.minmax[0])
        if x1 is not None:
            flag = da.logical_or(flag, coldata>self.minmax[1])
        # discretize
        if self.nlevels:
            # minmax set? discretize over that
            if self.discretized_delta is not None:
                coldata = da.minimum(da.floor((coldata - self.minmax[0])/self.discretized_delta), self.nlevels-1)
            else:
                if not numpy.issubdtype(coldata.dtype, numpy.integer):
                    raise TypeError(f"{self.name}: min/max must be set to colour by non-integer values")
                coldata = da.remainder(coldata, self.nlevels)
        # return masked array
        return dama.masked_array(coldata, flag)


def get_plot_data(myms, group_cols, mytaql, chan_freqs,
                  spws, fields, corrs, noflags, noconj,
                  iter_field, iter_spw, iter_scan,
                  join_corrs=False,
                  row_chunk_size=100000):

    ms_cols = {'FLAG', 'FLAG_ROW'}

    # get visibility columns
    for axis in DataAxis.all_axes.values():
        ms_cols.update(axis.columns)

    # get MS data
    msdata = daskms.xds_from_ms(myms, columns=list(ms_cols), group_cols=group_cols, taql_where=mytaql,
                                chunks=dict(row=row_chunk_size))

    log.info('                 : Indexing MS and building dataframes')

    np = 0  # number of points to plot

    # output dataframes, indexed by (field, spw, scan, antenna, correlation)
    # If any of these axes is not being iterated over, then the index is None
    output_dataframes = OrderedDict()

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

        freqs = chan_freqs[ddid]
        nchan = len(freqs)
        wavel = freq_to_wavel(freqs)
        extras = dict(chans=range(nchan), freqs=freqs, wavel=freq_to_wavel(freqs))
        shape = flag.shape[:-1]
        nrow = flag.shape[0]

        datums = OrderedDict()

        for corr in corrs:
            # make dictionary of extra values for DataMappers
            extras = dict(corr=corr, chans=range(nchan), freqs=freqs, wavel=freq_to_wavel(freqs), nrow=nrow)
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
                    value = axis.get_value(group, corr, extras, flag, flag_row)
                    # reshape values of shape NTIME to (NTIME,1) and NFREQ to (1,NFREQ), and scalar to (NTIME,1)
                    if value.ndim == 1:
                        timefreq_axis = axis.mapper.axis or 0
                        assert value.shape[0] == shape[timefreq_axis], \
                               f"{axis.mapper.fullname}: size {value.shape[0]}, expected {shape[timefreq_axis]}"
                        shape1 = [1,1]
                        shape1[timefreq_axis] = value.shape[0]
                        value = value.reshape(shape1)
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
    categorical_axes = [axis.label for axis in DataAxis.all_axes.values() if axis.nlevels]
    if categorical_axes:
        log.info(": counting colours")
        for key, ddf in list(output_dataframes.items()):
            output_dataframes[key] = ddf.categorize(categorical_axes)

    log.info(": complete")
    return output_dataframes, np

def create_plot(ddf, xdatum, ydatum, cdatum, xcanvas,ycanvas, cmap, bmap, dmap, normalize,
                xlabel, ylabel, title, pngname, bgcol, fontsize, figx=24, figy=12):

    xaxis = xdatum.label
    yaxis = ydatum.label
    caxis = cdatum and cdatum.label
    color_key = ncolors = color_mapping = color_labels = None

    if cdatum is not None:
        log.debug('making raster with color-by')
        canvas = datashader.Canvas(xcanvas, ycanvas)
        raster = canvas.points(ddf, xaxis, yaxis, agg=datashader.count_cat(caxis))
        color_bins = [int(x) for x in getattr(ddf.dtypes, caxis).categories]
        ncolors = len(color_bins)
        # true if axis is discretized and continuous
        if cdatum.discretized_delta is not None:
            # color labels are bin centres
            color_labels = [cdatum.discretized_bin_centers[i] for i in color_bins]
            # map to colors pulled from 256 color map
            color_key = [bmap[(i*256)//cdatum.nlevels] for i in color_bins]
            color_labels = list(map(str, sorted(color_labels)))
            log.info(f": shading using {ncolors} colors (bin centres are {' '.join(color_labels)})")
        # for a discrete axis, just pick the
        else:
            # just use bin numbers to look up a color directly
            color_key = [dmap[i] for i in color_bins]
            # the numbers may be out of order -- reorder for color bar purposes
            bin_color = sorted(zip(color_bins, color_key))
            color_labels = [str(bin) for bin, _ in bin_color]
            color_mapping = [col for _, col in bin_color]
            log.info(f": shading using {ncolors} colors (values {' '.join(color_labels)})")
        img = datashader.transfer_functions.shade(raster, color_key=color_key, how=normalize)
        rgb = holoviews.RGB(holoviews.operation.datashader.shade.uint32_to_uint8_xr(img))
    else:
        log.debug('making raster')
        canvas = datashader.Canvas(xcanvas, ycanvas)
        raster = canvas.points(ddf, xaxis, yaxis)
        log.debug('shading')
        img = datashader.transfer_functions.shade(raster, cmap=cmap, how=normalize)
        rgb = holoviews.RGB(holoviews.operation.datashader.shade.uint32_to_uint8_xr(img))

    log.debug('done')

    # Set plot limits based on data extent or user values for axis labels

    data_xmin = numpy.min(raster.coords[xaxis].values)
    data_xmax = numpy.max(raster.coords[xaxis].values)
    data_ymin = numpy.min(raster.coords[yaxis].values)
    data_ymax = numpy.max(raster.coords[yaxis].values)

    xmin = data_xmin if xdatum.minmax[0] is None else xdatum.minmax[0]
    xmax = data_xmax if xdatum.minmax[1] is None else xdatum.minmax[1]
    ymin = data_ymin if ydatum.minmax[0] is None else ydatum.minmax[0]
    ymax = data_ymax if ydatum.minmax[1] is None else ydatum.minmax[1]

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
    ax.plot(xmin,ymin,'.',alpha=0.0)
    ax.plot(xmax,ymax,'.',alpha=0.0)

    ax.set_xlim([numpy.min((data_xmin,xmin)),
        numpy.max((data_xmax,xmax))])

    ax.set_ylim([numpy.min((data_ymin,ymin)),
        numpy.max((data_ymax,ymax))])

    # colorbar?
    if color_key:
        import matplotlib.colors
        # int axis
        if color_mapping is not None:
            norm = matplotlib.colors.Normalize(-0.5, ncolors-0.5)
            ticks = numpy.arange(ncolors)
            colormap = matplotlib.colors.ListedColormap(color_mapping)
        # discretized axis
        else:
            norm = matplotlib.colors.Normalize(cdatum.minmax[0], cdatum.minmax[1])
            colormap = matplotlib.colors.ListedColormap(bmap)
            # auto-mark colorbar, since it represents a continuous range of values
            ticks = None

        cb = fig.colorbar(matplotlib.cm.ScalarMappable(norm=norm, cmap=colormap), ax=ax, ticks=ticks)

        if color_mapping is not None:
            cb.ax.set_yticklabels(color_labels)

        # mat, ticks=np.arange(np.min(data),np.max(data)+1))

    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(fontsize)
    fig.savefig(pngname, bbox_inches='tight')

    pylab.close()

    return pngname

