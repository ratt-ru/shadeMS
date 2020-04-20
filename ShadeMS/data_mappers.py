import dask.array as da
import dask.array.ma as dama
import xarray
import numpy
import math
import re
import argparse
from collections import OrderedDict
from ShadeMS import log

USE_COUNT_CAT = 0
COUNT_DTYPE = numpy.int16

def add_options(parser):
    parser.add_argument('--count-cat', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--count-dtype', default='int16', help=argparse.SUPPRESS)

def set_options(options):
    global USE_COUNT_CAT, COUNT_DTYPE
    USE_COUNT_CAT = options.count_cat
    COUNT_DTYPE = getattr(numpy, options.count_dtype)

def col_to_label(col):
    """Replaces '-' and "/" in column names with palatable characters acceptable in filenames and identifiers"""
    return col.replace("-", "min").replace("/", "div")


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
    def is_legit_colspec(cls, colspec, ms):
        match = re.fullmatch(r"(\w+)([*/+-])(\w+)", colspec)
        if match:
            return match.group(1) in ms.valid_columns and match.group(3) in ms.valid_columns
        else:
            return colspec in ms.valid_columns

    @classmethod
    def parse_datum_spec(cls, axis_spec, default_column=None, ms=None):
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
            elif DataAxis.is_legit_colspec(spec, ms):
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
    def register(cls, function, column, corr, ms, minmax=None, ncol=None, subset=None):
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
        minmax = tuple(minmax) if minmax is not None else (None, None)
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
            axis = cls.all_axes[key] = DataAxis(column, function, corr, ms, minmax, ncol, label, subset=subset)
            return axis

    def __init__(self, column, function, corr, ms, minmax=None, ncol=None, label=None, subset=None):
        """See register() class method above. Not called directly."""
        self.name = ":".join([str(x) for x in (function, column, corr, minmax, ncol) if x is not None ])
        self.ms = ms
        self.function = function        # function to apply to column (see list of DataMappers below)
        self.corr     = corr if corr != "all" else None
        self.nlevels  = ncol
        self.minmax   = vmin, vmax = tuple(minmax) if minmax is not None else (None, None)
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
        if function == 'CORR':
            # special case of "corr" if corr is fixed: return constant value fixed here
            if corr is not None:
                self.mapper = DataMapper("Correlation", "", column=False, axis=-1, mapper=lambda x: corr)
            self.discretized_labels = [name for name in ms.corr.names if name in subset.corr]
        elif column == "FIELD_ID":
            self.discretized_labels = [name for name in ms.field.names if name in subset.field]
        elif column == "ANTENNA1" or column == "ANTENNA2":
            self.discretized_labels = [name for name in ms.all_antenna.names if name in subset.ant]

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
            coldata = self.ms.corr_data_mappers[corr](coldata)
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
                flag = self.ms.corr_flag_mappers[corr](flag)
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

