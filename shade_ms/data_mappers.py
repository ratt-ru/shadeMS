import dask.array as da
import dask.array.core
import dask.array.ma as dama
import xarray
import numpy as np
import math
import re
import argparse
from . import ms_info
from collections import OrderedDict
from shade_ms import log

USE_COUNT_CAT = 0
COUNT_DTYPE = np.int16

def add_options(parser):
    parser.add_argument('--count-cat', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--count-dtype', default='int16', help=argparse.SUPPRESS)

def set_options(options):
    global USE_COUNT_CAT, COUNT_DTYPE
    USE_COUNT_CAT = options.count_cat
    COUNT_DTYPE = getattr(np, options.count_dtype)

def col_to_label(col):
    """Replaces '-' and "/" in column names with palatable characters acceptable in filenames and identifiers"""
    return col.replace("-", "min").replace("/", "div")


class DataMapper(object):
    """This class defines a mapping from a dask group to an array of real values to be plotted"""
    def __init__(self, fullname, unit, mapper, column=None, extras=[], conjugate=False, axis=None, const=False):
        """
        :param fullname:    full name of parameter (real, amplitude, etc.)
        :param unit:        unit string
        :param mapper:      function that maps column data to parameter
        :param column:      fix a column name (e.g. TIME, UVW. Not for visibility columns.)
        :param extras:      extra arguments needed by mapper (e.g. ["freqs", "wavel"])
        :param conjugate:   sets conjugation flag
        :param axis:        which axis the parameter represets (0 time, 1 freq), if 1-dimensional
        :param const:       if True, axis is constant (does not depend on MS rows)
        """
        assert mapper is not None
        self.fullname, self.unit, self.mapper, self.column, self.extras = fullname, unit, mapper, column, extras
        self.conjugate = conjugate
        self.axis = axis
        # if
        self.const = const


_identity = lambda x:x

# this dict maps short axis names into full DataMapper objects
data_mappers = OrderedDict(
    _     = DataMapper("", "", _identity),
    amp   = DataMapper("amplitude", "", abs),
    logamp = DataMapper("log-amplitude", "", lambda x:da.log10(abs(x))),
    phase = DataMapper("phase", "deg", lambda x:da.arctan2(da.imag(x), da.real(x))*180/math.pi),
    real  = DataMapper("real", "", da.real),
    imag  = DataMapper("imag", "", da.imag),
    TIME  = DataMapper("time", "s", axis=0, column="TIME", mapper=_identity),
    ROW   = DataMapper("row number", "", column=False, axis=0, extras=["rows"], mapper=lambda x,rows: rows),
    BASELINE = DataMapper("baseline", "", column=False, axis=0, extras=["baselines"], mapper=lambda x,baselines: baselines),
    BASELINE_M = DataMapper("baseline", "", column=False, axis=0, extras=["baselines"], mapper=lambda x,baselines: baselines),
    CORR  = DataMapper("correlation", "", column=False, axis=0, extras=["corr"], mapper=lambda x,corr: corr, const=True),
    CHAN  = DataMapper("channel", "", column=False, axis=1, extras=["chans"], mapper=lambda x,chans: chans, const=True),
    FREQ  = DataMapper("frequency", "Hz", column=False, axis=1, extras=["freqs"], mapper=lambda x, freqs: freqs, const=True),
    WAVEL = DataMapper("wavelength", "m", column=False, axis=1, extras=["wavel"], mapper=lambda x, wavel: wavel, const=True),
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
                    corr1 = ms.all_corr[spec]
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
        # if mapper has a "False" column, it's a fake column...
        mapper_column = data_mappers[function].column
        column = (column or mapper_column or default_column) if mapper_column is not False else None
        has_corr_axis = (column and (column.endswith("DATA") or column.endswith("SPECTRUM"))) or function == "CORR"
        if not has_corr_axis:
            if corr is not None:
                raise ValueError(f"'{axis_spec}': can't specify correlation when column is '{column}'")
            corr = False  # corr=False marks a datum without a correlation axis
        return function, column, corr, (has_corr_axis and corr is None)

    @classmethod
    def register(cls, function, column, corr, ms, minmax=None, ncol=None, subset=None, minmax_cache=None):
        """
        Registers a data axis, which ultimately ends up as a column in the assembled dataframe.
        For multiple plots, we want to reuse the same information (assuming the same
        clipping limits, etc.) so we have a dictionary of axis definitions here.

        Columns selects a column to operate on.
        Function selects a mapping (see datamappers below).
        Corr selects a correlation (or a Stokes product such as I, Q,...)
        minmax sets axis clipping levels
        ncol discretizes the axis into N colours between min and max
        minmax_cache provides a dict of cached min/max values, which will be looked up via the label, if minmax
                     is not explicitly set
        """
        # form up label
        label  = "{}_{}_{}".format(col_to_label(column or ''), function, corr)
        minmax = tuple(minmax) if minmax is not None else (None, None)
        key = label, minmax, ncol
        # see if this axis definition already exists, else create new one
        if key in cls.all_axes:
            return cls.all_axes[key]
        else:
            # see if minmax should be loaded
            if (minmax is None or tuple(minmax) == (None,None)) and minmax_cache and label in minmax_cache:
                log.info(f"loading {label} min/max from cache")
                minmax = minmax_cache[label]

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
        self.minmax   = tuple(minmax) if minmax is not None else (None, None)
        self._minmax_autorange = (self.minmax == (None, None))

        self.label    = label
        self._corr_reduce = None
        self._is_discrete = None
        self.mapper = data_mappers[function]

        # if set, axis is discrete and labelled
        self.discretized_labels = None

        # for discrete axes: if a subset of N indices is explicitly selected for plotting, then this
        # is a list of the selected indices, of length N
        self.subset_indices = None
        # ...and this is a dask array that maps selected indices into bins 0...N-1, and all other values into bin N
        self.subset_remapper = None
        # ...and this is the maximum valid index in MS
        maxind = None

        # columns with labels?
        if function == 'CORR' or function == 'STOKES':
            corrset = set(subset.corr.names)
            corrset1 = corrset - set("IQUV")
            if corrset1 == corrset:
                name = "Correlation"
            elif not corrset1:
                name = "Stokes"
            else:
                name = "Correlation or Stokes"
            # special case of "corr" if corr is fixed: return constant value fixed here
            # When corr is fixed, we're creating a mapper for a know correlation. When corr is not fixed,
            # we're creating one for a mapper that will iterate over correlations
            if corr is not None:
                self.mapper = DataMapper(name, "", column=False, axis=-1, mapper=lambda x: corr)
            self.subset_indices = subset.corr
            maxind = ms.all_corr.numbers[-1]
        elif column == "FIELD_ID":
            self.subset_indices = subset.field
            maxind = ms.field.numbers[-1]
        elif column == "ANTENNA1" or column == "ANTENNA2":
            self.subset_indices = subset.ant
            maxind = ms.antenna.numbers[-1]
        elif column == "SCAN_NUMBER":
            self.subset_indices = subset.scan
            maxind = ms.scan.numbers[-1]
        elif function == "BASELINE":
            self.subset_indices = subset.baseline
            maxind = ms.baseline.numbers[-1]
        elif function == "BASELINE_M":
            bl_subset = set(subset.baseline.numbers)   # active baselines
            numbers = [i for i in ms.baseline_m.numbers if i in bl_subset]
            names = [bl for i, bl in zip(ms.baseline_m.numbers, ms.baseline_m.names) if i in bl_subset]
            self.subset_indices = ms_info.NamedList("baseline_m", names, numbers)
            maxind = ms.baseline.numbers[-1]
        elif column == "FLAG" or column == "FLAG_ROW":
            self.discretized_labels = ["F", "T"]

        # make a remapper
        if self.subset_indices is not None:
            # if the mapping from indices to bins 1:1?
            subind = np.array(self.subset_indices.numbers)
            identity = subind[0] == 0 and ((subind[1:]-subind[:-1]) == 1).all()
            # If mapping is not 1:1, or subset is short of full set, then we need a remapper.
            # Map indices in subset into their ordinal numbers in the subset (0...N-1), and all other indices to N
            if len(self.subset_indices) < maxind+1 or not identity:
                remapper = np.full(maxind+1, len(self.subset_indices))
                for i, index in enumerate(self.subset_indices.numbers):
                    remapper[index] = i
                self.subset_remapper = da.array(remapper)
            self.discretized_labels = self.subset_indices.names
            self.subset_indices = self.subset_indices.numbers

        if self.discretized_labels:
            self._is_discrete = True

        # axis name
        self.fullname = self.mapper.fullname or column or ''

        # if labels were set up, adjust nlevels (but only down, never up)
        if self.discretized_labels is not None and self.nlevels is not None:
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

    @property
    def is_discrete(self):
        return self._is_discrete

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
        # if isinstance(flag, xarray.DataArray):
        #     flag = flag.data

        # if isinstance(flag_row, xarray.DataArray):
        #     flag_row = flag_row.data

        coldata = self.get_column_data(group)

        # if isinstance(coldata, xarray.DataArray):
        #     coldata = coldata.data

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
        mapper = self.mapper
        # complex values with an identity mapper get an amp mapper assigned to them by default
        if np.iscomplexobj(coldata) and mapper is data_mappers["_"]:
            mapper = data_mappers["amp"]
        coldata = mapper.mapper(coldata, **{name:extras[name] for name in self.mapper.extras })
        # for a constant axis, compute minmax on the fly
        if mapper.const and self._minmax_autorange:
            if np.isscalar(coldata):
                min1 = max1 = coldata
            else:
                min1, max1 = coldata.data.min(), coldata.data.max()
            self.minmax = min(self.minmax[0], min1) if self.minmax[0] is not None else min1, \
                          min(self.minmax[1], max1) if self.minmax[1] is not None else max1
        # scalar is just a scalar
        if np.isscalar(coldata):
            coldata = da.array(coldata)
            flag = None
        else:
            # apply channel slicing, if there's a channel axis in the array (and the array is a DataArray)
            if type(coldata) is xarray.DataArray and 'chan' in coldata.dims:
                coldata = coldata[dict(chan=chanslice)]
            # determine flags -- start with original flags
            if flag is not None:
                if coldata.ndim == 2:
                    flag = self.ms.corr_flag_mappers[corr](flag)
                elif coldata.ndim == 1:
                    if not self.mapper.axis:
                        flag = flag_row
                    elif self.mapper.axis == 1:
                        flag = None
                # shapes must now match
                if flag is not None and coldata.shape != flag.shape:
                    raise TypeError(f"{self.name}: unexpected column shape")
        # # discretize
        # if self.nlevels:

        if coldata.dtype is bool or np.issubdtype(coldata.dtype, np.integer):
            if self._is_discrete is False:
                raise TypeError(f"{self.label}: column changed from continuous-valued to discrete. This is a bug, or a very weird MS.")
            self._is_discrete = True
            # do we need to apply a remapping?
            if self.subset_remapper is not None:
                if type(coldata) is not dask.array.core.Array:  # could be xarray backed by dask array
                    coldata = coldata.data
                coldata = self.subset_remapper[coldata]
                bad_bins = da.greater_equal(coldata, len(self.subset_indices))
                if flag is None:
                    flag = bad_bins
                else:
                    flag = da.logical_or(flag.data, bad_bins)
        else:
            if self._is_discrete is True:
                raise TypeError(f"{self.label}: column chnaged from discrete to continuous-valued. This is a bug, or a very weird MS.")
            self._is_discrete = False

        if isinstance(coldata, xarray.DataArray):
            coldata = coldata.data

        if isinstance(flag, xarray.DataArray):
            flag = flag.data

        bad_data = da.logical_not(da.isfinite(coldata))
        if flag is not None:
            return dama.masked_array(coldata, da.logical_or(flag, bad_data))
        else:
            return dama.masked_array(coldata, bad_data)

