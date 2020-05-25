# -*- coding: future_fstrings -*-

import datashape.coretypes
import datashader.transfer_functions
import datashader.reductions

import xarray
import datashape.coretypes
import datashader.transfer_functions
import datashader.reductions
import numpy as np
from datashader.utils import ngjit
from datashader.reductions import Preprocess

try:
    import cudf
except ImportError:
    cudf = None

# class count_integers(datashader.count_cat):
#     """Aggregator. Counts of all elements in ``column``, grouped by value of another column. Like datashader.count_cat,
#     but for normal columns."""
#     _dshape = datashape.dshape(datashape.coretypes.int32)
#
#     def __init__(self, column, modulo):
#         datashader.count_cat.__init__(self, column)
#         self.modulo = modulo
#         self.codes = xarray.DataArray(list(range(self.modulo)))
#
#     def validate(self, in_dshape):
#         pass
#
#     @property
#     def inputs(self):
#         return (datashader.reductions.extract(self.column), )
#
#     @staticmethod
#     @ngjit
#     def _append(x, y, agg, field):
#         agg[y, x, int(field)] += 1
#
#     def out_dshape(self, input_dshape):
#         return datashape.util.dshape(datashape.Record([(c, datashape.coretypes.int32) for c in range(self.modulo)]))
#
#     def _build_finalize(self, dshape):
#         def finalize(bases, cuda=False, **kwargs):
#             dims = kwargs['dims'] + [self.column]
#             coords = kwargs['coords']
#             coords[self.column] = list(self.codes.values)
#             return xarray.DataArray(bases[0], dims=dims, coords=coords)
#         return finalize


class extract_multi(datashader.reductions.category_values):
    """Extract multiple columns from a dataframe as a numpy array of values. Like datashader.category_values,
    but with two columns."""
    def apply(self, df):
        if cudf and isinstance(df, cudf.DataFrame):
            import cupy
            cols = []
            for column in self.columns:
                nullval = np.nan if df[self.columns[1]].dtype.kind == 'f' else 0
                cols.append(df[self.columns[0]].to_gpu_array(fillna=nullval))
            return cupy.stack(cols, axis=-1)
        else:
            return np.stack([df[col].values for col in self.columns], axis=-1)

class integer_modulo(Preprocess):
    def __init__(self, column, modulo):
        super().__init__(column)
        self.modulo = modulo

    """Extract just the category codes from a categorical column."""
    def apply(self, df):
        if cudf and isinstance(df, cudf.DataFrame):
            return (df[self.column] % self.modulo).to_gpu_array()
        else:
            return df[self.column].values % self.modulo

class integer_modulo_values(Preprocess):
    """Extract multiple columns from a dataframe as a numpy array of values."""
    def __init__(self, columns, modulo):
        self.columns = list(columns)
        self.modulo = modulo

    @property
    def inputs(self):
        return self.columns

    def apply(self, df):
        if cudf and isinstance(df, cudf.DataFrame):
            import cupy
            if df[self.columns[1]].dtype.kind == 'f':
                nullval = np.nan
            else:
                nullval = 0
            a = (df[self.columns[0]] % self.modulo).to_gpu_array()
            b = df[self.columns[1]].to_gpu_array(fillna=nullval)
            return cupy.stack((a, b), axis=-1)
        else:
            a = df[self.columns[0]].values % self.modulo
            b = df[self.columns[1]].values
            return np.stack((a, b), axis=-1)


class by_integers(datashader.by):
    """Like datashader.by, but for integer-valued columns."""
    def __init__(self, cat_column, reduction, modulo):
        super().__init__(cat_column, reduction)
        self.modulo = modulo

    def _build_temps(self, cuda=False):
        return tuple(by_integers(self.cat_column, tmp, self.modulo) for tmp in self.reduction._build_temps(cuda))

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
            return (integer_modulo_values(self.columns, self.modulo),)
        else:
            return (integer_modulo(self.columns[0], self.modulo),)

    def _build_bases(self, cuda=False):
        bases = self.reduction._build_bases(cuda)
        if len(bases) == 1 and bases[0] is self:
            return bases
        return tuple(by_integers(self.cat_column, base, self.modulo) for base in bases)

    def _build_finalize(self, dshape):
        cats = list(range(self.modulo))

        def finalize(bases, cuda=False, **kwargs):
            kwargs['dims'] += [self.cat_column]
            kwargs['coords'][self.cat_column] = cats
            return self.reduction._finalize(bases, cuda=cuda, **kwargs)

        return finalize

class by_span(by_integers):
    """Like datashader.by, but for float-valued columns."""
    def __init__(self, cat_column, reduction, offset, delta, nsteps):
        super().__init__(cat_column, reduction, nsteps+1)  # allocate extra category for NaNs
        self.offset = offset
        self.delta = delta
        self.nsteps = nsteps

    def _build_temps(self, cuda=False):
        return tuple(by_span(self.cat_column, tmp, self.offset, self.delta, self.nsteps) for tmp in self.reduction._build_temps(cuda))

    def _build_bases(self, cuda=False):
        bases = self.reduction._build_bases(cuda)
        if len(bases) == 1 and bases[0] is self:
            return bases
        return tuple(by_span(self.cat_column, base, self.offset, self.delta, self.nsteps) for base in bases)

    def _build_append(self, dshape, schema, cuda=False):
        f = self.reduction._build_append(dshape, schema, cuda)
        # because we transposed, we also need to flip the
        # order of the x/y arguments
        if isinstance(self.reduction, datashader.reductions.m2):
            def _categorical_append(x, y, agg, cols, tmp1, tmp2, minval=self.offset, d=self.delta, n=self.nsteps):
                _agg = agg.transpose()
                value = cols[0]
                _ind = min(max(0, int((value - minval)/d)), n-1) if value == value else n  # value != itself when NaN
                #print("a", _ind, cols[0])
                f(y, x, _agg[_ind], cols[1], tmp1[_ind], tmp2[_ind])
        elif self.val_column is not None:
            def _categorical_append(x, y, agg, field, minval=self.offset, d=self.delta, n=self.nsteps):
                _agg = agg.transpose()
                value = field[0]
                _ind = min(max(0, int((value - minval)/d)), n-1) if value == value else n  # value != itself when NaN
                #print("b", _ind, field[0])
                f(y, x, _agg[_ind], field[1])
        else:
            def _categorical_append(x, y, agg, field, minval=self.offset, d=self.delta, n=self.nsteps):
                _agg = agg.transpose()
                value = field
                _ind = min(max(0, int((value - minval)/d)), n-1) if value == value else n  # value != itself when NaN
                #print("c", _ind, field, field != field)
                f(y, x, _agg[_ind])

        return ngjit(_categorical_append)
