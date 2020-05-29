# -*- coding: future_fstrings -*-

import datashape
import datashape.coretypes as ct
import datashader.reductions
import numpy as np

try:
    import cudf
except ImportError:
    cudf = None

# couldn't find anything in the datashape docs about how to check if a CType is an integer,
# so just define a big set
IntegerTypes = {ct.bool_, ct.uint8, ct.uint16, ct.uint32, ct.uint64, ct.int8, ct.int16, ct.int32, ct.int64}


def _column_modulo(df, column, modulo):
    """
    Helper function. Takes a DataFrame column, modulo integer value
    """
    if cudf and isinstance(df, cudf.DataFrame):
        ## dunno how to do this in CUDA
        raise NotImplementedError("this feature is not implemented in cudf")
        ## but is it as simple as this?
        # return (df[column] % modulo).to_gpu_array()
    else:
        return df[column].values % modulo


def _column_discretization(df, column, offset, delta, nsteps):
    """
    Helper function. Takes a DataFrame column, modulo integer value
    """
    if cudf and isinstance(df, cudf.DataFrame):
        ## dunno how to do this in CUDA
        raise NotImplementedError("this feature is not implemented in cudf")
    else:
        value = df[column].values
        index = ((value - offset) / delta).astype(np.uint32)
        index[index < 0] = 0
        index[index >= nsteps] = nsteps - 1
        index[np.isnan(value)] = nsteps
        return index


class integer_modulo(datashader.reductions.category_codes):
    """A variation on category_codes that replaces categories by the values from an integer column, modulo a certain number"""
    def __init__(self, column, modulo):
        super().__init__(column)
        self.modulo = modulo

    def apply(self, df):
        return _column_modulo(df, self.column, self.modulo)


class float_discretization(integer_modulo):
    """A variation on category_codes that replaces categories by the values from an integer column, modulo a certain number"""
    def __init__(self, column, offset, delta, nsteps):
        super().__init__(column, nsteps)
        self.offset = offset
        self.delta = delta
        self.nsteps = nsteps

    def apply(self, df):
        return _column_discretization(df, self.column, self.offset, self.delta, self.nsteps)


class integer_modulo_values(datashader.reductions.category_values):
    """A variation on category_values that replaces categories by the values from an integer column, modulo a certain number"""
    def __init__(self, columns, modulo):
        super().__init__(columns)
        self.modulo = modulo

    def apply(self, df):
        a = _column_modulo(df, self.columns[0], self.modulo)
        return self._attach_value(df, a)

    def _attach_value(self, df, a):
        if cudf and isinstance(df, cudf.DataFrame):
            import cupy
            if df[self.columns[1]].dtype.kind == 'f':
                nullval = np.nan
            else:
                nullval = 0
            b = df[self.columns[1]].to_gpu_array(fillna=nullval)
            return cupy.stack((a, b), axis=-1)
        else:
            b = df[self.columns[1]].values
            return np.stack((a, b), axis=-1)


class float_discretization_values(integer_modulo_values):
    """A variation on category_codes that replaces categories by the values from an integer column, modulo a certain number"""
    def __init__(self, columns, offset, delta, nsteps):
        super().__init__(columns, nsteps+1)  # one extra category for NaNs
        self.offset = offset
        self.delta = delta
        self.nsteps = nsteps

    def apply(self, df):
        a = _column_discretization(df, self.columns[0], self.offset, self.delta, self.nsteps)
        return self._attach_value(df, a)


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
        if not in_dshape.measure[self.cat_column] in IntegerTypes:
            raise ValueError("input must be an integer column")
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
    """Like datashader.by, but for float-valued columns, discretized by steps over a certain span"""
    def __init__(self, cat_column, reduction, offset, delta, nsteps):
        super().__init__(cat_column, reduction, nsteps+1)  # allocate extra category for NaNs
        self.offset = offset
        self.delta = delta
        self.nsteps = nsteps

    def _build_temps(self, cuda=False):
        return tuple(by_span(self.cat_column, tmp, self.offset, self.delta, self.nsteps)
                     for tmp in self.reduction._build_temps(cuda))

    def validate(self, in_dshape):
        if not self.cat_column in in_dshape.dict:
            raise ValueError("specified column not found")
        self.reduction.validate(in_dshape)

    @property
    def inputs(self):
        if self.val_column is not None:
            return (float_discretization_values(self.columns, self.offset, self.delta, self.nsteps),)
        else:
            return (float_discretization(self.columns[0], self.offset, self.delta, self.nsteps),)

    def _build_bases(self, cuda=False):
        bases = self.reduction._build_bases(cuda)
        if len(bases) == 1 and bases[0] is self:
            return bases
        return tuple(by_span(self.cat_column, base, self.offset, self.delta, self.nsteps) for base in bases)

