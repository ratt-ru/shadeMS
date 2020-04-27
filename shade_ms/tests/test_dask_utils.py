import dask
import dask.array as da
import dask.dataframe as dd
import numpy as np
from numpy.testing import assert_array_equal
import pytest

from shade_ms.dask_utils import dataframe_factory


@pytest.mark.parametrize("test_nan_shapes", [True, False])
def test_dataframe_factory(test_nan_shapes):
    nrow, nfreq = 100, 100

    data1a = da.arange(nrow, chunks=(10,))

    # Generate nan chunk shapes in data1a if requested
    if test_nan_shapes:
        data1a = data1a[da.where(data1a > 4)]

    data1b = da.zeros(dtype=float, shape=(nfreq,), chunks=(100,))

    df = dataframe_factory(("row", "chan"),
                           data1a, ("row",),
                           data1b, ("chan",))

    assert isinstance(df, dd.DataFrame)
    assert isinstance(df['x'], dd.Series)
    assert isinstance(df['y'], dd.Series)

    if test_nan_shapes:
        # With unknown shapes, we broadcast our arrays in numpy
        # to test
        x, y = dask.compute(data1a[:, None], data1b[None, :])
        x, y = np.broadcast_arrays(x, y)
        x = x.ravel()
        y = y.ravel()

        # Unknown divisions
        assert df.divisions == (None,) * (df.npartitions + 1)
    else:
        # With know chunks we can broadcast our arrays in dask
        x, y = da.broadcast_arrays(data1a[:, None], data1b[None, :])
        x = x.ravel()
        y = y.ravel()

        # Known divisions
        assert df.divisions == (0, 1000, 2000, 3000, 4000, 5000,
                                6000, 7000, 8000, 9000, 10000)
        assert df['x'].npartitions == x.npartitions
        assert df['y'].npartitions == y.npartitions

    # Compare our lazy dataframe series vs (dask or numpy) arrays
    assert_array_equal(df['x'], x)
    assert_array_equal(df['y'], y)
