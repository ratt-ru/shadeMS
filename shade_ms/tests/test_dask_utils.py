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

    data1b = da.random.random(size=(nfreq,), chunks=(100,))

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
    assert_array_equal(df['x'].min(), data1a.min())
    assert_array_equal(df['y'].min(), data1b.min())
    assert_array_equal(df['x'].max(), data1a.max())
    assert_array_equal(df['y'].max(), data1b.max())


def test_dataframe_factory_multicol():
    nrow, nfreq, ncorr = 100, 100, 4

    data1a = da.random.random(size=nrow, chunks=(10,))
    data1b = da.random.random(size=(nfreq, ncorr), chunks=(100, 4))
    data1c = da.random.random(size=(ncorr,), chunks=(4,))

    df = dataframe_factory(("row", "chan", "corr"),
                           data1a, ("row",),
                           data1b, ("chan", "corr"),
                           data1c, ("corr",))

    assert isinstance(df, dd.DataFrame)
    assert isinstance(df['x'], dd.Series)
    assert isinstance(df['y'], dd.Series)
    assert isinstance(df['c0'], dd.Series)


    x, y, c0 = da.broadcast_arrays(data1a[:, None, None],
                                   data1b[None, :, :],
                                   data1c[None, None, :])

    assert_array_equal(df['x'], x.ravel())
    assert_array_equal(df['y'], y.ravel())
    assert_array_equal(df['c0'], c0.ravel())

    assert_array_equal(df['x'].min(), data1a.min())
    assert_array_equal(df['x'].max(), data1a.max())
    assert_array_equal(df['y'].min(), data1b.min())
    assert_array_equal(df['y'].max(), data1b.max())
    assert_array_equal(df['c0'].min(), data1c.min())
    assert_array_equal(df['c0'].max(), data1c.max())

    df = dd.multi.concat([df, df])

    assert_array_equal(df['x'].min(), data1a.min())
    assert_array_equal(df['x'].max(), data1a.max())
    assert_array_equal(df['y'].min(), data1b.min())
    assert_array_equal(df['y'].max(), data1b.max())
    assert_array_equal(df['c0'].min(), data1c.min())
    assert_array_equal(df['c0'].max(), data1c.max())
