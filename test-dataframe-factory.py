import dask.array as da
from shade_ms.dask_utils import dataframe_factory, multicol_dataframe_factory


nrow, nfreq, ncorr = 100, 100, 4

data1a = da.arange(nrow, chunks=(10,))

data1b = da.zeros(dtype=float, shape=(nfreq,), chunks=(100,))

data1c = da.zeros(dtype=float, shape=(nfreq,ncorr), chunks=(100,4))

data1d = da.zeros(dtype=float, shape=())

df = dataframe_factory(("row", "chan"),
                       data1a, ("row",),
                       data1b, ("chan",))

df1 = multicol_dataframe_factory(("row", "chan", "corr"),
                                 dict(a=data1a, b=data1b, x=data1c, y=data1d),
                                 dict(a=("row",), b=("chan",), x=("chan", "corr"), y=()))

print(df1['y'])