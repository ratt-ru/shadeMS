import dask.array as da
import numpy as np
import xarray

from shade_ms.data_mappers import DataAxis


def _spw(freqs):
    return xarray.DataArray(da.from_array(np.array(freqs), chunks=len(freqs)), dims=("chan",))


def test_const_axis_minmax_spans_all_groups():
    """A constant axis accumulates its range over every group, not just the narrowest one."""
    axis = DataAxis(None, "FREQ", False, ms=None, label="FREQ")
    for freqs in ([1e9, 2e9], [3e9, 4e9]):   # two SPWs, as ddids of differing coverage
        axis.get_value(None, None, dict(freqs=_spw(freqs)),
                       flag=None, flag_row=None, chanslice=slice(None))
    assert axis.minmax == (1e9, 4e9)
