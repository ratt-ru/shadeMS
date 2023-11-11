import argparse

import dask
import dask.array as da
from daskms import xds_from_ms
import numpy as np


def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument("ms")
    p.add_argument("-rc", "--row-chunks", type=int, default=10000)
    return p


def _unique_time_metadata(time):
    return np.unique(time, return_inverse=True, return_counts=True)


def _unique_baseline_metadata(ant1, ant2):
    return np.unique(np.stack([ant1, ant2], axis=1),
                     axis=0, return_inverse=True, return_counts=True)


def sum_at_unique(utime_meta, bl_meta, data):
    utime, time_inv, time_count = utime_meta
    ubl, bl_inv, bl_count = bl_meta
    ntime = utime.shape[0]
    nbl = ubl.shape[0]
    nchan, ncorr = data.shape[1:]

    data_sum = np.zeros((ntime, nbl, nchan, ncorr), dtype=data.dtype)
    data_counts = np.zeros(data_sum.shape, dtype=np.intp)

    np.add.at(data_sum, (time_inv, bl_inv), data)
    np.add.at(data_counts, (time_inv, bl_inv), 1)

    print("Chunk shape %s" % (data_sum.shape,))

    return (utime, time_count), (ubl, bl_count), (data_sum, data_counts)


def _noop(x, axis, keepdims):
    """ Don't transform data on initial step """
    return x


def _data_sum_combine(x, axis, keepdims):
    """
    da.reduction can either supply list[value] or value when concatenate=False
    value in this case is the tuple of length 3
    """

    if isinstance(x, list):
        # Transpose to get lists of metadata
        time_meta, bl_meta, data_meta = zip(*x)

        # Transpose again to lists of times and time counts
        times, time_counts = (np.concatenate(a) for a in zip(*time_meta))
        utime, time_inv = np.unique(times, return_inverse=True)
        new_time_counts = np.zeros(utime.shape[0], time_counts[0].dtype)
        np.add.at(new_time_counts, time_inv, time_counts)

        # Transpose again to lists of baselines and baseline counts
        bl, bl_counts = (np.concatenate(a) for a in zip(*bl_meta))
        ubl, bl_inv = np.unique(bl, axis=0, return_inverse=True)
        new_bl_counts = np.zeros(ubl.shape[0], bl_counts[0].dtype)
        np.add.at(new_bl_counts, bl_inv, bl_counts)

        # Determine output dimensions
        ntime = utime.shape[0]
        nbl = ubl.shape[0]
        first_data = data_meta[0][0]
        nchan, ncorr = first_data.shape[2:]

        # Output data and count arrays
        new_data = np.zeros((ntime, nbl, nchan, ncorr), dtype=first_data.dtype)
        new_counts = np.zeros(new_data.shape, dtype=np.intp)

        print("Combining shape %s" % (new_data.shape,))

        t_start = 0
        bl_start = 0
        it = zip(time_meta, bl_meta, data_meta)

        # Iterate over times, baselines and data to aggregate
        for (time, _), (bl, _), (data, data_counts) in it:
            t_end = t_start + time.shape[0]
            bl_end = bl_start + bl.shape[0]

            # Each iteration is associated with a particular range
            # in the time and baseline inverses
            idx = (time_inv[t_start:t_end, None],
                   bl_inv[None, bl_start:bl_end])

            # Accumulate data associated with this iteration into
            # at the appropriate locations
            np.add.at(new_data, idx, data)
            np.add.at(new_counts, idx, data_counts)

            t_start = t_end
            bl_start = bl_end

        return ((utime, new_time_counts),
                (ubl, new_bl_counts),
                (new_data, new_counts))

    elif isinstance(x, tuple):
        return x
    else:
        raise TypeError("Unhandled x type %s" % type(x))


def _data_sum_agg(x, axis, keepdims):
    _, _, (data, counts) = _data_sum_combine(x, axis, keepdims)
    return data / counts


def script():
    args = create_parser().parse_args()

    for ds in xds_from_ms(args.ms, group_cols=["DATA_DESC_ID"],
                          chunks={'row': args.row_chunks}):

        time_meta = da.blockwise(_unique_time_metadata, ("row",),
                                 ds.TIME.data, ("row",),
                                 meta=np.empty((0,), dtype=np.object),
                                 dtype=np.object)

        bl_meta = da.blockwise(_unique_baseline_metadata, ("row",),
                               ds.ANTENNA1.data, ("row",),
                               ds.ANTENNA2.data, ("row",),
                               meta=np.empty((0,), dtype=np.object),
                               dtype=np.object)

        data_sum = da.blockwise(sum_at_unique, ("row", "chan", "corr"),
                                time_meta, ("row",),
                                bl_meta, ("row",),
                                ds.DATA.data, ("row", "chan", "corr"),
                                meta=np.empty((0, 0, 0), dtype=np.object),
                                dtype=np.object)

        # Reduce over the row dimension with
        # _utime_combine to combine chunks together
        # _utime_agg to produce the final reduced chunk  of (utime, time_avg)
        reduction = da.reduction(data_sum,
                                 # Function for transforming initial chunks
                                 chunk=_noop,
                                 # Function for combining adjacent chunks
                                 combine=_data_sum_combine,
                                 # Function for producing final chunk
                                 aggregate=_data_sum_agg,
                                 # Don't np.concatenate chunks (they're tuples)
                                 concatenate=False,
                                 # Reduce over dimension 0 ('row')
                                 axis=0,
                                 # Keep row dimension
                                 keepdims=True,
                                 # We don't know how many elements are present
                                 # in the final reduction
                                 output_size=np.nan,
                                 # Tell dask we're producing a 3D numpy array
                                 meta=np.empty((0, 0, 0, 0), dtype=np.object),
                                 dtype=np.object)

        print("DATA size %.3fGB" % (ds.DATA.data.nbytes / (1024.**3)))

        result = reduction.compute()
        a = result.reshape((-1,) + result.shape[2:])

        assert np.allclose(a, ds.DATA.values)


        # print(dask.compute(reduction, scheduler='sync')[0].shape)


if __name__ == "__main__":
    script()
