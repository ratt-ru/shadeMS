import argparse

import dask.array as da
import numpy as np

from africanus.averaging.dask import time_and_channel
from daskms import xds_from_ms
from daskms.dataset import Dataset

def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument("ms")
    p.add_argument("-t", "--time-bin", default=2.0, type=float)
    p.add_argument("-c", "--chan-bin", default=1, type=int)
    return p


def average(ms, time_bin, chan_bin):
    datasets = xds_from_ms(ms)
    out_datasets = []

    for ds in datasets:
        avg = time_and_channel(ds.TIME.data,
                               ds.INTERVAL.data,
                               ds.ANTENNA1.data,
                               ds.ANTENNA2.data,
                               visibilities=ds.DATA.data,
                               time_bin_secs=time_bin,
                               chan_bin_size=chan_bin)

        new_ds = Dataset({
            "TIME": (("row",), avg.time),
            "INTERVAL": (("row",), avg.interval),
            "ANTENNA1": (("row",), avg.antenna1),
            "ANTENNA2": (("row",), avg.antenna2),
            "DATA": (("row", "chan", "corr"), avg.visibilities),
        })

        out_datasets.append(new_ds)

    return out_datasets


from dask.array import reduction


def sum_at_unique(ant1, ant2, data):
    ubl, bl_inv, bl_count = np.unique(np.stack([ant1, ant2], axis=1),
                                      axis=0,
                                      return_inverse=True,
                                      return_counts=True)
    nbl = ubl.shape[0]
    nchan, ncorr = data.shape[1:]

    data_sum = np.zeros((nbl, nchan, ncorr), dtype=data.dtype)
    data_counts = np.zeros(data_sum.shape, dtype=np.intp)

    np.add.at(data_sum, bl_inv, data)
    np.add.at(data_counts, bl_inv, 1)

    print("Chunk shape %s" % (data_sum.shape,))

    return (ubl, bl_count), (data_sum, data_counts)


def _noop(x, axis, keepdims):
    """ Don't transform data on initial step """
    return x


def _data_sum_combine(x, axis, keepdims):
    """
    da.reduction can either supply list[value] or value when concatenate=False
    value in this case is the tuple of length 3
    """
    if isinstance(x, list):
        # [(bl_meta1, data_meta1), (bl_meta2, data_meta2), ..., ]

        # Transpose to get lists of metadata
        bl_meta, data_meta = zip(*x)

        # [(0, 0), (0, 1), (0, 2), (0, 0), (0, 1)]
        # [4     , 2     , 1     , 3     ,      2]

        # Transpose again to lists of baselines and baseline counts
        bl, bl_counts = (np.concatenate(a) for a in zip(*bl_meta))
        ubl, bl_inv = np.unique(bl, axis=0, return_inverse=True)
        new_bl_counts = np.zeros(ubl.shape[0], bl_counts[0].dtype)
        np.add.at(new_bl_counts, bl_inv, bl_counts)

        # [(0, 0), (0, 1), (0, 2)]
        # [7,    ,      4,      1]

        # Determine output dimensions
        nbl = ubl.shape[0]
        first_data = data_meta[0][0]
        nchan, ncorr = first_data.shape[2:]

        # Output data and count arrays
        new_data = np.zeros((nbl, nchan, ncorr), dtype=first_data.dtype)
        new_counts = np.zeros(new_data.shape, dtype=np.intp)

        print("Combining shape %s" % (new_data.shape,))

        bl_start = 0
        it = zip(bl_meta, data_meta)

        # Iterate over times, baselines and data to aggregate
        for (bl, _), (data, data_counts) in it:
            bl_end = bl_start + bl.shape[0]

            # Each iteration is associated with a particular range
            # in the time and baseline inverses
            idx = bl_inv[bl_start:bl_end]

            # Accumulate data associated with this iteration into
            # at the appropriate locations
            np.add.at(new_data, idx, data)
            np.add.at(new_counts, idx, data_counts)

            bl_start = bl_end

        return ((ubl, new_bl_counts),
                (new_data, new_counts))

    elif isinstance(x, tuple):
        return x
    else:
        raise TypeError("Unhandled x type %s" % type(x))


def _data_sum_agg(x, axis, keepdims):
    (_, _), (data, counts) = _data_sum_combine(x, axis, keepdims)
    return data / counts


def avg_reduction(ms):
    datasets = xds_from_ms(ms, chunks={"row": 100})

    for ds in datasets:
        # For each chunk, calculate the unique baselines,
        # the inverse index at which they occurs and their counts
        # represent this as a dask array of objects (they're tuples)
        data_sum = da.blockwise(sum_at_unique, ("row", "chan", "corr"),
                                ds.ANTENNA1.data, ("row",),
                                ds.ANTENNA1.data, ("row",),
                                ds.DATA.data, ("row", "chan", "corr"),
                                meta=np.empty((0, 0, 0), dtype=object))

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

        return reduction

if __name__ == "__main__":
    args = create_parser().parse_args()
    result = average(args.ms, args.time_bin, args.chan_bin)

    result = avg_reduction(args.ms)
    result.visualize("graph.pdf")
    data = result.compute()
    print(data.shape)
    None