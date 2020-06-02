from functools import reduce
from itertools import product
from operator import mul

import dask.array as da
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
import dask.dataframe as dd
import pandas as pd
import numpy as np


def start_ends(chunks):
    s = 0

    for c in chunks:
        e = s + c
        yield (s, e)
        s = e


def _create_dataframe(arrays, start, end, columns):
    index = None if start is None else np.arange(start, end)

    return pd.DataFrame({k: a.ravel() for k, a in zip(columns, arrays)},
                        index=index)

def dataframe_factory(out_ind, *arginds, columns=None):
    """
    Creates a dask Dataframe by broadcasting  *arginds
    against each other and then ravelling them.

    .. code-block:: python

        df = dataframe_factory(("row", "chan"),
                               x, ("row",),
                               y, ("chan",))

    Parameters
    ----------
    out_ind : sequence
        Output dimensions.
        e.g. :code:`(row, chan)`
    *arginds : Sequence of (:class:`dask.array.Array`, index)
        document me
    columns : sequence, optional
        Dataframe column names.
        Defaults to :code:`[x, y]`
    """
    if not len(arginds) % 2 == 0:
        raise ValueError("Must supply an index for each argument")

    args = arginds[::2]
    inds = arginds[1::2]

    if columns is None:
        columns = ['x', 'y'] + ["c%d" % i for i in range(len(args) - 2)]
    else:
        if (not isinstance(columns, (tuple, list)) and
            len(columns) != len(args)):

            raise ValueError("Columns must be a tuple/list of columns "
                             "matching the number of arrays")

    have_nan_chunks = False

    new_args = []

    for a, (arg, ind) in enumerate(zip(args, inds)):
        if not all(i in out_ind for i in ind):
            raise ValueError("Argument %d dimensions not in out_ind" % a)

        if not len(ind) == arg.ndim:
            raise ValueError("Argument %d len(ind) != arg.ndim" % a)

        have_nan_chunks = (any(np.isnan(c) for dc in arg.chunks for c in dc) or
                           have_nan_chunks)

        # Generate slicing tuple that will expand arg up to full resolution
        expand = tuple(slice(None) if i in ind else None for i in out_ind)
        new_args.append(arg[expand])

    # Create meta data so that blockwise doesn't call
    # np.broadcast_arrays and fall over on the tuple
    # of arrays that it returns
    dtype = np.result_type(*args)
    meta = np.empty((0,) * len(out_ind), dtype=dtype)

    blockargs = (v for pair in ((a, out_ind) for a in new_args) for v in pair)

    bcast = da.blockwise(np.broadcast_arrays, out_ind,
                         *blockargs,
                         subok=True,
                         align_arrays=not have_nan_chunks,
                         meta=meta,
                         dtype=dtype)

    # Now create a dataframe from the broadcasted arrays
    # with lower-level dask graph API

    # Flattened list of broadcast array keys
    # We'll use this to generate a 1D (ravelled) dataframe
    keys = product((bcast.name,), *(range(b) for b in bcast.numblocks))
    name = "dataframe-" + tokenize(bcast)

    # dictionary defining the graph for this part of the operation
    layers = {}

    if have_nan_chunks:
        # We can't create proper indices if we don't known our chunk sizes
        divisions = [None]

        for i, key in enumerate(keys):
            layers[(name, i)] = (_create_dataframe, key, None, None, columns)
            divisions.append(None)
    else:
        # We do know all our chunk sizes, create reasonable dataframe indices
        start_idx = 0
        divisions = [0]

        expr = ((e - s for s, e in start_ends(dim_chunks))
                for dim_chunks in bcast.chunks)
        chunk_sizes = (reduce(mul, shape, 1) for shape in product(*expr))
        chunk_ranges = start_ends(chunk_sizes)

        for i, (key, (start, end)) in enumerate(zip(keys, chunk_ranges)):
            layers[(name, i)] = (_create_dataframe, key, start, end, columns)
            start_idx += end - start
            divisions.append(start_idx)

    assert len(layers) == bcast.npartitions
    assert len(divisions) == bcast.npartitions + 1

    # Create the HighLevelGraph
    graph = HighLevelGraph.from_collections(name, layers, [bcast])
    # Metadata representing the broadcasted and ravelled data

    meta = pd.DataFrame(data={k: np.empty((0,), dtype=a.dtype)
                              for k, a in zip(columns, args)},
                        columns=columns)

    # Create the actual Dataframe
    return dd.DataFrame(graph, name, meta=meta, divisions=divisions)


def multicol_dataframe_factory(out_ind, arrays, array_dims):
    """
    Creates a dask Dataframe by broadcasting arrays (given by the arrays dict-like object)
    against each other and then ravelling them. The array_indices mapping specifies which indices
    the arrays have

    .. code-block:: python

        df = dataframe_factory(("row", "chan"), {'x': x, 'y': y}, {x: ("row",), y: ("chan",)})

    Parameters
    ----------
    out_ind : sequence
        Output dimensions.
        e.g. :code:`(row, chan)`
    """
    columns = list(arrays.keys())

    have_nan_chunks = None
    expand = {}
    barr = {}
    # build up list of arguments for blockwise call below
    blockwise_args = [np.broadcast_arrays, out_ind]

    for col, arr in arrays.items():
        if col not in array_dims:
            raise ValueError(f"{col} dimensions not specified")
        arr_ind = array_dims[col]
        if not all(i in out_ind for i in arr_ind):
            raise ValueError(f"{col} dimensions not in out_ind")
        if not len(arr_ind) == arr.ndim:
            raise ValueError(f"len({col}_ind) != {col}.ndim")
        have_nan_chunks = have_nan_chunks or any(np.isnan(c) for dc in arr.chunks for c in dc)

        # Generate slicing tuples that will expand arr up to the full
        # resolution
        expand[col] = tuple(slice(None) if i in arr_ind else None for i in out_ind)
        # broadcast vesion of array
        barr[col] = arr[expand[col]]

        blockwise_args += [barr[col], out_ind]

    # Create meta data so that blockwise doesn't call
    # np.broadcast_arrays and fall over on the tuple
    # of arrays that it returns
    dtype = np.result_type(*arrays.values())
    meta = np.empty((0,) * len(out_ind), dtype=dtype)

    bcast = da.blockwise(*blockwise_args,
                         align_arrays=not have_nan_chunks,
                         meta=meta,
                         dtype=dtype)

    # Now create a dataframe from the broadcasted arrays
    # with lower-level dask graph API

    # Flattened list of broadcast array keys
    # We'll use this to generate a 1D (ravelled) dataframe
    keys = product((bcast.name,), *(range(b) for b in bcast.numblocks))
    name = "dataframe-" + tokenize(bcast)

    # dictionary defining the graph for this part of the operation
    layers = {}

    if have_nan_chunks:
        # We can't create proper indices if we don't known our chunk sizes
        divisions = [None]

        for i, key in enumerate(keys):
            layers[(name, i)] = (_create_dataframe, key, None, None)
            divisions.append(None)
    else:
        # We do know all our chunk sizes, create reasonable dataframe indices
        start_idx = 0
        divisions = [0]

        expr = ((e - s for s, e in start_ends(dim_chunks))
                for dim_chunks in bcast.chunks)
        chunk_sizes = (reduce(mul, shape, 1) for shape in product(*expr))
        chunk_ranges = start_ends(chunk_sizes)

        for i, (key, (start, end)) in enumerate(zip(keys, chunk_ranges)):
            layers[(name, i)] = (_create_dataframe, key, start, end)
            start_idx += end - start
            divisions.append(start_idx)

    assert len(layers) == bcast.npartitions
    assert len(divisions) == bcast.npartitions + 1

    # Create the HighLevelGraph
    graph = HighLevelGraph.from_collections(name, layers, [bcast])
    # Metadata representing the broadcasted and ravelled data
    meta = pd.DataFrame(data={col: np.empty((0,), dtype=arr.dtype) for col, arr in arrays.items()},
                        columns=columns)

    # Create the actual Dataframe
    return dd.DataFrame(graph, name, meta=meta, divisions=divisions)
