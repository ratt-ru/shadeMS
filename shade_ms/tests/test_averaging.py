import dask.array as da
import numpy as np
import pytest
import xarray
from numpy.testing import assert_array_equal

from shade_ms.data_plots import _bin_mean_freqs, _pad_range, average_group
from shade_ms.main import parse_average_spec

NTIME, NBL, NCHAN, NCORR, DT, CHAN_WIDTH = 10, 3, 8, 2, 10.0, 1e6


@pytest.fixture(scope="module")
def group():
    """A synthetic MS group: NBL baselines over NTIME timeslots of DT seconds, NCHAN channels.

    FLAG_ROW deliberately disagrees with FLAG (row 0 flagged in one but not the other), as
    real MSs often do -- africanus rejects that combination if it is passed through as-is.
    """
    nrow = NTIME * NBL
    times = np.repeat(1e9 + DT * np.arange(NTIME), NBL)
    flag_row = np.zeros(nrow, bool)
    flag_row[0] = True
    arrays = dict(
        TIME=(("row",), times),
        INTERVAL=(("row",), np.full(nrow, DT)),
        ANTENNA1=(("row",), np.tile(np.array([0, 0, 1], np.int32), NTIME)),
        ANTENNA2=(("row",), np.tile(np.array([1, 2, 2], np.int32), NTIME)),
        DATA=(("row", "chan", "corr"), np.ones((nrow, NCHAN, NCORR), np.complex64)),
        FLAG=(("row", "chan", "corr"), np.zeros((nrow, NCHAN, NCORR), bool)),
        FLAG_ROW=(("row",), flag_row),
    )
    return xarray.Dataset({name: (dims, da.from_array(a, chunks=a.shape))
                           for name, (dims, a) in arrays.items()},
                          coords=dict(row=np.arange(nrow), chan=np.arange(NCHAN),
                                      corr=np.arange(NCORR)))


@pytest.mark.parametrize("avg_spec, nrow, nchan", [
    ({}, NTIME * NBL, NCHAN),                              # no averaging
    ({"TIME": ("count", 5, "5")}, 2 * NBL, NCHAN),         # 5 of 10 timeslots per bin
    ({"TIME": ("quantity", 50.0, "50s")}, 2 * NBL, NCHAN),  # ... and the same as a quantity
    ({"TIME": ("all", None, "all")}, NBL, NCHAN),
    ({"CHAN": ("count", 2, "2")}, NTIME * NBL, NCHAN // 2),
    ({"CHAN": ("quantity", 2e6, "2MHz")}, NTIME * NBL, NCHAN // 2),   # 2 channels wide
    ({"CHAN": ("quantity", 4e6, "4MHz")}, NTIME * NBL, NCHAN // 4),   # 4 channels wide
    ({"CHAN": ("all", None, "all")}, NTIME * NBL, 1),
])
def test_average_group_bin_sizes(group, avg_spec, nrow, nchan):
    freqs = 1e9 + CHAN_WIDTH * np.arange(NCHAN)
    avg, avg_freqs = average_group(group, freqs, ["DATA"], avg_spec, slice(None), True, 100000)
    assert len(avg.row) == nrow
    assert len(avg.chan) == nchan
    assert len(avg_freqs) == nchan


@pytest.mark.parametrize("vis_columns", [["DATA"], []])
def test_average_group_flags(group, vis_columns):
    """With use_flags, the averaged group carries flags; with --noflags it carries none.

    Averaging with no visibility columns at all (e.g. -x TIME -y uv) is the corner africanus
    cannot handle unaided, so exercise it both ways.
    """
    freqs = 1e9 + CHAN_WIDTH * np.arange(NCHAN)
    avg_spec = {"TIME": ("count", 5, "5")}
    for use_flags, want_flags in ((True, True), (False, False)):
        avg, _ = average_group(group, freqs, vis_columns, avg_spec, slice(None), use_flags, 100000)
        assert ("FLAG" in avg) is want_flags
        assert ("FLAG_ROW" in avg) is want_flags
        assert len(avg.row) == 2 * NBL
        assert len(avg.chan) == NCHAN
        assert len(avg.corr) == NCORR


def test_average_group_flagged_samples(group):
    """Flagged samples stay out of a bin that has unflagged data in it -- unless --noflags."""
    nrow, half = NTIME * NBL, (NTIME // 2) * NBL
    flag = np.zeros((nrow, NCHAN, NCORR), bool)
    data = np.ones((nrow, NCHAN, NCORR), np.complex64)
    flag[:half] = True    # flag the first half of the timeslots, with a value to tell them apart
    data[:half] = 5.0
    group = group.assign(FLAG=(("row", "chan", "corr"), da.from_array(flag, chunks=flag.shape)),
                         DATA=(("row", "chan", "corr"), da.from_array(data, chunks=data.shape)))
    freqs = 1e9 + CHAN_WIDTH * np.arange(NCHAN)
    avg_spec = {"TIME": ("all", None, "all")}   # one bin per baseline, mixing both halves

    avg, _ = average_group(group, freqs, ["DATA"], avg_spec, slice(None), True, 100000)
    assert_array_equal(np.unique(avg.DATA.data.compute().real), [1.0])
    assert not avg.FLAG.data.compute().any()   # every bin has unflagged data in it

    avg, _ = average_group(group, freqs, ["DATA"], avg_spec, slice(None), False, 100000)
    assert_array_equal(np.unique(avg.DATA.data.compute().real), [3.0])   # (5 + 1) / 2


@pytest.mark.parametrize("rng, expected", [
    ((0.0, 1.0), (0.0, 1.0)),      # a usable range is left alone
    ((3.0, 3.0), (2.0, 4.0)),      # zero width (e.g. a single averaged channel) is widened
    ((1.0, 0.0), (0.0, 2.0)),      # ... and so is an inverted one
])
def test_pad_range(rng, expected):
    assert _pad_range(*rng) == expected


def test_bin_mean_freqs_identity():
    freqs = np.array([0., 1., 2., 3.])
    # bin size of 1 (or less) leaves the grid untouched
    assert_array_equal(_bin_mean_freqs(freqs, 1), freqs)


def test_bin_mean_freqs_exact():
    freqs = np.array([0., 1., 2., 3.])
    assert_array_equal(_bin_mean_freqs(freqs, 2), np.array([0.5, 2.5]))


def test_bin_mean_freqs_partial_last_bin():
    freqs = np.array([0., 1., 2., 3., 4.])
    # last (partial) bin averages just the channels it contains
    assert_array_equal(_bin_mean_freqs(freqs, 2), np.array([0.5, 2.5, 4.0]))


def test_bin_mean_freqs_full_collapse():
    freqs = np.array([0., 1., 2., 3., 4.])
    assert_array_equal(_bin_mean_freqs(freqs, 5), np.array([2.0]))
    assert_array_equal(_bin_mean_freqs(freqs, 100), np.array([2.0]))


@pytest.mark.parametrize("spec, expected", [
    ("TIME:60", ("count", 60)),
    ("CHAN:4", ("count", 4)),
    ("TIME:60s", ("quantity", 60.0)),
    ("TIME:2min", ("quantity", 120.0)),
    ("CHAN:8MHz", ("quantity", 8e6)),
    ("CHAN:0.5GHz", ("quantity", 5e8)),
    ("TIME:all", ("all", None)),
    ("chan:ALL", ("all", None)),
])
def test_parse_average_spec(spec, expected):
    axis = spec.split(":")[0].upper()
    kind, value, _ = parse_average_spec([spec])[axis]
    assert kind == expected[0]
    assert value == pytest.approx(expected[1])


@pytest.mark.parametrize("specs", [
    ["TIME"],                    # no bin size
    ["TIME:"],                   # empty bin size
    ["BANANA:4"],                # unknown axis
    ["BASELINE:4"],              # not implemented yet
    ["TIME:60", "TIME:30"],      # repeated axis
    ["TIME:banana"],             # unparseable
    ["TIME:8MHz"],               # wrong dimensionality
    ["CHAN:60s"],                # wrong dimensionality
    ["CHAN:0"],                  # not positive
    ["TIME:-1s"],                # not positive
])
def test_parse_average_spec_errors(specs):
    with pytest.raises(ValueError):
        parse_average_spec(specs)
