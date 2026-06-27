import numpy as np
from numpy.testing import assert_array_equal

from shade_ms.data_plots import _bin_mean_freqs


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
