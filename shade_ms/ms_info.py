from casacore.tables import table
import re
import daskms
import math
import numpy as np
from collections import OrderedDict

STOKES_TYPES = {
    0  : "Undefined",
    1  : "I",
    2  : "Q",
    3  : "U",
    4  : "V",
    5  : "RR",
    6  : "RL",
    7  : "LR",
    8  : "LL",
    9  : "XX",
    10 : "XY",
    11 : "YX",
    12 : "YY",
    13 : "RX",
    14 : "RY",
    15 : "LX",
    16 : "LY",
    17 : "XR",
    18 : "XL",
    19 : "YR",
    20 : "YL",
    21 : "PP",
    22 : "PQ",
    23 : "QP",
    24 : "QQ",
    25 : "RCircular",
    26 : "LCircular",
    27 : "Linear",
    28 : "Ptotal",
    29 : "Plinear",
    30 : "PFtotal",
    31 : "PFlinear",
    32 : "Pangle",
}

class NamedList(object):
    """Holds a list of names (e.g. field names), and provides common indexing and subset operations"""

    def __init__(self, label, names, numbers=None):
        self.label = label
        self.names = names
        self.numbers = numbers or range(len(self.names))
        self.map = dict(zip(names, self.numbers))
        self.numindex = {num: i for i, num in enumerate(self.numbers)}

    def __len__(self):
        return len(self.names)

    def __contains__(self, name):
        return name in self.map if type(name) is str else name in self.numbers

    def __getitem__(self, item, default=None):
        if type(item) is str:
            return self.map.get(item, default)
        elif type(item) is slice:
            return self.names[item]
        else:
            return self.names[item] # self.numindex[item]]

    def get_subset(self, subset, allow_numeric_indices=True):
        """Extracts subset using a comma-separated string or list of indices"""
        if type(subset) in (list, tuple):
            return NamedList(self.label, [self.names[x] for x in subset], [self.numbers[x] for x in subset])
        elif type(subset) is str:
            if subset == "all":
                return self
            ind = []
            for x in subset.split(","):
                if allow_numeric_indices and re.fullmatch('\d+', x):
                    x = int(x)
                    if x not in self.numindex:
                        raise ValueError(f"invalid {self.label} number {x}")
                    ind.append(self.numindex[x])
                elif x in self.map:
                    ind.append(self.numindex[self.map[x]])
                else:
                    raise ValueError(f"invalid {self.label} '{x}'")
            return NamedList(self.label, [self.names[x] for x in ind], [self.numbers[x] for x in ind])
        else:
            raise TypeError(f"unknown subset of type {type(subset)}")

    def str_list(self):
        return " ".join([f"{i}:{name}" for i, name in zip(self.numbers, self.names)])

class MSInfo(object):
    """Holds information about the MS structure"""

    def __init__(self, msname=None, log=None):
        if not msname:
            return

        self.msname = msname
        self.log = log

        tab = table(msname, ack=False)
        log and log.info(f": MS {msname} contains {tab.nrows()} rows")

        self.valid_columns = set(tab.colnames())

        # indexing columns are read into memory in their entirety up front
        self.indexing_columns = dict()

        spw_tab = daskms.xds_from_table(msname + '::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
        self.chan_freqs = spw_tab[0].CHAN_FREQ   # important for this to be an xarray
        self.nspw = self.chan_freqs.shape[0]
        self.spw = NamedList("spw", list(map(str, range(self.nspw))))

        log and log.info(f":   {self.chan_freqs.shape} spectral windows and channels")

        self.field = NamedList("field", table(msname +'::FIELD', ack=False).getcol("NAME"))
        log and log.info(f":   {len(self.field)} fields: {' '.join(self.field.names)}")

        self.indexing_columns["SCAN_NUMBER"] = tab.getcol("SCAN_NUMBER")
        scan_numbers = sorted(set(self.indexing_columns["SCAN_NUMBER"]))
        log and log.info(f":   {len(scan_numbers)} scans: {' '.join(map(str, scan_numbers))}")
        all_scans = NamedList("scan", list(map(str, range(scan_numbers[-1]+1))))
        self.scan = all_scans.get_subset(scan_numbers)

        anttab = table(msname +'::ANTENNA', ack=False)
        antnames = anttab.getcol("NAME")
        self.all_antenna = antnames = NamedList("antenna", antnames)
        self.antpos = anttab.getcol("POSITION")

        ant1col = self.indexing_columns["ANTENNA1"] = tab.getcol("ANTENNA1")
        ant2col = self.indexing_columns["ANTENNA2"] = tab.getcol("ANTENNA2")
        self.antenna = self.all_antenna.get_subset(list(set(ant1col)|set(ant2col)))

        log and log.info(f":   {len(self.antenna)}/{len(self.all_antenna)} antennas: {self.antenna.str_list()}")

        # list of all possible baselines
        nant = len(antnames)
        blnames = [f"{a1}-{a2}" for i1, a1 in enumerate(antnames) for a2 in antnames[i1:]]
        self.all_baseline = NamedList("baseline", blnames)

        # list of baseline lengths
        self.baseline_lengths = [math.sqrt(((pos1-pos2)**2).sum())
                                 for i1, pos1 in enumerate(self.antpos) for pos2 in self.antpos[i1:]]

        # sort order to put baselines by length
        sorted_by_length = sorted([(x, i) for i, x in enumerate(self.baseline_lengths)])
        self.baseline_sorted_index  = [i for _, i in sorted_by_length]
        self.baseline_sorted_length = [x for x, _ in sorted_by_length]

        # baselines actually present
        a1 = np.minimum(ant1col, ant2col)
        a2 = np.maximum(ant1col, ant2col)
        bl_set = set(self.baseline_number(a1, a2))
        bls = sorted(bl_set)
        self.baseline = NamedList("baseline", [blnames[b] for b in bls], bls)

        # make list of baselines present, in meters
        blm = [i for i in self.baseline_sorted_index if i in bl_set]
        self.baseline_m = NamedList("baseline_m", [f"{int(round(self.baseline_lengths[b]))}m" for b in blm], blm)

        log and log.info(f":   {len(self.baseline)}/{len(self.all_baseline)} baselines present")

        pol_tab = table(msname + '::POLARIZATION', ack=False)

        all_corr_labels = [STOKES_TYPES[icorr] for icorr in pol_tab.getcol("CORR_TYPE", 0, 1).ravel()]
        self.corr = NamedList("correlation", all_corr_labels.copy())

        # Maps correlation -> callable that extracts that correlation from visibility data
        # By default, populated with slicing functions for 0...3,
        # but can also be extended with "I", "Q", etx.
        self.corr_data_mappers = OrderedDict({i: lambda x,icorr=i:x[...,icorr] for i in range(len(all_corr_labels))})

        # Maps correlation -> callable that extracts that correlation from flag data
        self.corr_flag_mappers = self.corr_data_mappers.copy()

        # add mappings and labels for Stokes parameters
        xx, xy, yx, yy = [self.corr.map.get(c) for c in ("XX", "XY", "YX", "YY")]
        rr, rl, lr, ll = [self.corr.map.get(c) for c in ("RR", "RL", "LR", "LL")]

        def add_stokes(a, b, I, J, imag=False):
            """Adds mappers for Stokes A and B as the sum/difference of components I and J, divided by 2 or 2j"""
            def _sum(x):
                return (x[..., I] + x[..., J]) / 2
            def _diff(x):
                return (x[..., I] - x[..., J]) / (2j if imag else 2)
            def _or(x):
                return (x[..., I] | x[..., J])
            nonlocal all_corr_labels
            if a not in self.corr_data_mappers:
                self.corr_data_mappers[len(all_corr_labels)] = _sum
                self.corr_flag_mappers[len(all_corr_labels)] = _or
                all_corr_labels.append(a)
            if b not in self.corr_data_mappers:
                self.corr_data_mappers[len(all_corr_labels)] = _diff
                self.corr_flag_mappers[len(all_corr_labels)] = _or
                all_corr_labels.append(b)

        if xx is not None and yy is not None:
            add_stokes("I", "Q", xx, yy)
        if rr is not None and ll is not None:
            add_stokes("I", "V", rr, ll)
        if xy is not None and yx is not None:
            add_stokes("U", "V", xy, yx, True)
        if rl is not None and lr is not None:
            add_stokes("Q", "U", rl, lr, True)

        self.all_corr = NamedList("correlation", all_corr_labels)

        log and log.info(f":   corrs/Stokes {' '.join(self.all_corr.names)}")

    def baseline_number(self, a1, a2):
        """Returns baseline number, for a1<=a2"""
        return a1 * len(self.all_antenna) - a1 * (a1 - 1) // 2 + a2 - a1

