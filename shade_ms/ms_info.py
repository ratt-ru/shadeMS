from MSUtils.msutils import STOKES_TYPES
from casacore.tables import table
import re
import daskms
import numpy
from collections import OrderedDict
from xarray import DataArray

class NamedList(object):
    """Holds a list of names (e.g. field names), and provides common indexing and subset operations"""

    def __init__(self, label, names, numbers=None):
        self.label = label
        self.names = names
        self.numbers = numbers or range(len(self.names))
        self.map = dict(zip(names, self.numbers))

    def __len__(self):
        return len(self.names)

    def __contains__(self, name):
        return name in self.map if type(name) is str else name in self.numbers

    def __getitem__(self, item, default=None):
        return self.map.get(item, default) if type(item) is str else self.names[item]

    def get_subset(self, subset):
        """Extracts subset using a comma-separated string or list of indices"""
        if type(subset) in (list, tuple):
            return NamedList(self.label, [self.names[x] for x in subset], subset)
        elif type(subset) is str:
            if subset == "all":
                return self
            numbers = []
            for x in subset.split(","):
                if re.fullmatch('\d+', x):
                    x = int(x)
                    if x < 0 or x >= len(self):
                        raise ValueError(f"invalid {self.label} number {x}")
                    numbers.append(x)
                elif x in self.map:
                    numbers.append(self.map[x])
                else:
                    raise ValueError(f"invalid {self.label} '{x}'")
            return NamedList(self.label, [self.names[x] for x in numbers], numbers)
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

        spw_tab = daskms.xds_from_table(msname + '::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
        self.chan_freqs = spw_tab[0].CHAN_FREQ   # important for this to be an xarray
        self.nspw = self.chan_freqs.shape[0]
        self.spw = NamedList("spw", list(map(str, range(self.nspw))))

        log and log.info(f":   {self.chan_freqs.shape} spectral windows and channels")

        self.field = NamedList("field", table(msname +'::FIELD', ack=False).getcol("NAME"))
        log and log.info(f":   {len(self.field)} fields: {' '.join(self.field.names)}")

        scan_numbers = list(set(tab.getcol("SCAN_NUMBER")))
        log and log.info(f":   {len(scan_numbers)} scans, first #{scan_numbers[0]}, last #{scan_numbers[-1]}")
        all_scans = NamedList("scan", list(map(str, range(scan_numbers[-1]+1))))
        self.scan = all_scans.get_subset(scan_numbers)

        self.all_antenna = NamedList("antenna", table(msname +'::ANTENNA', ack=False).getcol("NAME"))

        self.antenna = self.all_antenna.get_subset(list(set(tab.getcol("ANTENNA1"))|set(tab.getcol("ANTENNA2"))))

        baselines = [(p,q) for p in self.antenna.numbers for q in self.antenna.numbers if p <= q]
        self.baseline_numbering = { (p, q): i for i, (p, q) in enumerate(baselines)}
        self.baseline_numbering.update({ (q, p): i for i, (p, q) in enumerate(baselines)})

        log and log.info(f":   {len(self.antenna)} antennas: {self.antenna.str_list()}")

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

    def baseline_number(self, ant1, ant2):
        a1 = DataArray.minimum(ant1, ant2)
        a2 = DataArray.maximum(ant1, ant2)
