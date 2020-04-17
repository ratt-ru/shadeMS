from MSUtils.msutils import STOKES_TYPES
from casacore.tables import table
import re

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
        return name in self.map

    def __getitem__(self, item, default=None):
        return self.map.get(item, default) if type[item] is str else self.names[item]

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

    def __init__(self, msname, log=None):
        log and log.info(f": MS is {msname}")
        self.log = log

        tab = table(msname, ack=False)

        self.valid_columns = set(tab.colnames())

        spw_tab = table(msname + '::SPECTRAL_WINDOW', ack=False)
        self.nspw = spw_tab.nrows()
        self.chan_freqs = spw_tab.getcol("CHAN_FREQ")
        self.spws = NamedList("spw", list(map(str, range(self.nspw))))

        log and log.info(f":   {self.chan_freqs.shape} spectral windows and channels")

        self.field = NamedList("field", table(msname +'::FIELD', ack=False).getcol("NAME"))
        log and log.info(f":   {len(self.field)} fields: {' '.join(self.field.names)}")

        scan_numbers = list(set(tab.getcol("SCAN_NUMBER")))
        log and log.info(f":   {len(scan_numbers)} scans, first #{scan_numbers[0]}, last #{scan_numbers[-1]}")
        all_scans = NamedList("scan", list(map(str, range(scan_numbers[-1]+1))))
        self.scan = all_scans.get_subset(scan_numbers)

        all_antennas = NamedList("antenna", table(msname +'::ANTENNA', ack=False).getcol("NAME"))

        self.antennas = all_antennas.get_subset(list(set(tab.getcol("ANTENNA1"))|set(tab.getcol("ANTENNA2"))))

        log and log.info(f":   {len(self.antennas)} antennas: {self.antennas.str_list()}")

        pol_tab = table(msname + '::POLARIZATION', ack=False)

        corr_labels = [STOKES_TYPES[icorr] for icorr in pol_tab.getcol("CORR_TYPE", 0, 1).ravel()]
        self.corr = NamedList("correlation", corr_labels)
        log and log.info(f":   correlations {' '.join(self.corr.names)}")

