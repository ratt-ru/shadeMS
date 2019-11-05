#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import daskms as xms
import time
import sys
import pylab
import numpy

def get_chan_freqs(myms):
    spw_tab = xms.xds_from_table(
        myms+'::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
    chan_freqs = spw_tab[0].CHAN_FREQ
    return chan_freqs


def get_field_names(myms):
    field_tab = xms.xds_from_table(
        myms+'::FIELD', columns=['NAME', 'SOURCE_ID'])
    field_ids = field_tab[0].SOURCE_ID.values
    field_names = field_tab[0].NAME.values
    return field_ids, field_names


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff


def make_plot(data, xmin, xmax, ymin, ymax, xlabel, ylabel, title, pngname, bgcol, fontsize, figx=24, figy=12):

    def match(artist):
        return artist.__module__ == 'matplotlib.text'

    fig = pylab.figure(figsize=(figx, figy))
    ax = fig.add_subplot(111, facecolor=bgcol)
    ax.imshow(X=data, extent=[xmin, xmax, ymin, ymax],
              aspect='auto', origin='upper')
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(fontsize)
    fig.savefig(pngname, bbox_inches='tight')
    return pngname


def now():
    # stamp = time.strftime('[%Y-%m-%d %H:%M:%S]: ')
    # msg = '\033[92m'+stamp+'\033[0m' # time in green
    stamp = time.strftime(' [%H:%M:%S] ')
    msg = stamp+' '
    return msg


def stamp():
    return str(time.time()).replace('.', '')


def blank():
    print(('%s' % now()))


def fullname(shortname):
    fullnames = [('a', 'Amplitude', ''),
                 ('p', 'Phase', '[rad]'),
                 ('r', 'Real', ''),
                 ('i', 'Imaginary', ''),
                 ('t', 'Time', '[s]'),
                 ('c', 'Channel', ''),
                 ('f', 'Frequency', '[Hz]'),
                 ('uv', 'uv-distance', '[wavelengths]'),
                 ('u', 'u', '[wavelengths]'),
                 ('v', 'v', '[wavelengths]')]
    for xx in fullnames:
        if xx[0] == shortname:
            fullname = xx[1]
            print(xx)
            units = xx[2]
    return fullname, units
