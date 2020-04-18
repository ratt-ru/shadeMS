# -*- coding: future_fstrings -*-

# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import colorcet
import daskms
import dask.array as da
import dask.array.ma as dama
import dask.dataframe as dask_df
import datetime
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
import math
import pandas as pd
import pylab
import ShadeMS
import sys
import time
import os.path
import re
from collections import OrderedDict

from MSUtils.msutils import STOKES_TYPES

log = ShadeMS.log

def get_chan_freqs(myms):
    spw_tab = daskms.xds_from_table(
        myms+'::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
    chan_freqs = spw_tab[0].CHAN_FREQ
    return chan_freqs


def get_field_names(myms):
    field_tab = daskms.xds_from_table(
        myms+'::FIELD', columns=['NAME','SOURCE_ID'])
    field_ids = field_tab[0].SOURCE_ID.values
    field_names = field_tab[0].NAME.values
    return field_ids, field_names


def get_scan_numbers(myms):
    tab = daskms.xds_from_table(
        myms, columns=['SCAN_NUMBER'])
    scan_numbers = numpy.unique(tab[0].SCAN_NUMBER.values)
    return scan_numbers.tolist()


def get_antennas(myms):
    tab = daskms.xds_from_table(
        myms, columns=['ANTENNA1','ANTENNA2'])
    ant1 = numpy.unique(tab[0].ANTENNA1.values)
    ant2 = numpy.unique(tab[0].ANTENNA2.values)
    ants = numpy.unique(numpy.concatenate((ant1,ant2)))
    # ant_tab = xmd.xds_from_table(
    #     myms+'::ANTENNA', columns=['NAME','STATION'])
    # names = ant_tab[0].NAME.values
    # stations = ant_tab[0].STATION.values
    return ants.tolist()

def get_correlations(myms):
    pol_tab = daskms.xds_from_table(
        myms+'::POLARIZATION', columns=['CORR_TYPE'])
    return [STOKES_TYPES[icorr] for icorr in pol_tab[0].CORR_TYPE.values[0]]


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff


def now():
    # stamp = time.strftime('[%Y-%m-%d %H:%M:%S]: ')
    # msg = '\033[92m'+stamp+'\033[0m' # time in green
    stamp = time.strftime(' [%H:%M:%S] ')
    msg = stamp+' '
    return msg


def blank():
    log.info('------------------------------------------------------')


class DataMapper(object):
    """This class defines a mapping from a dask group to an array of real values to be plotted"""
    def __init__(self, fullname, unit, mapper=None, column=None, extras=None, conjugate=False, axis=None):
        """
        :param fullname:    full name of parameter (real, amplitude, etc.)
        :param unit:        unit string
        :param mapper:      function that maps column data to parameter
        :param column:      fix a column name (e.g. TIME, UVW. Not for visibility columns.)
        :param extras:      extra arguments needed by mapper (e.g. ["freqs", "wavel"])
        :param conjugate:   sets conjugation flag
        :param axis:        which axis the parameter represets (0 time, 1 freq), if 1-dimensional
        """
        self.fullname, self.unit, self.mapper, self.column, self.extras = fullname, unit, mapper, column, extras
        self.conjugate = conjugate
        self.axis = axis

    @staticmethod
    def get_column(group, colname):
        """
        Given a DASK group and a column name, returns corresponding dask array
        """
        if colname is None:
            return None
        elif hasattr(group, colname):
            return getattr(group, colname)
        elif colname == "D-M":
            return group.DATA - group.MODEL_DATA
        elif colname == "C-M":
            return group.CORRECTED_DATA - group.MODEL_DATA
        elif colname == "D/M":
            return group.DATA / group.MODEL_DATA
        elif colname == "C/M":
            return group.CORRECTED_DATA / group.MODEL_DATA
        else:
            raise ValueError(f"unknown column name {colname}")

    @staticmethod
    def get_involved_columns(colname):
        """Given a column name, returns list of columns involved"""
        if colname is None:
            return []
        elif colname in ("D-M", "D/M"):
            return ["DATA", "MODEL_DATA"]
        elif colname == "C-M":
            return ["CORRECTED_DATA", "MODEL_DATA"]
        else:
            return [colname]

    def map_value(self, group, colname, corr, extras, flag, flag_row, vmin, vmax):
        """
        """
        # preset column in constructor (UVBW and such) overrides dynamic column name (e.g. for vis columns)
        coldata = self.get_column(group, self.column or colname)
        if coldata.ndim == 3:
            coldata = coldata[..., corr]
        if self.extras:
            coldata = self.mapper(coldata, **{name:extras[name] for name in self.extras })
        elif self.mapper:
            coldata = self.mapper(coldata)
        # determine flags -- start with original flags
        if self.axis is None:
            flag = flag[..., corr]
        elif self.axis == 0:
            flag = flag_row
        elif self.axis == 1:
            flag = da.zeros_like(coldata, bool)
        # apply clipping
        if vmin is not None:
            flag = da.logical_or(flag, coldata<vmin)
        if vmax is not None:
            flag = da.logical_or(flag, coldata>vmax)
        # return masked array
        return dama.masked_array(coldata, flag)



#
# this dict maps short axis names into full DataMapper objects
mappers = OrderedDict(
    a=DataMapper("Amplitude", "", abs),
    p=DataMapper("Phase", "[deg]", lambda x:da.arctan2(da.imag(x), da.real(x))*180/math.pi),
    r=DataMapper("Real", "", da.real),
    i=DataMapper("Imag", "", da.imag),
    t=DataMapper("Time", "s", axis=0, column="TIME"),
    c=DataMapper("Channel", "", column=None, axis=1, extras=["chans"], mapper=lambda x,chans: chans),
    f=DataMapper("Frequency", "Hz", column=None, axis=1, extras=["freqs"], mapper=lambda x, freqs: freqs),
    uv=DataMapper("uv-distance", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: da.sqrt((uvw[:,:2]**2).sum(axis=1))/wavel),
    u=DataMapper("u", "wavelengths", column="UVW", extras=["wavel"],
                  mapper=lambda uvw, wavel: uvw[:, 0] / wavel,
                 conjugate=True),
    v=DataMapper("v", "wavelengths", column="UVW", extras=["wavel"],
                 mapper=lambda uvw, wavel: uvw[:, 1] / wavel,
                 conjugate=True),
)


def col_to_label(col):
    """Replaces '-' and "/" in column names with palatable characters acceptable in filenames"""
    return col.replace("-", "min").replace("/", "div")


def getxydata(myms, group_cols, mytaql, chan_freqs, all_plots,
              spws, fields, corrs, noflags, noconj,
              iter_field, iter_spw, iter_scan, iter_corr,
              axis_min, axis_max):

    ms_cols = {'FLAG', 'FLAG_ROW'}

    # get visibility columns
    for _, _, col in all_plots:
        ms_cols.update(DataMapper.get_involved_columns(col))

    # get other columns associated with plot axes
    for xaxis, yaxis, col in all_plots:
        for axis in xaxis, yaxis:
            if mappers[axis].column:
                ms_cols.add(mappers[axis].column)

    # get MS data
    msdata = daskms.xds_from_ms(myms, columns=list(ms_cols), group_cols=group_cols, taql_where=mytaql)

    log.info('                 : Indexing MS, please wait')

    np = 0  # number of points to plot

    # sort out which combinations of axes/columns are necessary
    all_axes_cols = set()
    for xaxis, yaxis, col in all_plots:
        all_axes_cols.add((xaxis, col))
        all_axes_cols.add((yaxis, col))

    axis_col_labels = {}
    for axis, col in all_axes_cols:
        axis_col_labels[axis, col] = "{}_{}".format(axis, col_to_label(col))

    # output dataframes, indexed by (field, spw, scan, antenna, correlation)
    # If any of these axes is not being iterated over, then the index is None
    output_dataframes = OrderedDict()

    # iterate over groups
    for group in msdata:
        ddid     =  group.DATA_DESC_ID  # always present
        fld      =  group.FIELD_ID # always present
        if fld not in fields or ddid not in spws:
            log.debug(f"field {fld} ddid {ddid} not in selection, skipping")
            continue

        scan    = getattr(group, 'SCAN_NUMBER', None)  # will be present if iterating over scans

        # TODO: antenna iteration. None force no iteration for now
        antenna = None

        # always read flags -- easier that way
        flag = group.FLAG
        flag_row = group.FLAG_ROW

        if noflags:
            flag = da.zeros_like(flag)
            flag_row = da.zeros_like(flag_row)

        freqs = chan_freqs[ddid]
        nchan = len(freqs)

        for corr in corrs:
            # make dictionary of extra values for DataMappers
            extras = dict(corr=corr, chans=range(nchan), freqs=freqs, wavel=freq_to_wavel(freqs))

            # get data values per axis
            datums = []
            shapes = []
            # overall shape is NTIME x NFREQ
            shape = flag.shape[:-1]
            # determine overall shape
            for axis, col in all_axes_cols:
                map = mappers[axis]
                vmin, vmax = axis_min.get(axis), axis_max.get(axis)
                value = map.map_value(group, col, corr, extras, flag, flag_row, vmin, vmax)
                # reshape values of shape NTIME to (NTIME,1) and NFREQ to (1,NFREQ)
                if map.axis is not None:
                    assert value.ndim == 1
                    assert value.shape[0] == shape[map.axis], f"{map.fullname}: size {value.shape[0]}, expected {shape[map.axis]}"
                    shape1 = [1,1]
                    shape1[map.axis] = value.shape[0]
                    value = value.reshape(shape1)
                # else 2D value better match expected shape
                else:
                    assert value.shape == shape, f"{map.fullname}: shape {value.shape}, expected {shape}"
                datums.append(value)
                log.debug(f"axis {map.fullname} has shape {value.shape}")

            # broadcast and unravel
            datums = [arr.ravel() for arr in da.broadcast_arrays(*datums)]

            np += datums[0].size

            # if any axis needs to be conjugated, double up all of them
            if not noconj and any([mappers[axis].conjugate for axis, _ in all_axes_cols]):
                for i, (axis, _) in enumerate(all_axes_cols):
                    if mappers[axis].conjugate:
                        datums[i] = da.concatenate([datums[i], -datums[i]])
                    else:
                        datums[i] = da.concatenate([datums[i], datums[i]])

            # now stack them all into a big dataframe
            ddf = dask_df.from_array(da.stack(datums, axis=1), columns=[axis_col_labels[x] for x in all_axes_cols])

            # now, are we iterating or concatenating? Make frame key accordingly
            dataframe_key = (fld if iter_field else None,
                             ddid if iter_spw else None,
                             scan if iter_scan else None,
                             antenna,
                             corr if iter_corr else None)

            # do we already have a frame for this key
            ddf0 = output_dataframes.get(dataframe_key)

            if ddf0 is None:
                log.debug(f"first frame for {dataframe_key}")
                output_dataframes[dataframe_key] = ddf
            else:
                log.debug(f"appending to frame for {dataframe_key}")
                output_dataframes[dataframe_key] = ddf0.append(ddf)

    return output_dataframes, axis_col_labels, np

def run_datashader(ddf,xaxis,yaxis,xcanvas,ycanvas,mycmap,normalize):

    canvas = ds.Canvas(xcanvas, ycanvas)
    agg = canvas.points(ddf, xaxis, yaxis)
    img = hd.shade(hv.Image(agg), cmap=getattr(
        colorcet, mycmap), normalization=normalize)

    # Set plot limits based on data extent or user values for axis labels

    data_xmin = numpy.min(agg.coords[xaxis].values)
    data_ymin = numpy.min(agg.coords[yaxis].values)
    data_xmax = numpy.max(agg.coords[xaxis].values)
    data_ymax = numpy.max(agg.coords[yaxis].values)

    return img.data,data_xmin,data_xmax,data_ymin,data_ymax


def generate_pngname(dirname, name_template, myms,col,corr,xfullname,yfullname,
                myants,ants,myspws,spws,myfields,fields,myscans,scans,
                iterate=None, myiter=0, dostamp=None):

    col = col.replace("/", "div")   # no slashes allowed, so D/M becomes DdivM
    if not name_template:
        pngname = 'plot_'+myms.split('/')[-1]+'_'+col+'_CORR-'+str(corr)
        if myants != 'all' and iterate != 'ant':
            pngname += '_ANT-'+myants.replace(',','-')
        if myspws != 'all' and iterate != 'spw':
            pngname += '_SPW-'+myspws.replace(',', '-')
        if myfields != 'all' and iterate != 'field':
            pngname += '_FIELD-'+myfields.replace(',','-')
        if myscans != 'all' and iterate != 'scan':
            pngname += '_SCAN-'+myscans.replace(',','-')
        if iterate is not None and myiter != -1:
            pngname += "_{}-{}".format(iterate.upper(), myiter)
        pngname += '_'+yfullname+'_vs_'+xfullname+'_'+'corr'+str(corr)
        if dostamp:
            pngname += '_'+stamp()
        pngname += '.png'
    else:
        pngname = name_template.format(**locals())
    if dirname:
        pngname = os.path.join(dirname, pngname)
    return pngname


def generate_title(myms,col,corr,xfullname,yfullname,
                myants,ants,myspws,spws,myfields,fields,myscans,scans,
                iterate,myiter):

    title = myms+' '+col+' (CORR-'+str(corr)+')'
    if myants != 'all' and iterate != 'ant':
        title += ' (ANT-'+myants.replace(',','-')+')'
    if myspws != 'all' and iterate != 'spw':
        title += ' (SPW-'+myspws.replace(',', '-')+')'
    if myfields != 'all' and iterate != 'field':
        title += ' (FIELD-'+myfields.replace(',','-')+')'
    if myscans != 'all' and iterate != 'scan':
        title += ' (SCAN-'+myscans.replace(',','-')+')'
    if myiter != -1:
        title += ' ('+iterate.upper()+'-'+str(myiter)+')'
    return title


def make_plot(data, data_xmin, data_xmax, data_ymin, data_ymax, xmin, xmax, ymin, ymax, 
                xlabel, ylabel, title, pngname, bgcol, fontsize, figx=24, figy=12):

    log.info('                 : Rendering image')

    def match(artist):
        return artist.__module__ == 'matplotlib.text'

    xmin = data_xmin if xmin is None else xmin
    xmax = data_xmax if xmax is None else xmax
    ymin = data_ymin if ymin is None else ymin
    ymax = data_ymax if ymax is None else ymax

    fig = pylab.figure(figsize=(figx, figy))
    ax = fig.add_subplot(111, facecolor=bgcol)
    ax.imshow(X=data, extent=[data_xmin, data_xmax, data_ymin, data_ymax],
              aspect='auto', origin='lower')
    ax.set_title(title,loc='left')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.plot(xmin,ymin,'.',alpha=0.0)
    ax.plot(xmax,ymax,'.',alpha=0.0)

    ax.set_xlim([numpy.min((data_xmin,xmin)),
        numpy.max((data_xmax,xmax))])

    ax.set_ylim([numpy.min((data_ymin,ymin)),
        numpy.max((data_ymax,ymax))])

    for textobj in fig.findobj(match=match):
        textobj.set_fontsize(fontsize)
    fig.savefig(pngname, bbox_inches='tight')

    pylab.close()

    return pngname

