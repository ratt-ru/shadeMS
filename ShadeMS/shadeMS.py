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
import pandas as pd
import pylab
import ShadeMS
import sys
import time
import os.path
import re
from collections import OrderedDict

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


def freq_to_wavel(ff):
    c = 299792458.0  # m/s
    return c/ff


def now():
    # stamp = time.strftime('[%Y-%m-%d %H:%M:%S]: ')
    # msg = '\033[92m'+stamp+'\033[0m' # time in green
    stamp = time.strftime(' [%H:%M:%S] ')
    msg = stamp+' '
    return msg


def stamp():
    now = str(datetime.datetime.now()).replace(' ','-').replace(':','-').split('.')[0]
    return now


def blank():
    log.info('------------------------------------------------------')


class DataMapper(object):
    """This class defines a mapping from a dask group to an array of real values to be plotted"""
    def __init__(self, fullname, unit, mapper=None, column=None, extras=None, conjugate=False, corr_axis=True):
        self.fullname, self.unit, self.mapper, self.column, self.extras = fullname, unit, mapper, column, extras
        self.conjugate = conjugate
        self.corr_axis = corr_axis

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

    def map_value(self, group, colname, corr, extras, flag, flag_row):
        """
        """
        # preset column in constructor (UVBW and such) overrides dynamic column name (e.g. for vis columns)
        coldata = self.get_column(group, self.column or colname)
        if self.corr_axis:
            coldata = coldata[..., corr]
        if self.extras:
            coldata = self.mapper(coldata, **{name:extras[name] for name in self.extras })
        elif self.mapper:
            coldata = self.mapper(coldata)
        # apply flags
        if flag is not None and coldata.shape == flag.shape:
            coldata = dama.masked_array(coldata, flag)
        elif flag_row is not None and coldata.shape == flag_row.shape:
            coldata = dama.masked_array(coldata, flag_row)
        return coldata



#
# this dict maps short axis names into full DataMapper objects
mappers = OrderedDict(
    a=DataMapper("Amplitude", "", abs),
    p=DataMapper("Phase", "[deg]", lambda x:da.rad2deg(da.angle(x))),
    r=DataMapper("Real", "", da.real),
    i=DataMapper("Imag", "", da.imag),
    t=DataMapper("Time", "[s]", column="TIME"),
    c=DataMapper("Channel", "", column=None, corr_axis=False, extras=["chans"], mapper=lambda x,chans: chans),
    f=DataMapper("Frequency", "[Hz]", column=None, corr_axis=False, extras=["freqs"], mapper=lambda x, freqs: freqs),
    uv=DataMapper("uv-distance", "[wavelengths]", column="UVW", corr_axis=False, extras=["wavel"],
                  mapper=lambda uvw, wavel: da.sqrt((uvw[:,:2]**2).sum(axis=1))/wavel),
    u=DataMapper("u", "[wavelengths]", column="UVW", corr_axis=False, extras=["wavel"],
                  mapper=lambda uvw, wavel: uvw[:, 0] / wavel,
                 conjugate=True),
    v=DataMapper("v", "[wavelengths]", column="UVW", corr_axis=False, extras=["wavel"],
                 mapper=lambda uvw, wavel: uvw[:, 1] / wavel,
                 conjugate=True),
)


def getxydata(myms,col,group_cols,mytaql,chan_freqs,xaxis,yaxis,spws,fields,corr,noflags,noconj):

    ms_cols = set(DataMapper.get_involved_columns(col))

    if not noflags:
        ms_cols.add('FLAG')
        ms_cols.add('FLAG_ROW')

    for axis in xaxis, yaxis:
        if mappers[axis].column:
            ms_cols.add(mappers[axis].column)

    msdata = daskms.xds_from_ms(
        myms, columns=list(ms_cols),# 'FIELD_ID', 'UVW'],
        group_cols=group_cols,
        taql_where=mytaql)

    log.info('                 : Reading MS, please wait')

    output_df = None
    np = 0  # number of points to plot

    xmap = mappers[xaxis]
    ymap = mappers[yaxis]
    # Get plot data into a pair of numpy arrays

    for group in msdata:
        if noflags:
            flag = flag_row = None
        else:
            flag = group.FLAG
            flag_row = group.FLAG_ROW

        fld = group.FIELD_ID
        ddid = group.DATA_DESC_ID

        if fld in fields and ddid in spws:
            freqs = chan_freqs[ddid]
            nchan = len(freqs)

            # make dictionary of extra values for DataMappers
            extras = dict(corr=corr, chans=range(nchan), freqs=freqs, wavel=freq_to_wavel(freqs))

            # get x and y values
            xdata = xmap.map_value(group, col, corr, extras, flag, flag_row)
            ydata = ymap.map_value(group, col, corr, extras, flag, flag_row)

            # broadcast them to same shape (expanding missing axes as appropriate) and unravel to linear
            xdata, ydata = [arr.ravel() for arr in da.broadcast_arrays(xdata, ydata)]

            # conjugate either axis as appropriatre
            if xmap.conjugate:
                xdata = da.concatenate([xdata,-xdata])
                if not ymap.conjugate:
                    ydata = da.concatenate([ydata,ydata])
            if ymap.conjugate:
                ydata = da.concatenate([ydata,-ydata])
                if not xmap.conjugate:
                    xdata = da.concatenate([xdata,ydata])



            np += xdata.size

            ddf = dask_df.from_array(da.stack([xdata, ydata], axis=1), columns=(xaxis, yaxis))

            if output_df is None:
                output_df = ddf
            else:
                output_df = output_df.append(ddf)

    return output_df, np


def run_datashader(ddf,xaxis,yaxis,xcanvas,ycanvas,
            xmin,xmax,ymin,ymax,mycmap,normalize):

    # if xmin != '':
    #     xmin = float(xmin)
    #     masked_xdata = numpy.ma.masked_less(xdata, xmin)
    #     masked_ydata = numpy.ma.masked_array(
    #         data=ydata, mask=masked_xdata.mask)
    #     ydata = masked_ydata.compressed()
    #     xdata = masked_xdata.compressed()
    #
    # if xmax != '':
    #     xmax = float(xmax)
    #     masked_xdata = numpy.ma.masked_greater(xdata, xmax)
    #     masked_ydata = numpy.ma.masked_array(
    #         data=ydata, mask=masked_xdata.mask)
    #     ydata = masked_ydata.compressed()
    #     xdata = masked_xdata.compressed()
    #
    # if ymin != '':
    #     ymin = float(ymin)
    #     masked_ydata = numpy.ma.masked_less(ydata, ymin)
    #     masked_xdata = numpy.ma.masked_array(
    #         data=xdata, mask=masked_ydata.mask)
    #     ydata = masked_ydata.compressed()
    #     xdata = masked_xdata.compressed()
    #
    # if ymax != '':
    #     ymax = float(ymax)
    #     masked_ydata = numpy.ma.masked_greater(ydata, ymax)
    #     masked_xdata = numpy.ma.masked_array(
    #         data=xdata, mask=masked_ydata.mask)
    #     ydata = masked_ydata.compressed()
    #     xdata = masked_xdata.compressed()
    #
    # # Put plotdata into pandas data frame
    # # This should be possible with xarray directly, but for freq plots we need a corner turn
    #
    # dists = {'plotdata': pd.DataFrame(OrderedDict([(xaxis, xdata), (yaxis, ydata)]))}
    # df = pd.concat(dists, ignore_index=True)
    #
    # Run datashader on the pandas df

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

    if ymin == '':
        ymin = data_ymin
    else:
        ymin = float(ymin)
    if ymax == '':
        ymax = data_ymax
    else:
        ymax = float(ymax)
    if xmin == '':
        xmin = data_xmin
    else:
        xmin = float(xmin)
    if xmax == '':
        xmax = data_xmax
    else:
        xmax = float(xmax)

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

