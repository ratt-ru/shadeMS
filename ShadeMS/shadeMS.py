#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

import colorcet
import daskms as xms
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
import pandas as pd
import pylab
import ShadeMS
import sys
import time

from collections import OrderedDict as odict

log = ShadeMS.log

def get_chan_freqs(myms):
    spw_tab = xms.xds_from_table(
        myms+'::SPECTRAL_WINDOW', columns=['CHAN_FREQ'])
    chan_freqs = spw_tab[0].CHAN_FREQ
    return chan_freqs


def get_field_names(myms):
    field_tab = xms.xds_from_table(
        myms+'::FIELD', columns=['NAME','SOURCE_ID'])
    field_ids = field_tab[0].SOURCE_ID.values
    field_names = field_tab[0].NAME.values
    return field_ids, field_names


def get_scan_numbers(myms):
    tab = xms.xds_from_table(
        myms, columns=['SCAN_NUMBER'])
    scan_numbers = numpy.unique(tab[0].SCAN_NUMBER.values)
    return scan_numbers.tolist()


def get_antennas(myms):
    tab = xms.xds_from_table(
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
    return str(time.time()).replace('.', '')


def blank():
    log.info('--------------------------------------------------')


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
            units = xx[2]
    return fullname, units


def getxydata(myms,col,group_cols,mytaql,chan_freqs,xaxis,yaxis,spws,fields,corr,noflags,noconj):

    msdata = xms.xds_from_ms(
        myms, columns=[col, 'TIME', 'FLAG', 'FIELD_ID', 'UVW'], 
        group_cols=group_cols,
        taql_where=mytaql)

    # Replace xarray data with a,p,r,i in situ

    log.info('Getting plot data, please wait...')

    for i in range(0, len(msdata)):
        msdata[i] = msdata[i].rename({col: 'VISDATA'})

    # Initialise arrays for plot data

    ydata = numpy.array(())
    xdata = numpy.array(())
    flags = numpy.array(())

    # Get plot data into a pair of numpy arrays

    for group in msdata:
        nrows = group.VISDATA.shape[0]
        nchan = group.VISDATA.shape[1]
        fld = group.FIELD_ID
        ddid = group.DATA_DESC_ID

        if fld in fields and ddid in spws:
            chans = chan_freqs.values[ddid]
            flags = numpy.append(flags, group.FLAG.values[:, :, corr])

            if xaxis == 'uv' or xaxis == 'u' or yaxis == 'v':
                uu = group.UVW.values[:, 0]
                vv = group.UVW.values[:, 1]
                chans_wavel = freq_to_wavel(chans)
                uu_wavel = numpy.ravel(
                    uu / numpy.transpose(numpy.array([chans_wavel, ]*len(uu))))
                vv_wavel = numpy.ravel(
                    vv / numpy.transpose(numpy.array([chans_wavel, ]*len(vv))))
                uvdist_wavel = ((uu_wavel**2.0)+(vv_wavel**2.0))**0.5

            if yaxis == 'a':
                ydata = numpy.append(ydata, numpy.abs(
                    group.VISDATA.values[:, :, corr]))
            elif yaxis == 'p':
                ydata = numpy.append(ydata, numpy.angle(
                    group.VISDATA.values[:, :, corr]))
            elif yaxis == 'r':
                ydata = numpy.append(ydata, numpy.real(
                    group.VISDATA.values[:, :, corr]))
            elif yaxis == 'i':
                ydata = numpy.append(ydata, numpy.imag(
                    group.VISDATA.values[:, :, corr]))
            elif yaxis == 'v':
                ydata = numpy.append(ydata, vv_wavel)

            if xaxis == 'f':
                xdata = numpy.append(xdata, numpy.tile(chans, nrows))
            elif xaxis == 'c':
                xdata = numpy.append(xdata, numpy.tile(
                    numpy.arange(nchan), nrows))
            elif xaxis == 't':
                # Add t = t - t[0] and make it relative
                xdata = numpy.append(
                    xdata, numpy.repeat(group.TIME.values, nchan))
            elif xaxis == 'uv':
                xdata = numpy.append(xdata, uvdist_wavel)
            elif xaxis == 'r':
                xdata = numpy.append(xdata, numpy.real(
                    group.VISDATA.values[:, :, corr]))
            elif xaxis == 'u':
                xdata = numpy.append(xdata, uu_wavel)
            elif xaxis == 'a':
                xdata = numpy.append(xdata, numpy.abs(
                    group.VISDATA.values[:, :, corr]))

    # Drop flagged data if required

    if not noflags:

        bool_flags = list(map(bool, flags))

        masked_ydata = numpy.ma.masked_array(data=ydata, mask=bool_flags)
        masked_xdata = numpy.ma.masked_array(data=xdata, mask=bool_flags)

        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    # Plot the conjugate points for a u,v plot if requested
    # This is done at this stage so we don't have to worry about the flags

    if not noconj and xaxis == 'u' and yaxis == 'v':
        xdata = numpy.append(xdata, xdata*-1.0)
        ydata = numpy.append(ydata, ydata*-1.0)


    return xdata,ydata


def run_datashader(xdata,ydata,xaxis,yaxis,xcanvas,ycanvas,
            xmin,xmax,ymin,ymax,mycmap,normalize):

    if xmin != '':
        xmin = float(xmin)
        masked_xdata = numpy.ma.masked_less(xdata, xmin)
        masked_ydata = numpy.ma.masked_array(
            data=ydata, mask=masked_xdata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if xmax != '':
        xmax = float(xmax)
        masked_xdata = numpy.ma.masked_greater(xdata, xmax)
        masked_ydata = numpy.ma.masked_array(
            data=ydata, mask=masked_xdata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if ymin != '':
        ymin = float(ymin)
        masked_ydata = numpy.ma.masked_less(ydata, ymin)
        masked_xdata = numpy.ma.masked_array(
            data=xdata, mask=masked_ydata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    if ymax != '':
        ymax = float(ymax)
        masked_ydata = numpy.ma.masked_greater(ydata, ymax)
        masked_xdata = numpy.ma.masked_array(
            data=xdata, mask=masked_ydata.mask)
        ydata = masked_ydata.compressed()
        xdata = masked_xdata.compressed()

    # Put plotdata into pandas data frame
    # This should be possible with xarray directly, but for freq plots we need a corner turn

    dists = {'plotdata': pd.DataFrame(odict([(xaxis, xdata), (yaxis, ydata)]))}
    df = pd.concat(dists, ignore_index=True)

    # Run datashader on the pandas df

    canvas = ds.Canvas(xcanvas, ycanvas)
    agg = canvas.points(df, xaxis, yaxis)
    img = hd.shade(hv.Image(agg), cmap=getattr(
        colorcet, mycmap), normalization=normalize)

    # Set plot limits based on data extent or user values for axis labels

    data_xmin = numpy.min(agg.coords[xaxis].values)
    data_ymin = numpy.min(agg.coords[yaxis].values)
    data_xmax = numpy.max(agg.coords[xaxis].values)
    data_ymax = numpy.max(agg.coords[yaxis].values)

    return img.data,data_xmin,data_xmax,data_ymin,data_ymax


def generate_pngname(myms,col,corr,xfullname,xunits,yfullname,yunits,spwstr,fieldstr,scanstr,dostamp):
    title = myms+' '+col+' (correlation '+str(corr)+')'
    ylabel = yfullname+' '+yunits
    xlabel = xfullname+' '+xunits
    if pngname == '':
        pngname = 'plot_'+myms.split('/')[-1]+'_'+col+'_'
        pngname += 'SPW-' + myspws.replace(',', '-')+ \
            '_FIELD-'+myfields.replace(',', '-')+\
            '_SCAN-'+myscans.replace(',', '-')+'_'
        pngname += yfullname+'_vs_'+xfullname+'_'+'corr'+str(corr)
        if dostamp:
            pngname += '_'+sms.stamp()
        pngname += '.png'
    return pngname



def make_plot(data, data_xmin, data_xmax, data_ymin, data_ymax, xmin, xmax, ymin, ymax, 
                xlabel, ylabel, title, pngname, bgcol, fontsize, figx=24, figy=12):

    log.info('Rendering plot')

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

    fig.clf()

    return pngname

