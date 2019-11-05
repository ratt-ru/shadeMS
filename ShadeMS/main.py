#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk

import matplotlib
matplotlib.use('agg')

from argparse import ArgumentParser
import logging
from ShadeMS import shadeMS as sms
import time
import numpy
import daskms as xms
import pandas as pd
import colorcet
import holoviews as hv
import holoviews.operation.datashader as hd
import datashader as ds
import dask.dataframe as dd
from collections import OrderedDict as odict
import pkg_resources
import logging

# create logger with 'spam_application'
log = logging.getLogger('shadems')
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('log-shadems.txt')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)

try:
    __version__ = pkg_resources.require("shadems")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"

def main(argv):

    clock_start = time.time()

    # Command line options

    parser = ArgumentParser(description='Rapid Measurement Set plotting with xarray-ms and datashader. Version {0:s}'.format(__version__))

    parser.add_argument('ms', 
                      help='Measurement set')
    parser.add_argument("-v", "--version", action='version',
                      version='{:s} version {:s}'.format(parser.prog, __version__))
    parser.add_argument('--xaxis', dest='xaxis',
                      help='[t]ime (default), [f]requency, [c]hannels, [u], [uv]distance, [r]eal, [a]mplitude', default='t')
    parser.add_argument('--yaxis', dest='yaxis',
                      help='[a]mplitude (default), [p]hase, [r]eal, [i]maginary, [v]', default='a')
    parser.add_argument('--col', dest='col',
                      help='Measurement Set column to plot (default = DATA)', default='DATA')
    parser.add_argument('--field', dest='myfields',
                      help='Field ID(s) to plot (comma separated list, default = all)', default='all')
    parser.add_argument('--spws', dest='myspws',
                      help='Spectral windows (DDIDs) to plot (comma separated list, default = all)', default='all')
    parser.add_argument('--corr', dest='corr',
                      help='Correlation index to plot (default = 0)', default=0)
    parser.add_argument('--noflags', dest='noflags',
                      help='Plot flagged data (default = False)', action='store_true', default=False)
    parser.add_argument('--noconj', dest='noconj',
                      help='Do not show conjugate points in u,v plots (default = plot conjugates)', action='store_true', default=False)
    parser.add_argument('--xmin', dest='xmin',
                      help='Minimum x-axis value (default = data min)', default='')
    parser.add_argument('--xmax', dest='xmax',
                      help='Maximum x-axis value (default = data max)', default='')
    parser.add_argument('--ymin', dest='ymin',
                      help='Minimum y-axis value (default = data min)', default='')
    parser.add_argument('--ymax', dest='ymax',
                      help='Maximum y-axis value (default = data max)', default='')
    parser.add_argument('--xcanvas', dest='xcanvas',
                      help='Canvas x-size in pixels (default = 1280)', default=1280)
    parser.add_argument('--ycanvas', dest='ycanvas',
                      help='Canvas y-size in pixels (default = 800)', default=900)
    parser.add_argument('--norm', dest='normalize',
                      help='Pixel scale normalization: eq_hist (default), cbrt, log, linear', default='eq_hist')
    parser.add_argument('--cmap', dest='mycmap',
                      help='Colorcet map to use (default = bkr)', default='bkr')
    parser.add_argument('--bgcol', dest='bgcol',
                      help='RGB hex code for background colour (default = FFFFFF)', default='FFFFFF')
    parser.add_argument('--fontsize', dest='fontsize',
                      help='Font size for all text elements (default = 20)', default=20)
    parser.add_argument('--png', dest='pngname',
                      help='PNG name (default = something verbose)', default='')
    parser.add_argument('--stamp', dest='dostamp',
                      help='Add timestamp to default PNG name', action='store_true', default=False)

    # Assign inputs

    options = parser.parse_args(argv)

    xaxis = options.xaxis.lower()
    yaxis = options.yaxis.lower()
    col = options.col.upper()
    myfields = options.myfields
    corr = int(options.corr)
    myspws = options.myspws
    noflags = options.noflags
    noconj = options.noconj
    xmin = options.xmin
    xmax = options.xmax
    ymin = options.ymin
    ymax = options.ymax
    xcanvas = int(options.xcanvas)
    ycanvas = int(options.ycanvas)
    normalize = options.normalize
    mycmap = options.mycmap
    bgcol = '#'+options.bgcol.lstrip('#')
    fontsize = options.fontsize
    pngname = options.pngname
    dostamp = options.dostamp

    # Trap no MS

    myms = options.ms.rstrip('/')

    # Check for allowed axes

    allowed = ['a', 'p', 'r', 'i', 't', 'f', 'c', 'uv', 'u', 'v']
    if xaxis not in allowed or yaxis not in allowed:
        raise ValueError('xaxis "%s" is unknown. Please check requested axes' % xaxis)

    xfullname, xunits = sms.fullname(xaxis)
    yfullname, yunits = sms.fullname(yaxis)

    log.info('Plotting %s vs %s' % (yfullname, xfullname))
    log.info('Correlation index %d' % corr)

    # Get MS metadata

    chan_freqs = sms.get_chan_freqs(myms)
    field_ids, field_names = sms.get_field_names(myms)

    # Sort out field selection(s)

    if myfields == 'all':
        fields = field_ids
        # fields = []
        # for group in msdata:
        #   fields.append(group.FIELD_ID)
        # fields = numpy.unique(fields)
    else:
        fields = list(map(int, myfields.split(',')))

    sms.blank()
    log.info('FIELD_ID   NAME')
    for i in fields:
        log.info('%-10s %-16s' % (i, field_names[i]))

    # Sort out SPW selection(s)

    if myspws == 'all':
        spws = numpy.arange(len(chan_freqs))
    else:
        spws = list(map(int, myspws.split(',')))

    sms.blank()
    log.info('SPW_ID     NCHAN ')
    for i in spws:
        nchan = len(chan_freqs.values[i])
        log.info('%-10s %-16s' % (i, nchan))

    sms.blank()

    # Construct TaQL string based on FIELD and SPW selections

    field_taq = []
    for fld in fields:
        field_taq.append('FIELD_ID=='+str(fld))

    spw_taq = []
    for spw in spws:
        spw_taq.append('DATA_DESC_ID=='+str(spw))

    mytaql = '('+' || '.join(field_taq)+') && ('+' || '.join(spw_taq)+')'

    # Read the selected data

    log.info('Reading %s' % (myms))
    log.info('%s column' % (col))

    msdata = xms.xds_from_ms(
        myms, columns=[col, 'TIME', 'FLAG', 'FIELD_ID', 'UVW'], taql_where=mytaql)

    # Replace xarray data with a,p,r,i in situ

    log.info('Rearranging the deck chairs')

    for i in range(0, len(msdata)):
        msdata[i] = msdata[i].rename({col: 'VISDATA'})

    # Initialise arrays for plot data

    ydata = numpy.array(())
    xdata = numpy.array(())
    flags = numpy.array(())

    # Get plot data into a pair of numpy arrays

    for group in msdata:
        nrows = group.VISDATA.values.shape[0]
        nchan = group.VISDATA.values.shape[1]
        fld = group.FIELD_ID
        ddid = group.DATA_DESC_ID
        if fld in fields and ddid in spws:
            chans = chan_freqs.values[ddid]
            flags = numpy.append(flags, group.FLAG.values[:, :, corr])

            if xaxis == 'uv' or xaxis == 'u' or yaxis == 'v':
                uu = group.UVW.values[:, 0]
                vv = group.UVW.values[:, 1]
                chans_wavel = sms.freq_to_wavel(chans)
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
                ydata = uu_wavel

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
                xdata = uvdist_wavel
            elif xaxis == 'r':
                xdata = numpy.append(xdata, numpy.real(
                    group.VISDATA.values[:, :, corr]))
            elif xaxis == 'u':
                xdata = vv_wavel
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

    # Drop data out of plot range(s)

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

    log.info('Making Pandas dataframe')

    dists = {'plotdata': pd.DataFrame(odict([(xaxis, xdata), (yaxis, ydata)]))}
    df = pd.concat(dists, ignore_index=True)

    # Run datashader on the pandas df

    log.info('Running datashader')

    canvas = ds.Canvas(xcanvas, ycanvas)
    agg = canvas.points(df, xaxis, yaxis)
    img = hd.shade(hv.Image(agg), cmap=getattr(
        colorcet, mycmap), normalization=normalize)

    # Set plot limits based on data extent or user values for axis labels

    if ymin == '':
        ymin = numpy.min(agg.coords[yaxis].values)
    else:
        ymin = float(ymin)
    if ymax == '':
        ymax = numpy.max(agg.coords[yaxis].values)
    else:
        ymax = float(ymax)
    if xmin == '':
        xmin = numpy.min(agg.coords[xaxis].values)
    else:
        xmin = float(xmin)
    if xmax == '':
        xmax = numpy.max(agg.coords[xaxis].values)
    else:
        xmax = float(xmax)

    # Setup plot labels and PNG name

    ylabel = yfullname+' '+yunits
    xlabel = xfullname+' '+xunits
    title = myms+' '+col+' (correlation '+str(corr)+')'
    if pngname == '':
        pngname = 'plot_'+myms.split('/')[-1]+'_'+col+'_'
        pngname += 'SPW-' + \
            myspws.replace(',', '-')+'_FIELD-'+myfields.replace(',', '-')+'_'
        pngname += yfullname+'_vs_'+xfullname+'_'+'corr'+str(corr)
        if dostamp:
            pngname += '_'+sms.stamp()
        pngname += '.png'

    # Render the plot

    log.info('Rendering plot')

    sms.make_plot(img.data,
              xmin,
              xmax,
              ymin,
              ymax,
              xlabel,
              ylabel,
              title,
              pngname,
              bgcol,
              fontsize,
              figx=xcanvas/60,
              figy=ycanvas/60)

    # Stop the clock

    clock_stop = time.time()
    elapsed = str(round((clock_stop-clock_start), 2))

    log.info('Done. Elapsed time: %s seconds.' % (elapsed))
