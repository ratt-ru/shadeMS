#!/usr/bin/env python
# ian.heywood@physics.ox.ac.uk

# pip install xarray-ms
# pip install dask
# pip install "dask[dataframe]"
# pip install datashader
# pip install holoviews
# pip install colorcet
# pip install pandas

import matplotlib
matplotlib.use('agg')

from collections import OrderedDict as odict
import colorcet
import dask.dataframe as dd
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
from optparse import OptionParser
import pandas as pd
import pylab
import sys
import time
import xarray as xa
import xarrayms as xms

clock_start = time.time()

myms = sys.argv[1].rstrip('/')

xaxis = 'CHAN'
yaxis = 'DATA'
corr = 0
spw = 0
field = 0
doplot = 'amp'
normalize = 'eq_hist'


msdata = xms.xds_from_ms(myms,columns=[yaxis,'TIME','FLAG'])



for group in msdata:
	group.rename({yaxis:'VISDATA'},inplace=True)
	if doplot == 'amp':
		group.VISDATA.values = numpy.abs(group.VISDATA.values)
	elif doplot == 'phase':
		group.VISDATA.values = numpy.angle(group.VISDATA.values)
	elif doplot == 'real':
		group.VISDATA.values = numpy.real(group.VISDATA.values)
	elif doplot == 'imag':
		group.VISDATA.values = numpy.imag(group.VISDATA.values)

visdata = numpy.array(())
xdata = numpy.array(())

if xaxis == 'TIME':
	for group in msdata:
		nchan = group.VISDATA.values.shape[1]
		fld = group.FIELD_ID
		ddid = group.DATA_DESC_ID
		visdata = numpy.append(visdata,group.VISDATA.values[:,:,corr])
		xdata = numpy.append(xdata,numpy.repeat(group.TIME.values,nchan))

elif xaxis == 'CHAN':
	for group in msdata:
		nrows = group.VISDATA.values.shape[0]
		nchan = group.VISDATA.values.shape[1]
		fld = group.FIELD_ID
		ddid = group.DATA_DESC_ID
		visdata = numpy.append(visdata,group.VISDATA.values[:,:,corr])
		xdata = numpy.tile(numpy.arange(nchan),nrows)


print len(visdata)


dists = {'plotdata': pd.DataFrame(odict([(xaxis,xdata),(yaxis,visdata)]))}
df = pd.concat(dists,ignore_index=True)


canvas = ds.Canvas(2048,1024)
agg = canvas.points(df,xaxis,yaxis)
#agg = canvas.points(msdata[0],'VISDATA','TIME')
img = hd.shade(hv.Image(agg),cmap=colorcet.bkr,normalization=normalize)
#img = tf.set_background(tf.shade(agg, cmap=colorcet.dimgray,how='log'),"black")
	

fig = pylab.figure(figsize=(24,12))
ax = fig.add_subplot(111)
#ax.set_facecolor('black')

ymin = numpy.min(agg.coords[yaxis].values)
ymax = numpy.max(agg.coords[yaxis].values)
xmin = numpy.min(agg.coords[xaxis].values)
xmax = numpy.max(agg.coords[xaxis].values)

ax.imshow(X=img.data,extent=[xmin,xmax,ymin,ymax],aspect='auto',origin='upper',cmap='gist_heat')

ax.set_title(myms)
ax.set_xlabel(xaxis.capitalize())
ax.set_ylabel(yaxis+' '+doplot.capitalize())

fig.savefig('test.png',bbox_inches='tight')

clock_stop = time.time()
elapsed = str(round((clock_stop-clock_start),2))
print 'Plotted',len(visdata),' visibility points in '+elapsed+' seconds.'