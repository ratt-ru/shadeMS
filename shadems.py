# pip install xarray-ms
# pip install datashader
# pip install holoviews
# pip install pandas

import matplotlib
matplotlib.use('agg')

from collections import OrderedDict as odict
import colorcet
import dask.dataframe as dd
import datashader as ds
from datashader.utils import export_image
from datashader import transfer_functions as tf
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
import os
import pandas as pd
import pylab
import sys
import time
import xarray as xa
import xarrayms as xms

clock_start = time.time()

myms = sys.argv[1].rstrip(os.sep)

xaxis = 'TIME'
yaxis = 'DATA'
corr = 2
spw = 0
doplot = 'amp'
normalize = 'eq_hist'


msdata = xms.xds_from_ms(myms,columns=[yaxis,'TIME','FLAG'])


for data in msdata:
	data.rename({yaxis:'VISDATA'},inplace=True)


if doplot == 'amp':
	visdata = numpy.abs(msdata[spw].VISDATA.values[:,:,corr])
elif doplot == 'phase':
	visdata = numpy.angle(msdata[spw].VISDATA.values[:,:,corr])
elif doplot == 'real':
	visdata = numpy.real(msdata[spw].VISDATA.values[:,:,corr])
elif doplot == 'imag':
	visdata = numpy.imag(msdata[spw].VISDATA.values[:,:,corr])


nchan = visdata.shape[1]
visdata = numpy.ravel(visdata)
xdata = numpy.repeat(msdata[spw].TIME.values,nchan)


dists = {'plotdata': pd.DataFrame(odict([(xaxis,xdata),(yaxis,visdata)]))}
df = pd.concat(dists,ignore_index=True)


canvas = ds.Canvas(2048,1024)
agg = canvas.points(df,xaxis,yaxis)
img = hd.shade(hv.Image(agg),cmap=colorcet.bkr,normalization=normalize)
#img = tf.set_background(tf.shade(agg, cmap=colorcet.dimgray,how='log'),"black")
	

fig = pylab.figure(figsize=(18,8))
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
print 'Plot was rendered in '+elapsed+' seconds.'