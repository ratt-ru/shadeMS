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

def make_plot(data,xmin,xmax,ymin,ymax,xlabel,ylabel,title,pngname,figx=24,figy=12):

	fig = pylab.figure(figsize=(figx,figy))
	ax = fig.add_subplot(111)
	ax.imshow(X=data,extent=[xmin,xmax,ymin,ymax],aspect='auto',origin='upper',cmap='gist_heat')
	ax.set_title(myms)
	ax.set_xlabel(xaxis.capitalize())
	ax.set_ylabel(yaxis+' '+doplot.capitalize())
	fig.savefig(pngname,bbox_inches='tight')

	return pngname


def main():


	parser = OptionParser(usage='%prog [options] ms')
	parser.add_option('--xaxis',dest='xaxis',help='x-axis (TIME [default] or CHAN)',default='TIME')
	parser.add_option('--yaxis',dest='yaxis',help='y-axis data column (default = DATA)',default='DATA')
	parser.add_option('--doplot',dest='doplot',help='[a]mplitude (default), [p]hase, [r]eal, [i]maginary',default='a')
	parser.add_option('--field',dest='fields',help='Field ID',default='all')
	parser.add_option('--corr',dest='corr',help='Correlation (default = 0)',default=0)
	parser.add_option('--spw',dest='spw',help='Spectral window (or DDID, default = all)',default='all')
	parser.add_option('--norm',dest='nomralize',help='Pixel scale noramlization (default = eq_hist)',default='eq_hist')
	parser.add_option('--xmin',dest='xmin',help='Minimum x-axis value (default = data min)',default='')
	parser.add_option('--xmax',dest='xmax',help='Maximum x-axis value (default = data max)',default='')
	parser.add_option('--ymin',dest='ymin',help='Minimum y-axis value (default = data min)',default='')
	parser.add_option('--ymax',dest='ymax',help='Maximum y-axis value (default = data max)',default='')
	parser.add_option('--xcanvas',dest='xcanvas',help='Canvas x-size in pixels (default = 2048)',default=2048)
	parser.add_option('--ycanvas',dest='ycanvas',help='Canvas y-size in pixels (default = 1024)',default=1024)
	parser.add_option('--png',dest='pngname',help='PNG name (default = something sensible)',default='')

	(options,args) = parser.parse_args()
	xaxis = options.xaxis
	yaxis = options.yaxis
	doplot = options.doplot.lower()
	fields = options.fields
	corr = options.corr
	normalize = options.normalize
	xmin = options.xmin
	xmax = options.xmax
	ymin = options.ymin
	ymax = options.ymax
	xcanvas = options.xcanvas
	ycancas = options.ycanvas

	if len(args) != 1:
        print 'Please specify a Measurement Set to plot'
        sys.exit()
	else:
        myms = args[0].rstrip('/')

	clock_start = time.time()



	msdata = xms.xds_from_ms(myms,columns=[yaxis,'TIME','FLAG'])



	for group in msdata:
		group.rename({yaxis:'VISDATA'},inplace=True)
		if doplot == 'a':
			group.VISDATA.values = numpy.abs(group.VISDATA.values)
			ylabel = yaxis.capitalize()+' Amplitude'
		elif doplot == 'p':
			group.VISDATA.values = numpy.angle(group.VISDATA.values)
			ylabel = yaxis.capitalize()+' Phase'
		elif doplot == 'r':
			group.VISDATA.values = numpy.real(group.VISDATA.values)
			ylabel = yaxis.capitalize()+' Real'
		elif doplot == 'i':
			group.VISDATA.values = numpy.imag(group.VISDATA.values)
			ylabel = yaxis.capitalize()+' Imaginary'



	visdata = numpy.array(())
	xdata = numpy.array(())


	if xaxis == 'TIME':
		for group in msdata:
			nchan = group.VISDATA.values.shape[1]
			fld = group.FIELD_ID
			ddid = group.DATA_DESC_ID
			visdata = numpy.append(visdata,group.VISDATA.values[:,:,corr])
			xdata = numpy.append(xdata,numpy.repeat(group.TIME.values,nchan))
			xlabel = xaxis.capitalize()+' [s]' # Add t = t - t[0] and make it relative

	elif xaxis == 'CHAN':
		for group in msdata:
			nrows = group.VISDATA.values.shape[0]
			nchan = group.VISDATA.values.shape[1]
			fld = group.FIELD_ID
			ddid = group.DATA_DESC_ID
			visdata = numpy.append(visdata,group.VISDATA.values[:,:,corr])
			xdata = numpy.tile(numpy.arange(nchan),nrows)
			xlabel = xaxis.capitalise()

	title = myms

	dists = {'plotdata': pd.DataFrame(odict([(xaxis,xdata),(yaxis,visdata)]))}
	df = pd.concat(dists,ignore_index=True)

	canvas = ds.Canvas(xcanvas,ycanvas)
	agg = canvas.points(df,xaxis,yaxis)
	img = hd.shade(hv.Image(agg),cmap=colorcet.bkr,normalization=normalize)
	#img = tf.set_background(tf.shade(agg, cmap=colorcet.dimgray,how='log'),"black")
		

	ymin = numpy.min(agg.coords[yaxis].values)
	ymax = numpy.max(agg.coords[yaxis].values)
	xmin = numpy.min(agg.coords[xaxis].values)
	xmax = numpy.max(agg.coords[xaxis].values)

	make_plot(img.data,xmin,xmax,ymin,ymax,xlabel,ylabel,title,pngname)

	clock_stop = time.time()
	elapsed = str(round((clock_stop-clock_start),2))
	print 'Plotted ',len(visdata),' visibility points in '+elapsed+' seconds.'


if __name__ == "__main__":


    main()
