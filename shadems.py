# pip install xarray-ms
# pip install datashader
# pip install holoviews

import colorcet
import dask.dataframe as dd
import datashader as ds
from datashader.utils import export_image
from datashader import transfer_functions as tf
import holoviews as hv
import holoviews.operation.datashader as hd
import numpy
import sys
import xarrayms as xa

hv.extension('matplotlib')
#%%output backend='matplotlib'


myms = sys.argv[1]

msdata = xa.xds_from_ms(myms,columns=['DATA','TIME'])
msdata[0].DATA.values = numpy.abs(msdata[0].DATA.values)


canvas = ds.Canvas(1024,512)
agg = canvas.points(msdata[0],'TIME','DATA')
export_image(tf.set_background(tf.shade(agg, cmap=colorcet.bmy,how='log'),"black"),'test')
#export_image(tf.shade(agg),'test.png')
