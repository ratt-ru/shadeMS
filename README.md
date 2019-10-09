# shadeMS

Rapid Measurement Set plotting with xarray-ms and datashader.

```
pip install colorcet
pip install dask
pip install "dask[dataframe]"
pip install datashader
pip install holoviews
pip install pandas
pip install xarray-ms
```

```
Usage: shadeMS.py [options] ms

Options:
  -h, --help         show this help message and exit
  --xaxis=XAXIS      [t] (default), [f]requency, [c]hannels, [uv]distance,
                     [r]eal
  --yaxis=YAXIS      [a]mplitude (default), [p]hase, [r]eal, [i]maginary
  --col=COL          Measurement Set column to plot (default = DATA)
  --field=MYFIELDS   Field ID(s) to plot (comma separated list, default = all)
  --spws=MYSPWS      Spectral windows (DDIDs) to plot (comma separated list,
                     default = all)
  --corr=CORR        Correlation index to plot (default = 0)
  --noflags          Plot flagged data (default = False)
  --norm=NORMALIZE   Pixel scale normalization (default = eq_hist)
  --xmin=XMIN        Minimum x-axis value (default = data min)
  --xmax=XMAX        Maximum x-axis value (default = data max)
  --ymin=YMIN        Minimum y-axis value (default = data min)
  --ymax=YMAX        Maximum y-axis value (default = data max)
  --xcanvas=XCANVAS  Canvas x-size in pixels (default = 1280)
  --ycanvas=YCANVAS  Canvas y-size in pixels (default = 800)
  --png=PNGNAME      PNG name (default = something very verbose)
```
