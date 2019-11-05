# shadeMS

Rapid Measurement Set plotting with xarray-ms and datashader.

Requirements:

```
"colorcet",
"holoviews",
"pandas",
"dask[complete]",
"dask-ms[xarray]",
"datashader",
"future",
"matplotlib<=2.2.3; python_version <= '2.7'",
"matplotlib>2.2.3; python_version >= '3.5'",
"numpy>=1.14",
"python-casacore",
"xarray",
"requests[cmd]",
```

Help:

```
usage: shadems [options] ms

Rapid Measurement Set plotting with xarray-ms and datashader

positional arguments:
  ms                   Measurement set

optional arguments:
  -h, --help           show this help message and exit
  --xaxis XAXIS        [t]ime (default), [f]requency, [c]hannels, [u],
                       [uv]distance, [r]eal, [a]mplitude
  --yaxis YAXIS        [a]mplitude (default), [p]hase, [r]eal, [i]maginary,
                       [v]
  --col COL            Measurement Set column to plot (default = DATA)
  --field MYFIELDS     Field ID(s) to plot (comma separated list, default =
                       all)
  --spws MYSPWS        Spectral windows (DDIDs) to plot (comma separated list,
                       default = all)
  --corr CORR          Correlation index to plot (default = 0)
  --noflags            Plot flagged data (default = False)
  --noconj             Do not show conjugate points in u,v plots (default =
                       plot conjugates)
  --xmin XMIN          Minimum x-axis value (default = data min)
  --xmax XMAX          Maximum x-axis value (default = data max)
  --ymin YMIN          Minimum y-axis value (default = data min)
  --ymax YMAX          Maximum y-axis value (default = data max)
  --xcanvas XCANVAS    Canvas x-size in pixels (default = 1280)
  --ycanvas YCANVAS    Canvas y-size in pixels (default = 800)
  --norm NORMALIZE     Pixel scale normalization: eq_hist (default), cbrt,
                       log, linear
  --cmap MYCMAP        Colorcet map to use (default = bkr)
  --bgcol BGCOL        RGB hex code for background colour (default = FFFFFF)
  --fontsize FONTSIZE  Font size for all text elements (default = 20)
  --png PNGNAME        PNG name (default = something verbose)
  --stamp              Add timestamp to default PNG name
```

To-do:

```
- CASA-style formatting for SPW selection
- Antenna / baseline selection
- Arbitrary plot selections for x and y axes
- Iteration option for multi-panel plots?
```
