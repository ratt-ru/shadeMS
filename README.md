# shadeMS

Rapid Measurement Set plotting with xarray-ms and datashader.

```
positional arguments:
  ms                   Measurement set

optional arguments:
  -h, --help           show this help message and exit
  -v, --version        show program's version number and exit

Data selection:
  --xaxis XAXIS        [t]ime (default), [f]requency, [c]hannels, [u],
                       [uv]distance, [r]eal, [a]mplitude
  --yaxis YAXIS        [a]mplitude (default), [p]hase, [r]eal, [i]maginary,
                       [v]
  --col COL            Measurement Set column to plot (default = DATA)
  --antenna MYANTS     Antenna(s) to plot (comma-separated list, default =
                       all)
  --spw MYSPWS         Spectral windows (DDIDs) to plot (comma-separated list,
                       default = all)
  --field MYFIELDS     Field ID(s) to plot (comma-separated list, default =
                       all)
  --scan MYSCANS       Scans to plot (comma-separated list, default = all)
  --corr CORR          Correlation index to plot (default = 0)

Plot settings:
  --iterate ITERATE    Set to antenna, spw, field or scan to produce a plot
                       per selection or MS content (default = do not iterate)
  --noflags            Enable to include flagged data
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

Output:
  --png PNGNAME        PNG name, without extension, iterations will be added
                       (default = something verbose)
  --dest DESTDIR       Destination path for output PNGs (will be created if
                       not present, default = CWD)
  --stamp              Enable to add timestamp to default PNG name
```