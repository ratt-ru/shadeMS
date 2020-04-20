# shadeMS

`shadems` is a tool for plotting interferometric visibilities or associated metadata from CASA format Measurement Sets. The primary goal is rapid visualisation of the many billions of data points produced by a typical observation with next-generation radio telescopes such as [MeerKAT](https://www.sarao.ac.za/science-engineering/meerkat/). This is achieved by using [`dask-ms`](https://github.com/ska-sa/dask-ms) for access to the MS tables, [`datashader`](https://datashader.org/) for rendering, as well as internal parallelism. `shadems` supports arbitrary axis selections for MS columns and derivatives (including two-column arithmetic operations) as well as flexible colourisation and plot customisation options.

---

## Installation

Installation within a Python 3 [virtual environment](https://pypi.org/project/virtualenv/) is recommended, for example:

* Set up a virtual environment:

```
$ virtualenv ~/venv/shadems
$ source ~/venv/shadems/bin/activate
```

* Clone and install the `shadems` repository:

```
$ cd ~/Software
$ git clone https://github.com/IanHeywood/shadeMS.git
$ pip install -e shadeMS/
```

* The latest version of `dask-ms` is also recommended:

```
$ pip install git+https://github.com/ska-sa/dask-ms.git
```

* As is (for now) this fork of `datashader` which fixes an bug involving layer opacities:

```
$ pip install git+https://github.com/o-smirnov/datashader
```

---

## Operation  

There is no GUI component to `shadems`, to facilitate easy integration into pipelined data reductions on remote machines. All operations are performed via the command line using the `shadems` executable. A full list of arguments grouped by function can be obtained by running:

```
$ shadems -h
```

and is also provided at the end of this page.

### My first plot

* The default settings will produce a plot of amplitude vs time for the `DATA` column. All fields, spectral windows, and correlation products will be selected, and the default [colour scale](https://colorcet.holoviz.org/) will represent the density of visibility points. To do this for your Measurement Set simply run:

```
$ shadems <msname>
```

### Changing the plot axes

* To change the plot axes you can provide selections via the `--xaxis` / `--yaxis` (or `-x` / `-y`) options. Existing Measurement Set columns (e.g. `DATA`, `CORRECTED_DATA`, `TIME`) as well as standard subsets and derivatives and (e.g. `CHAN`, `FREQ`, `U`, `V`) can be provided. Note the capitalised convention. 

```
$ shadems --xaxis FREQ --yaxis DATA <msname>
```

### Sergeant Colon 

* For complex-valued columns a single component (`amp`, `phase`, `real` or `imag`) can be provided using the colon delimiter, as follows:

```
$ shadems --xaxis FREQ --yaxis DATA:amp <msname>
```

* You can also use the colon delimiter to specify a correlation product:

```
$ shadems --xaxis FREQ --yaxis DATA:amp:XX <msname>
```

### Multiple plots

* The axis selection arguments can also be given a comma-separated list to tell `shadems` to make multiple plots in a single session, for example:

```
$ shadems --xaxis FREQ,CHAN --yaxis DATA:amp,DATA:amp <msname>
```

### Data selection

* Providing a comma-separated list via the relevant argument allows for arbitrary data selection. For example, if your calibrator sources are fields `0` and `2`, then you can make 'phaseball' plots and amplitude vs uv-distance plots for XX and YY of your corrected calibrator data as follows:

```
$ shadems --xaxis CORRECTED_DATA:real,UV --yaxis CORRECTED_DATA:imag,CORRECTED_DATA:amp --field 0,2 --corr XX,YY <msname>
```

* For antenna and channel selection a `[start]:[stop]:[step]` slice syntax can be used, e.g. to plot only channels 10-20 (inclusive):

```
$ shadems --xaxis CHAN --yaxis DATA:amp --chan 10:21 <msname>
```

### Iteration

* The first data selection example given above is a much more useful diagnostic tool if you can have a single plot for each of the calibrator fields. This is easily achieved with `shadems` by using the `--iter-fields` switch. The example below will produce a plot per field. If no field selection is provided then `shadems` will iterate over all fields in the MS:

```
$ shadems --xaxis CORRECTED_DATA:real,UV --yaxis CORRECTED_DATA:imag,CORRECTED_DATA:amp --field 0,2 --corr XX,YY --iter-fields <msname>
```

### Plotting residuals

* If you want to see how well your model fits your data then you can subtract the `MODEL_DATA` column from the `CORRECTED_DATA` column prior to plotting. For example, to show this residual product on a uv-distance plot:

```
$ shadems --xaxis UV --yaxis CORRECTED_DATA-MODEL_DATA:amp --field 0 --corr XX,YY <msname>
```

### Colourisation 

* For the plots above the default behaviour is to have the colour scale trace the density of points in the plot according to the selected colourmap. You can instruct `shadems` to instead colour the points on the plot according to a data attribute using the `--colour-by` switch. For example, to plot amplitude against uv-distance coloured by antenna:

```
$ shadems --xaxis UV --yaxis DATA:amp:XX --colour-by antenna <msname>
```

* The `--colour-by` option also supports full MS columns as well as metadata, allowing for colourisation by properties such as data amplitude. To make a u,v coverage plot colourised by the corrected visibility amplitudes in XX:

```
$ shadems --xaxis U --yaxis V --colour-by CORRECTED_DATA:amp:XX <msname>
```

---

## Full list of arguments

```
positional arguments:
  ms                    Measurement set

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit

Plot types and data sources:
  -x XAXIS, --xaxis XAXIS
                        X axis of plot, e.g. "amp:CORRECTED_DATA" This
                        recognizes all column names (also CHAN, FREQ, CORR,
                        ROW, WAVEL, U, V, W, UV), and, for complex columns,
                        keywords such as 'amp', 'phase', 'real', 'imag'. You
                        can also specify correlations, e.g. 'DATA:phase:XX',
                        and do two-column arithmetic with "+-*/", e.g. 'DATA-
                        MODEL_DATA:amp'. The order of specifiers does not
                        matter.
  -y YAXIS, --yaxis YAXIS
                        Y axis to plot. Must be given the same number of times
                        as --xaxis. Note that X/Y can employ different columns
                        and correlations.
  -c COLOUR_BY, --colour-by COLOUR_BY
                        Colour by axis and/or column. Can be none, or given
                        once, or given the same number of times as --xaxis.
  -C COLUMN, --col COLUMN
                        Name of visibility column (default is DATA), if
                        needed. This is used if the axis specifications do not
                        explicitly include a column. For multiple plots, thuis
                        can be given multiple times, or as a comma-separated
                        list. Two-column arithmetic is recognized.
  --noflags             Enable to ignore flags. Default is to omit flagged
                        data.
  --noconj              Do not show conjugate points in u,v plots (default =
                        plot conjugates)

Plot axes setup:
  --xmin XMIN           Minimum x-axis value (default = data min). For
                        multiple plots, you can give this multiple times, or
                        use a comma-separated list, but note that the clipping
                        is the same per axis across all plots, so only the
                        last applicable setting will be used. The list may
                        include empty elements (or 'None') to not apply a
                        clip.
  --xmax XMAX           Maximum x-axis value (default = data max)
  --ymin YMIN           Minimum y-axis value (default = data min)
  --ymax YMAX           Maximum y-axis value (default = data max)
  --cmin CMIN           Minimum colouring value. Must be supplied for every
                        non-discrete axis to be coloured by
  --cmax CMAX           Maximum colouring value. Must be supplied for every
                        non-discrete axis to be coloured by
  --cnum CNUM           Number of steps used to discretize a continuous axis.
                        Default is 16.

Options for multiple plots or combined plots:
  --iter-field          Separate plots per field (default is to combine in one
                        plot)
  --iter-antenna        Separate plots per antenna (default is to combine in
                        one plot)
  --iter-spw            Separate plots per spw (default is to combine in one
                        plot)
  --iter-scan           Separate plots per scan (default is to combine in one
                        plot)
  --iter-corr           Separate plots per correlation (default is to combine
                        in one plot)

Data subset selection:
  --ant ANT             Antennas to plot (comma-separated list of names,
                        default = all)
  --ant-num ANT_NUM     Antennas to plot (comma-separated list of numbers, or
                        a [start]:[stop][:step] slice, overrides --ant)
  --baseline BASELINE   Baselines to plot, as 'ant1-ant2' (comma-separated
                        list, default = all)
  --spw SPW             Spectral windows (DDIDs) to plot (comma-separated
                        list, default = all)
  --field FIELD         Field ID(s) to plot (comma-separated list, default =
                        all)
  --scan SCAN           Scans to plot (comma-separated list, default = all)
  --corr CORR           Correlations to plot, use indices or labels (comma-
                        separated list, default is 0)
  --chan CHAN           Channel slice, as [start]:[stop][:step], default is to
                        plot all channels

Rendering settings:
  --xcanvas XCANVAS     Canvas x-size in pixels (default = 1280)
  --ycanvas YCANVAS     Canvas y-size in pixels (default = 900)
  --norm {eq_hist,cbrt,log,linear}
                        Pixel scale normalization (default = eq_hist)
  --cmap CMAP           Colorcet map used without --color-by (default = bkr)
  --bmap BMAP           Colorcet map used when coloring by a continuous axis
                        (default = bkr)
  --dmap DMAP           Colorcet map used when coloring by a discrete axis
                        (default = glasbey_dark)
  --bgcol BGCOL         RGB hex code for background colour (default = FFFFFF)
  --fontsize FONTSIZE   Font size for all text elements (default = 20)

Output settings:
  --dir DIR             send all plots to this output directory
  --png PNGNAME         template for output png files, default "plot-{ms}{_fie
                        ld}{_Spw}{_Scan}{_Ant}-{label}{_colorlabel}.png"
  --title TITLE         template for plot titles, default
                        "{ms}{_field}{_Spw}{_Scan}{_Ant}{_title}{_Colortitle}"
  --xlabel XLABEL       template for X axis labels, default "{xname}{_xunit}"
  --ylabel YLABEL       template for X axis labels, default "{yname}{_yunit}"

Performance & tweaking:
  -d, --debug           Enable debugging output
  -z NROWS, --row-chunk-size NROWS
                        row chunk size for dask-ms. Larger chunks may or may
                        not be faster, but will certainly use more RAM.
  -j N, --num-parallel N
                        run up to N renderers in parallel. Default is serial.
                        Use -j0 to auto-set this to half the available cores
                        (16 on this system). This is not necessarily faster,
                        as they might all end up contending for disk I/O. This
                        might also work against sdask-ms's own intrinsic
                        parallelism. You have been advised.
```