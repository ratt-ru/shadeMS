# shadems

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![PyPI version shields.io](https://img.shields.io/pypi/v/shadems.svg)](https://pypi.python.org/pypi/shadems/)

`shadems` is a tool for plotting interferometric visibilities or associated metadata from CASA format Measurement Sets. 
The primary goal is rapid visualisation of the many billions of data points produced by a typical observation 
with next-generation radio telescopes such as [MeerKAT](https://www.sarao.ac.za/science-engineering/meerkat/). 
This is achieved by using [`dask-ms`](https://github.com/ska-sa/dask-ms) for access to the MS tables, [`datashader`](https://datashader.org/) 
for rendering, as well as internal parallelism. `shadems` supports arbitrary axis selections for 
MS columns and derivatives (including two-column arithmetic operations) as well as flexible colourisation 
and plot customisation options.

Some example shadeMS outputs (using a 4k channel MeerKAT dataset, and 64 channel VLA dataset) are given below:

| | | | | | |
|-|-|-|-|-|-|
|points plotted|5 billion|10 billion|5 billion|5 billion|25 million|
|runtime|250s|170s|250s|140s|3.5s|
||![](doc/examples/plot-ms-4k-cal-0408-65-V-U-CORRECTED_DATA-I-phase-z10000.png?raw=true "uv-phase")|![](doc/examples/plot-ms-4k-cal-V-U-ANTENNA1-z10000.png?raw=true "uv-coverage")|![](doc/examples/plot-ms-4k-cal-CORRECTED_DATA-XX-amp-FREQ-ANTENNA1-z10000.png?raw=true "Spectrum")|![](doc/examples/plot-ms-4k-cal-J0538-4405-CORRECTED_DATA-I-imag-real-ANTENNA1-z10000.tree.png?raw=true "Phaseball")|![](doc/examples/plot-3C273-C-8424-DATA-I-imag-real-BASELINE.png?raw=true "VLA phaseball")|

---

## Installation

A stable release is installable in the usual way PyPI. 

Installation within a Python 3 [virtual environment](https://pypi.org/project/virtualenv/) is suggested. To begin with:

```
$ virtualenv -p python3 ~/venv/shadems
$ source ~/venv/shadems/bin/activate
```


Then:

```
$ pip install shadems
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

* To change the plot axes you can provide selections via the `--xaxis` / `--yaxis` (or `-x` / `-y`) options. Existing Measurement Set columns (e.g. `DATA`, `CORRECTED_DATA`, `TIME`) as well as standard subsets and derivatives and (e.g. `CHAN`, `FREQ`, `u`, `v`) can be provided. Note the capitalised convention. 

```
$ shadems --xaxis FREQ --yaxis DATA:amp <msname>
```

### Sergeant Colon 

* For complex-valued columns such as in the `DATA` example above, a single component (`amp`, `phase`, `real` or `imag`) must be provided using the colon delimiter. You can also use the 
colon delimiter to specify a correlation product (or a Stokes component -- ``shadems`` knows how
to form them up from correlations in the conventional way):

```
$ shadems --xaxis FREQ --yaxis DATA:amp:XX <msname>
```

### Multiple plots

* The axis selection arguments can also be given a comma-separated list to tell `shadems` to make multiple plots in a single session, for example:

```
$ shadems --xaxis FREQ,CHAN --yaxis DATA:amp,DATA:amp <msname>
```

### Data selection

* Providing a comma-separated list via the relevant argument allows for arbitrary data selection. 
For example, if your calibrator sources are fields 0 and 2, then you can make 'phaseball' 
plots and amplitude vs uv-distance plots for XX and YY of your corrected calibrator data as follows:

  ```
  $ shadems --xaxis CORRECTED_DATA:real,UV --yaxis CORRECTED_DATA:imag,CORRECTED_DATA:amp --field 0,2 --corr XX,YY <msname>
  ```

* You can also use names for ``--field``, ``--ant`` and ``--corr``. 

* ``--corr all`` selects all correlations. Use ``--corr iquv`` to plot them as Stokes instead.

* For channel selection, a `[start]:[stop]:[step]` slice syntax can be used, e.g. to plot only channels 10-20 
(note the Pythonic meaning of ``stop``):

  ```
  $ shadems --xaxis CHAN --yaxis DATA:amp --chan 10:21 <msname>
  ```

* Antenna selection with `[start]:[stop]:[step]` can be done via ``--ant-num`` (and in fact multiple 
comma-separated slices or number may be passed)

  ```
  $ shadems --xaxis CHAN --yaxis DATA:amp --ant-num 0:8,20 <msname>
  ```
  Note the difference w.r.t. ``--ant``. The ``--ant`` form recognizes a list of names or numbers, but 
  will preferentially recognize names. Connoisseurs of VLA MSs will know that antenna number 0 is often
  named "1", causing the users great convenience. ``--ant 1`` will match that antenna. Use ``--ant-num`` if 
  you want to be sure of using numbers.
 


### Iteration

* The first data selection example given above is a much more useful diagnostic tool if you 
can have a single plot for each of the calibrator fields. This is easily achieved with `shadems` by using the `--iter-field` switch. 
The example below will produce a plot per field. 
If no ``--field`` selection is provided, then `shadems` will iterate over all fields in the MS:

```
$ shadems --xaxis CORRECTED_DATA:real,uv --yaxis CORRECTED_DATA:imag,CORRECTED_DATA:amp --field 0,2 --corr XX,YY --iter-field <msname>
```

* You can also iterate over SPWs, scans, correlations and (coming soon) antennas.

### Plotting residuals

* If you want to see how well your model fits your data then you can subtract the `MODEL_DATA` column from the `CORRECTED_DATA` column prior to plotting. For example, to show this residual product on a uv-distance plot:

```
$ shadems --xaxis uv --yaxis CORRECTED_DATA-MODEL_DATA:amp --field 0 --corr XX,YY <msname>
```

* ``CORRECTED_DATA/MODEL_DATA`` can also be useful. For the sake of completeness, ``*`` and ``+`` are also 
recognized (but let us know if you find a good use for them!)

### Colourisation 

* For the plots above, the default behaviour is to have the colour scale trace the density of 
points in the plot according to the selected colourmap. You can instruct `shadems` to instead colour the points on the plot according to a data attribute 
using the `--colour-by` switch. For example, to plot amplitude against uv-distance coloured by antenna 1:

```
$ shadems --xaxis uv --yaxis DATA:amp:XX --colour-by ANTENNA1 <msname>
```

* The `--colour-by` option also supports full MS columns as well as metadata, allowing for 
colourisation by properties such as data amplitude. To colour using such a "continuous" property,
you must specify explicit limits with ``--cmin`` and ``--cmax``.
For example, to make a u,v coverage plot colourised by the corrected visibility amplitudes in XX:

```
$ shadems --xaxis u --yaxis v --colour-by CORRECTED_DATA:amp:XX --cmin 0 --cmax 5 <msname>
```

---

## Full list of arguments

```
Rapid Measurement Set plotting with dask-ms and datashader. Version 0.2.0

positional arguments:
  ms                    Measurement set

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit

Plot types and data sources:
  -x XAXIS, --xaxis XAXIS
                        X axis of plot, e.g. "amp:CORRECTED_DATA" This
                        recognizes all column names (also CHAN, FREQ, CORR,
                        ROW, WAVEL, u, v, w, uv), and, for complex columns,
                        keywords such as 'amp', 'phase', 'real', 'imag'. You
                        can also specify correlations, e.g. 'DATA:phase:XX',
                        and do two-column arithmetic with "+-*/", e.g. 'DATA-
                        MODEL_DATA:amp'. Correlations may be specified by label,
                        number, or as a Stokes parameter (upper case is required
                        to avoid ambiguity with baseline coordinates). The order
                        of specifiers does not matter
  -y YAXIS, --yaxis YAXIS
                        Y axis to plot. Must be given the same number of times
                        as --xaxis. Note that X/Y can employ different columns
                        and correlations.
  -a AAXIS, --aaxis AAXIS
                        Intensity axis. Can be none, or given once, or given
                        the same number of times as --xaxis. If none, plot
                        intensity (a.k.a. alpha channel) is proportional to
                        density of points. Otherwise, a reduction function
                        (see --ared below) is applied to the given values, and
                        the result is used to determine intensity.
  --ared ARED           Alpha axis reduction function. Recognized reductions
                        are count, any, sum, min, max, mean, std, first, last,
                        mode. Default is mean.
  -c COLOUR_BY, --colour-by COLOUR_BY
                        Colour axis. Can be none, or given once, or given the
                        same number of times as --xaxis.
  -C COLUMN, --col COLUMN
                        Name of visibility column (default is DATA), if
                        needed. This is used if the axis specifications do not
                        explicitly include a column. For multiple plots, this
                        can be given multiple times, or as a comma-separated
                        list. Two-column arithmetic is recognized.
  --noflags             Enable to ignore flags. Default is to omit flagged
                        data.
  --noconj              Do not show conjugate points in u,v plots (default =
                        plot conjugates).

Plot axes setup:
  --xmin XMIN           Minimum x-axis value (default = data min). For
                        multiple plots, you can give this multiple times, or
                        use a comma-separated list, but note that the clipping
                        is the same per axis across all plots, so only the
                        last applicable setting will be used. The list may
                        include empty elements (or 'None') to not apply a
                        clip.
  --xmax XMAX           Maximum x-axis value (default = data max).
  --ymin YMIN           Minimum y-axis value (default = data min).
  --ymax YMAX           Maximum y-axis value (default = data max).
  --cmin CMIN           Minimum colouring value. Must be supplied for every
                        non-discrete axis to be coloured by.
  --cmax CMAX           Maximum colouring value. Must be supplied for every
                        non-discrete axis to be coloured by.
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
  --iter-corr           Separate plots per correlation or Stokes (default is
                        to combine in one plot)

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
  --corr CORR           Correlations or Stokes to plot, use indices or labels
                        (comma-separated list, default = all)
  --chan CHAN           Channel slice, as [start]:[stop][:step], default is to
                        plot all channels

Rendering settings:
  -X XCANVAS, --xcanvas XCANVAS
                        Canvas x-size in pixels (default = 1280)
  -Y YCANVAS, --ycanvas YCANVAS
                        Canvas y-size in pixels (default = 900)
  --norm {auto,eq_hist,cbrt,log,linear}
                        Pixel scale normalization (default is 'log' when
                        colouring, and 'eq_hist' when not)
  --cmap CMAP           Colorcet map used without --colour-by (default = bkr),
                        see https://colorcet.holoviz.org
  --bmap BMAP           Colorcet map used when colouring by a continuous axis
                        (default = bkr)
  --dmap DMAP           Colorcet map used when colouring by a discrete axis
                        (default = glasbey_dark)
  --spread-pix PIX      Dynamically spread rendered pixels to this size
  --spread-thr THR      Threshold parameter for spreading (0 to 1, default
                        0.5)
  --bgcol BGCOL         RGB hex code for background colour (default = FFFFFF)
  --fontsize FONTSIZE   Font size for all text elements (default = 20)

Output settings:
  --dir DIR             Send all plots to this output directory
  -s SUFFIX, --suffix SUFFIX
                        suffix to be included in filenames, can include
                        {options}
  --png PNGNAME         Template for output png files, default "plot-{ms}{_fie
                        ld}{_Spw}{_Scan}{_Ant}-{label}{_alphalabel}{_colorlabe
                        l}{_suffix}.png"
  --title TITLE         Template for plot titles, default "{ms}{_field}{_Spw}{
                        _Scan}{_Ant}{_title}{_Alphatitle}{_Colortitle}"
  --xlabel XLABEL       Template for X axis labels, default "{xname}{_xunit}"
  --ylabel YLABEL       Template for X axis labels, default "{yname}{_yunit}"

Performance & tweaking:
  -d, --debug           Enable debugging output
  -z NROWS, --row-chunk-size NROWS
                        Row chunk size for dask-ms. Larger chunks may or may
                        not be faster, but will certainly use more RAM.
  -j N, --num-parallel N
                        Run up to N renderers in parallel. Default is serial.
                        Use -j0 to auto-set this to half the available cores
                        (36 on this system). This is not necessarily faster,
                        as they might all end up contending for disk I/O. This
                        might also work against dask-ms's own intrinsic
                        parallelism. You have been advised.
  --profile             Enable dask profiling output
```
