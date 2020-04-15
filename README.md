# shadeMS

Rapid Measurement Set plotting with xarray-ms and datashader.

```
Rapid Measurement Set plotting with dask-ms and datashader. Version 0.1.0

positional arguments:
  ms                    Measurement set

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -d, --debug           Enable debugging output

Data selection:
  -x XAXIS, --xaxis XAXIS
                        X axis of plot. Use [t]ime, [f]requency, [c]hannels,
                        [u], [v], [uv]distance, [r]eal, [i]mag, [a]mplitude,
                        [p]hase. For multiple plots, use comma-separated list,
                        or specify multiple times for multiple plots.
  -y YAXIS, --yaxis YAXIS
                        Y axis to plot. Must be given the same number of times
                        as --xaxis.
  -c COL, --col COL     Name of visibility column (default is DATA), if
                        needed. You can also employ 'D-M', 'C-M', 'D/M', 'C/M'
                        for various combinations of data, corrected and model.
                        Can use multiple times, or use comma-separated list,
                        for multiple plots (or else specify it just once).
  --antenna MYANTS      Antenna(s) to plot (comma-separated list, default =
                        all)
  --spw MYSPWS          Spectral windows (DDIDs) to plot (comma-separated
                        list, default = all)
  --field MYFIELDS      Field ID(s) to plot (comma-separated list, default =
                        all)
  --scan MYSCANS        Scans to plot (comma-separated list, default = all)
  --corr CORR           Correlations to plot, use indices or labels (comma-
                        separated list, default is 0)

Plot settings:
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
  --noflags             Enable to include flagged data
  --noconj              Do not show conjugate points in u,v plots (default =
                        plot conjugates)
  --xmin XMIN           Minimum x-axis value (default = data min). Use
                        multiple times for multiple plots, but note that the
                        clipping is the same per axis across all plots, so
                        only the last applicable setting will be used.
  --xmax XMAX           Maximum x-axis value (default = data max)
  --ymin YMIN           Minimum y-axis value (default = data min)
  --ymax YMAX           Maximum y-axis value (default = data max)
  --xcanvas XCANVAS     Canvas x-size in pixels (default = 1280)
  --ycanvas YCANVAS     Canvas y-size in pixels (default = 900)
  --norm {eq_hist,cbrt,log,linear}
                        Pixel scale normalization (default = eq_hist)
  --cmap MYCMAP         Colorcet map to use (default = bkr)
  --bgcol BGCOL         RGB hex code for background colour (default = FFFFFF)
  --fontsize FONTSIZE   Font size for all text elements (default = 20)

Output:
  --png PNGNAME         template for output png files, default "plot-{ms}{_fie
                        ld}{_Spw}{_Scan}{_Ant}{_corr}-{yname}_vs_{xname}.png"
  --title TITLE         template for plot titles, default
                        "{ms}{_field}{_Spw}{_Scan}{_Ant}{_corr}"
  --xlabel XLABEL       template for X axis labels, default "{xname}{_xunit}"
  --ylabel YLABEL       template for X axis labels, default "{yname}{_yunit}"
  -j N, --num-parallel N
                        run up to N renderers in parallel (default = 1). This
                        is not necessarily faster, as they might all end up
                        contending for disk I/O. This might also work against
                        dask-ms's own intrinsic parallelism. You have been
                        advised.s
```