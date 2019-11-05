#!/usr/bin/env python

import os
from setuptools import setup

requirements = [
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
]

PACKAGE_NAME = 'shadems'
__version__ = '0.0.1'

setup(name = PACKAGE_NAME,
    version = __version__,
    description = "Rapid Measurement Set plotting with dask-ms and datashader",
    author = "Ian Heywood",
    author_email = "ian.heywood@gmail.com",
    url = "https://github.com/IanHeywood/shadeMS",
    packages=["ShadeMS"], 
    install_requires = requirements,
    include_package_data = True,
    scripts=["bin/" + i for i in os.listdir("bin")],
    license=["GNU GPL v2"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: Astronomy"
    ]
     )
