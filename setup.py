#!/usr/bin/env python

import os
from setuptools import setup

requirements = [
"dask-ms[xarray]",
"dask[dataframe]",
"datashader",
"holoviews",
"matplotlib>2.2.3; python_version >= '3.5'",
"requests",
]

PACKAGE_NAME = 'shadems'
__version__ = '0.0.2'

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
