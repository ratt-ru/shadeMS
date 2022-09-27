#!/usr/bin/env python

import os
from setuptools import setup, find_packages

requirements = [
"dask-ms[xarray]",
"dask[complete]",
"datashader>=0.12.0", 
"holoviews",
"matplotlib>2.2.3; python_version >= '3.5'",
"cmasher",
"future-fstrings",
"requests",
"MSUtils"
]

extras_require = {'testing': ['pytest', 'pytest-flake8']}

PACKAGE_NAME = 'shadems'
__version__ = '0.5.1'

def readme():
    """Get readme content for package long description"""
    with open(os.path.join(build_root, 'README.md')) as f:
        return f.read()

setup(name = PACKAGE_NAME,
    long_description=readme(),
    long_description_content_type="text/markdown",
    version = __version__,
    description = "Rapid Measurement Set plotting with dask-ms and datashader",
    author = "Ian Heywood & RATT",
    author_email = "ian.heywood@physics.ox.ac.uk",
    url = "https://github.com/ratt-ru/shadeMS",
    packages=find_packages(),
    install_requires = requirements,
    extras_require=extras_require,
    include_package_data = True,
    scripts=["bin/" + i for i in os.listdir("bin")],
    license="GNU GPL v2",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: Astronomy"
    ]
)
