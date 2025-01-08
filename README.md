# Spine Database API

[![Documentation Status](https://readthedocs.org/projects/spine-database-api/badge/?version=latest)](https://spine-database-api.readthedocs.io/en/latest/?badge=latest)
[![Unit tests](https://github.com/spine-tools/Spine-Database-API/workflows/Unit%20tests/badge.svg)](https://github.com/spine-tools/Spine-Database-API/actions?query=workflow%3A"Unit+tests")
[![codecov](https://codecov.io/gh/spine-tools/Spine-Database-API/branch/master/graph/badge.svg)](https://codecov.io/gh/spine-tools/Spine-Database-API)
[![PyPI version](https://badge.fury.io/py/spinedb-api.svg)](https://badge.fury.io/py/spinedb-api)

A Python package to access and manipulate Spine databases in a customary, unified way.

## License

Spine Database API is released under the GNU Lesser General Public License (LGPL) license. All accompanying
documentation and manual are released under the Creative Commons BY-SA 4.0 license.

## Getting started

### Installation

To install the package run:

    $ pip install spinedb_api

To upgrade to the most recent version, run:

    $ pip install --upgrade spinedb_api

You can also specify a version, for instance:

    $ pip install spinedb_api==0.32.0

To install the latest development version use the Git repository url:

    $ pip install --upgrade git+https://github.com/spine-tools/Spine-Database-API.git


## Building the documentation

Source files for the documentation can be found in `docs/source` directory. In order to 
build the HTML docs, you need to install the developer dependencies
by running:

    $ pip install -r dev-requirements.txt

This installs Sphinx (among other things), which is required in building the documentation.
When Sphinx is installed, you can build the HTML pages from the source files by running:

    > bin\build_doc.bat
    
or

    $ bin/build_doc.py
    
depending on your operating system.        
 
After running the build, the index page can be found in `docs/build/html/index.html`.

&nbsp;
<hr>
<table width=500px frame="none">
<tr>
<td valign="middle" width=100px>
<img src=fig/eu-emblem-low-res.jpg alt="EU emblem" width=100%></td>
<td valign="middle">This project has received funding from European Climate, Infrastructure and Environment Executive Agency under the European Union’s HORIZON Research and Innovation Actions under grant agreement N°101095998.</td>
<tr>
<td valign="middle" width=100px>
<img src=fig/eu-emblem-low-res.jpg alt="EU emblem" width=100%></td>
<td valign="middle">This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 774629.</td>
</table>
