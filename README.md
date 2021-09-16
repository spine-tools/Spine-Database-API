# Spine Database API

[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://spine-project.github.io/Spine-Database-API/index.html)
[![Unit tests](https://github.com/Spine-project/Spine-Database-API/workflows/Unit%20tests/badge.svg)](https://github.com/Spine-project/Spine-Database-API/actions?query=workflow%3A"Unit+tests")
[![codecov](https://codecov.io/gh/Spine-project/Spine-Database-API/branch/master/graph/badge.svg)](https://codecov.io/gh/Spine-project/Spine-Database-API)
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

You can also specify a branch, or a tag, for instance:

    $ pip install spinedb_api==0.12.1

To install the latest development version use the Git repository url:

    $ pip install --upgrade git+https://github.com/Spine-project/Spine-Database-API.git


## Building the documentation

Source files for the documentation can be found in `docs/source` directory. In order to 
build the HTML docs, you need to install the additional documentation building requirements
by running:

    $ pip install -r dev-requirements.txt 

This installs Sphinx (among other things), which is required in building the documentation.
When Sphinx is installed, you can build the HTML pages from the source files by running:

    > docs\make.bat html
    
or

    $ pushd docs
    $ make html
    $ popd
    
depending on your operating system.        
 
After running the build, the index page can be found in `docs/build/html/index.html`.

&nbsp;
<hr>
<center>
<table width=500px frame="none">
<tr>
<td valign="middle" width=100px>
<img src=https://europa.eu/european-union/sites/europaeu/files/docs/body/flag_yellow_low.jpg alt="EU emblem" width=100%></td>
<td valign="middle">This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 774629.</td>
</table>
</center>
