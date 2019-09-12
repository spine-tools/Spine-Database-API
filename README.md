# Spine Database API

[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://spine-project.github.io/Spine-Database-API/index.html)

A Python package to access and manipulate Spine databases in a customary, unified way.

## License

Spine Database API is released under the GNU Lesser General Public License (LGPL) license. All accompanying
documentation and manual are released under the Creative Commons BY-SA 4.0 license.

## Getting started

### Installation

To install the package run:

    $ pip install git+https://github.com/Spine-project/Spine-Database-API.git

To upgrade to the most recent version, run:

    $ pip install --upgrade git+https://github.com/Spine-project/Spine-Database-API.git

You can also specify a branch, or a tag, for instance:

    $ pip install --upgrade git+https://github.com/Spine-project/Spine-Database-API.git@dev
    $ pip install --upgrade git+https://github.com/Spine-project/Spine-Database-API.git@v0.0.10


## Building the documentation

Source files for the documentation can be found in `docs/source` directory. In order to 
build the HTML docs, you need to install the additional documentation building requirements
by running:

    $ pip install -r docs-requirements.txt 

This installs Sphinx (among other things), which is required in building the documentation.
When Sphinx is installed, you can build the HTML pages from the source files by running:

    > docs\make.bat html
    
or

    $ pushd docs
    $ make html
    $ popd
    
depending on your operating system.        
 
After running the build, the index page can be found in `docs/build/html/index.html`.

