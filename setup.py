from setuptools import setup, find_packages

version = {}
with open("spinedb_api/version.py") as fp:
    exec(fp.read(), version)

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="spinedb_api",
    version=version["__version__"],
    description="An API to talk to Spine databases.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Spine-project/Spine-Database-API",
    author="Spine Project consortium",
    author_email="spine_info@vtt.fi",
    license="LGPL-3.0-or-later",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy==1.3.24",
        "alembic==1.5.8",
        "faker==8.1.2",
        "python-dateutil==2.8.1",
        "numpy==1.20.2",
        "openpyxl==3.0.7",
        "gdx2py==2.1.1",
        "ijson==3.1.4",
    ],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/Spine-project/Spine-Database-API/issues",
        "Documentation": "https://spine-project.github.io/Spine-Database-API/"
    },
)
