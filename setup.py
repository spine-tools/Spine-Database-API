from setuptools import setup, find_packages

version = {}
with open("spinedb_api/version.py") as fp:
    exec(fp.read(), version)

setup(
    name="spinedb_api",
    version=version["__version__"],
    description="An API to talk to Spine databases.",
    url="https://github.com/Spine-project/Spine-Database-API",
    author="Spine Project consortium",
    author_email="spine_info@vtt.fi",
    license="LGPL-3.0-or-later",
    packages=find_packages(),
    install_requires=["sqlalchemy>=1.3.17", "alembic", "faker", "python-dateutil", "numpy>=1.8.0"],
    include_package_data=True,
    zip_safe=False,
)
