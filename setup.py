from setuptools import setup, find_packages

version = {}
with open("spinedb_api/version.py") as fp:
    exec(fp.read(), version)

setup(
    name="spinedb_api",
    version=version["__version__"],
    description="An API to talk to Spine databases",
    url="https://github.com/Spine-project/Spine-Database-API",
    author="Manuel Marin, Per Vennström, Fabiano Pallonetto",
    author_email="manuelma@kth.se",
    license="LGPL",
    packages=find_packages(),
    install_requires=["sqlalchemy>=1.3", "alembic", "faker"],
    include_package_data=True,
    zip_safe=False,
)
