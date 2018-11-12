from setuptools import setup, find_packages
import spinedatabase_api

setup(
    name='spinedatabase_api',
    version=spinedatabase_api.__version__,
    description='An API to talk to Spine databases',
    url='https://github.com/Spine-project/Spine-Database-API',
    author='Manuel Marin, Per Vennström',
    author_email='manuelma@kth.se',
    license='LGPL',
    packages=find_packages(),
    install_requires=[
          'sqlalchemy',
      ],
    zip_safe=False
)
