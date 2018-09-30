from setuptools import setup, find_packages

setup(
    name='spinedatabase_api',
    version='0.0.4',
    description='An API to access Spine databases',
    url='https://gitlab.vtt.fi/spine/data',
    author='Manuel Marin',
    author_email='manuelma@kth.se',
    license='LGPL',
    packages=find_packages(),
    install_requires=[
          'sqlalchemy',
      ],
    zip_safe=False
)
