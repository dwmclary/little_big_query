#! /usr/bin/env python


# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

setup(
    name='LittleBigQuery',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.1.0',

    description='A sample Python project',

    # The project's main homepage.
    url='https://github.com/dwmclary/little_big_query',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['pandas', 'google-api-python-client >= 1.5.1'],
    dependency_links = ["https://github.com/google/google-api-python-client.git"]
    )