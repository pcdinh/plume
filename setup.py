#!/usr/bin/env python

from setuptools import setup, find_packages

import os
execfile(os.path.join('plume', 'version.py'))

setup(
    name = 'plume',
    version = VERSION,
    description = 'Plume is a library for working with Flume',
    author = 'Samuel Stauffer',
    author_email = 'samuel@descolada.com',
    url = 'http://github.com/samuel/plume',
    packages = find_packages(),
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires = [
        'thrift',
    ],
)
