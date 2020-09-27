#!/usr/bin/env python

import os, glob
from setuptools import setup, find_packages

setup(
    name='minidb',
    version='2.0.1',
    url='https://github.com/kunyo/sqlite-minidb',
    license='UNLICENSED',
    author='KN',
    author_email='',
    description='',
    long_description='',
    packages=find_packages(exclude=['test']),
    include_package_data=True,
    platforms=['MacOS X', 'Posix'],
    test_suite='test',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: UNLICENSED',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 5 - Production/Stable',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    dependency_links=[],
    install_requires=[]
)