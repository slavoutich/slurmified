#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='slurmified',
      version='0.1.0',
      description='Dask.Didtributed on SLURM-managed clusters',
      url='https://gitlab.kwant-project.org/slavoutich/slurmified/',
      maintainer='Viacheslav Ostroukh',
      maintainer_email='v.dev@ostroukh.me',
      license='BSD 2-clause',
      keywords='',
      packages=['slurmified'],
      install_requires=list(open('requirements.txt')
                            .read()
                            .strip()
                            .split('\n')),
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      zip_safe=False)
