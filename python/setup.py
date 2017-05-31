#!/usr/bin/env python3

from distutils.core import setup

setup(name='vizier',
      version='1.0',
      description='Vizier library',
      author='Paul Glotfelter',
      url='https://github.com/robotarium/vizier',
      packages=['mqtt_interface', 'graph', 'node', 'utils', 'vizier'],
     )
