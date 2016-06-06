
import os
import subprocess
from setuptools import setup, Command
from glob import glob

      
setup(
    name='gwftool',
    version='0.1.dev0',
    packages=[
        'gwftool'
    ],
    entry_points={
      'console_scripts': [
          'gwftool = gwftool.__main__:main'
      ]
    },
    install_requires=['requests', 'galaxy-lib', 'Cheetah'],
    license='Apache',
    long_description=open('README.md').read(),
)
