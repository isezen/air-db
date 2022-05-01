"""
airdb package
~~~~~~~~~~~~~~~~~~~~~
An interface easily to access environmental time series datasets
from various sources
"""

# pylint: disable=C0103
import os
from setuptools import setup


def get(x):
    """ Get something """
    with open(os.path.join(os.path.dirname(__file__),
                           'airdb', '__init__.py')) as f:
        for line in f.readlines():
            if line.startswith('__' + x + '__'):
                return line.split('=')[1].strip()[1:-1]
    return None


setup(
    name='air-db',
    version=get('version'),
    platforms=['linux', 'darwin', 'windows'],
    packages=['airdb'],
    package_dir={'airdb': 'airdb'},
    include_package_data=True,
    package_data={'airdb': ['data/README.md']},
    setup_requires=['pytest-runner'],
    install_requires=['pandas', 'numpy', 'xarray'],
    tests_require=['pytest'],
    author=get('author'),
    author_email=get('email'),
    description='A data access layer for atmospheric time series datasets',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    license=get('license'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
    ],
    python_requires=">=3.6",
    keywords=['data', 'environment', 'pollutant', 'meteorology', 'turkey'],
    url='https://github.com/isezen/air-db',
    download_url='https://pypi.org/project/air-db/#files',
    project_urls={
        'Bug Tracker': 'https://github.com/isezen/air-db/issues',
        'Documentation': 'https://github.com/isezen/air-db/wiki',
        'Source Code': 'https://github.com/isezen/air-db'})
