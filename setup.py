# Copyright (c) 2015-2019 Chris Withers
# See LICENSE.txt for license details.

import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)

setup(
    name='mortar_mixins',
    author='Chris Withers',
    version='2.3.5',
    author_email='chris@withers.org',
    license='MIT',
    description="SQLAlchemy mixins for use with Postgres.",
    long_description=open(os.path.join(base_dir, 'README.rst')).read(),
    url='https://github.com/Mortar/mortar_mixins',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires = (
        'SQLAlchemy',
        'psycopg2',
        ),
    extras_require=dict(
        test=['pytest', 'pytest-cov', 'mortar_rdb', 'testfixtures', 'coveralls'],
        build=['setuptools-git', 'wheel', 'twine']
        ),
    )

