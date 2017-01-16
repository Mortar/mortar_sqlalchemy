# Copyright (c) 2015 Chris Withers
# See README.md for license details.

import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)

setup(
    name='mortar_mixins',
    author='Chris Withers',
    version='2.2',
    author_email='chris@withers.org',
    license='MIT',
    description="SQLAlchemy mixins for use with Postgres.",
    long_description=open(os.path.join(base_dir, 'README.rst')).read(),
    url='https://github.com/Mortar/mortar_mixins',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires = (
        'mortar_rdb',
        'SQLAlchemy',
        'psycopg2',
        ),
    extras_require=dict(
        test=['nose', 'nose-cov','testfixtures', 'coveralls'],
        build=['setuptools-git', 'wheel', 'twine']
        ),
    )

