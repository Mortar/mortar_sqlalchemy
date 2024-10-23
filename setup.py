# Copyright (c) 2015 onwards Chris Withers
# See LICENSE.txt for license details.

import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)

setup(
    name='mortar_sqlalchemy',
    author='Chris Withers',
    version='4.0.0',
    author_email='chris@withers.org',
    license='MIT',
    description="Mixins, Helpers and patterns for SQLAlchemy.",
    long_description=open(os.path.join(base_dir, 'README.rst')).read(),
    url='https://github.com/Mortar/mortar_sqlalchemy',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.11",
    install_requires=(
        'SQLAlchemy>=2',
        'psycopg>=3',
    ),
    extras_require=dict(
        test=[
            'pytest',
            'pytest-cov',
            'sybil',
            'testfixtures',
            'testservices >= 0.3',
        ],
        build=[
            'setuptools',
            'setuptools-git',
            'wheel',
            'twine',
        ],
        docs=[
            'sphinx',
            'furo',
        ],
    ),
)

