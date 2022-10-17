# -*- coding: utf-8 -*-
import datetime
import os
import time

import pkg_resources

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
build_date = datetime.datetime.utcfromtimestamp(int(os.environ.get('SOURCE_DATE_EPOCH', time.time())))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx'
    ]

intersphinx_mapping = {
    'https://docs.python.org/3/': None,
}

# General
source_suffix = '.rst'
master_doc = 'index'
project = 'mortar_sqlalchemy'
copyright = '2015-%s Chris Withers' % build_date.year
version = release = pkg_resources.get_distribution(project).version
exclude_trees = ['_build']
pygments_style = 'sphinx'

# Options for HTML output
html_theme = 'furo'
htmlhelp_basename = project+'doc'

# Options for LaTeX output
exclude_patterns = ['**/furo.js.LICENSE.txt']

autodoc_member_order = 'bysource'
