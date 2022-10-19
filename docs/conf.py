# -*- coding: utf-8 -*-
import os, pkg_resources, datetime, time

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'testfixures': ('https://testfixtures.readthedocs.io/en/latest/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/20', None),
    'psycopg': ('https://www.psycopg.org/psycopg3/docs/', None),
}

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx'
    ]

# General
source_suffix = '.rst'
master_doc = 'index'
project = 'mortar_sqlalchemy'
build_date = datetime.datetime.utcfromtimestamp(int(os.environ.get('SOURCE_DATE_EPOCH', time.time())))
copyright = '2015-%s Chris Withers' % build_date.year
version = release = pkg_resources.get_distribution(project).version
exclude_patterns = [
    '_build',
    'example*',
]
pygments_style = 'sphinx'

# Options for HTML output
html_theme = 'furo'
html_title = 'mortar_sqlalchemy'
htmlhelp_basename = project+'doc'

# Options for LaTeX output
latex_documents = [
  ('index',project+'.tex', project+u' Documentation',
   'Chris Withers', 'manual'),
]

autodoc_member_order = 'bysource'
nitpicky = True
nitpick_ignore = []
toc_object_entries = False
