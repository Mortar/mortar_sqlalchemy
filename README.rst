mortar_mixins
=============

SQLAlchemy mixins for use with Postgres.

Install from PyPI with pip.

Development
-----------

Get a clone of the git repo and then do the following::

  virtualenv .
  bin/py install -e .[build,test]
  
  sudo -u postgres psql -d postgres -c "create user testuser with password 'testpassword';"
  sudo -u postgres createdb -O testuser testdb
  sudo -u postgres psql -d testdb -c "CREATE EXTENSION btree_gist;"

  export DB_URL=postgres://testuser:testpassword@localhost:5432/testdb
  bin/nosetests --with-cov --cov=mortar_mixins

Releasing
---------

::

  $ bin/pip install -e .[test,build]
  $ bin/python setup.py sdist bdist_wheel
  $ bin/twine upload dist/mortar_mixins-<version>*

