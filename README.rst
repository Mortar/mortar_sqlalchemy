|CircleCI|_

.. |CircleCI| image:: https://circleci.com/gh/Mortar/mortar_mixins/tree/master.svg?style=shield
.. _CircleCI: https://circleci.com/gh/Mortar/mortar_mixins/tree/master

mortar_mixins
=============

SQLAlchemy mixins for use with Postgres 9.2+.

Install from PyPI with pip.

Development
-----------

Get a clone of the git repo and then do the following::

  virtualenv .
  bin/pip install -e .[build,test]
  
  sudo -u postgres psql -d postgres -c "create user testuser with password 'testpassword';"
  sudo -u postgres createdb -O testuser testdb
  sudo -u postgres psql -d testdb -c "CREATE EXTENSION btree_gist;"

  export DB_URL=postgres://testuser:testpassword@localhost:5432/testdb
  pytest --cov

Releasing
---------

To make a release, just update the version in ``setup.py``
and push to https://github.com/Mortar/mortar_mixins
and Carthorse should take care of the rest.
