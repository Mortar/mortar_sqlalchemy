|CircleCI|_

.. |CircleCI| image:: https://circleci.com/gh/Mortar/mortar_sqlalchemy/tree/master.svg?style=shield
.. _CircleCI: https://circleci.com/gh/Mortar/mortar_sqlalchemy/tree/master

mortar_sqlalchemy
=================

Mixins, Helpers and patterns for SQLAlchemy.

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
and push to https://github.com/Mortar/mortar_sqlalchemy
and Carthorse should take care of the rest.
