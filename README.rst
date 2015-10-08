|Travis|_ |Coveralls|_

.. |Travis| image:: https://api.travis-ci.org/Mortar/mortar_mixins.png?branch=master
.. _Travis: https://travis-ci.org/Mortar/mortar_mixins

.. |Coveralls| image:: https://coveralls.io/repos/Mortar/mortar_mixins/badge.png?branch=master
.. _Coveralls: https://coveralls.io/r/Mortar/mortar_mixins?branch=master

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
  bin/nosetests --with-cov --cov=mortar_mixins

Releasing
---------

To make a release, just update the version in ``setup.py``, tag it
and push to https://github.com/Mortar/mortar_mixins
and Travis CI should take care of the rest.


