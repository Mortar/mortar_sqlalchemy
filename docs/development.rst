Development
===========

.. highlight:: bash

If you wish to contribute to this project, then you should fork the
repository found here:

https://github.com/mortar/mortar_sqlalchemy/

Once that has been done and you have a checkout, you can follow these
instructions to perform various development tasks:

Setting your environment
-------------------------

The recommended way to set up a development environment is to create
a virtualenv and then install the package in editable form as follows::

  $ python3 -m venv ~/virtualenvs/mortar_sqlalchemy
  $ source ~/virtualenvs/mortar_sqlalchemy/bin/activate
  $ pip install -U pip setuptools
  $ pip install -U -e .[test,build]

You'll also need a Postgres database in order to run the tests::

  sudo -u postgres psql -d postgres -c "create user testuser with password 'testpassword';"
  sudo -u postgres createdb -O testuser testdb
  sudo -u postgres psql -d testdb -c "CREATE EXTENSION btree_gist;"

  export DB_URL=postgres://testuser:testpassword@localhost:5432/testdb

Running the tests
-----------------

Once you've set up a virtualenv, the tests can be run in the activated
virtualenv and from the root of a source checkout as follows::

  $ pytest

Building the documentation
--------------------------

The Sphinx documentation is built by doing the following from the
directory containing ``setup.py``::

  $ cd docs
  $ make html

To check that the description that will be used on PyPI renders properly,
do the following::

  $ python setup.py --long-description | rst2html.py > desc.html

The resulting ``desc.html`` should be checked by opening in a browser.

Making a release
----------------

To make a release, just update the version in ``setup.py``, update the change log
and push to https://github.com/mortar/mortar_sqlalchemy.
Carthorse should take care of the rest.
