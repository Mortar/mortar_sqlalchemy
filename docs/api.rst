API Reference
=============

.. currentmodule:: mortar_sqlalchemy


Mixins
------

.. autoclass:: Common
    :members:
    :special-members:
    :exclude-members: __ne__,__weakref__

    .. https://github.com/sqlalchemy/sqlalchemy/discussions/8682

    .. attribute:: __tablename__

       The table name is, by default, the snake-case version of the class name.

.. autoclass:: Temporal
    :members:

    .. method:: __init__(period: Range[datetime] = None, value_from: datetime = None, value_to: datetime = None, ...)

        This behaves the same as the normal constructor for a mapped object, but the
        :attr:`period` may alternatively be set by passing either or both of
        :attr:`value_from` and :attr:`value_to`.

.. _testing-api:

Testing
-------

.. automodule:: mortar_sqlalchemy.testing
    :members:
