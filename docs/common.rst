Common Mixin
============

This mixin provides a common set of functionality that is slightly opinionated.
Here's an example usage:

.. code-block:: python

  from sqlalchemy import Column, Integer
  from sqlalchemy.orm import declarative_base
  from mortar_sqlalchemy.mixins import Common


  Base = declarative_base()

  class SampleModel(Common, Base):
    id = Column(Integer, primary_key=True)
    value = Column(Integer)

This mixin provides:

- An automatically generated table name:

  >>> SampleModel.__tablename__
  'sample_model'

- Equality and inequality based on comparison of the column values
  in the mapped object:

  >>> SampleModel(id=1, value=2) == SampleModel(id=1, value=2)
  True
  >>> SampleModel(id=1, value=2) == SampleModel(id=3, value=4)
  False

- Hashability based on the Python identity of the object:

  >>> s = set()
  >>> s.add(SampleModel(id=1, value=2))
  >>> s.add(SampleModel(id=1, value=2))
  >>> s
  {SampleModel(id=1, value=2), SampleModel(id=1, value=2)}

- String representation showing attribute names and values:

  >>> str(SampleModel(id=1, value=2))
  'SampleModel(id=1, value=2)'
  >>> repr(SampleModel(id=3, value=4))
  'SampleModel(id=3, value=4)'

It also registers a :func:`testfixtures.compare` comparer that uses the same
equality and inequality as provided by the mixin, along with the ability to
ignore certain columns:

>>> from testfixtures import compare
>>> compare(expected=SampleModel(id=1, value=2), actual=SampleModel(id=2, value=2))
Traceback (most recent call last):
...
AssertionError: SampleModel not as expected:
<BLANKLINE>
same:
['value']
<BLANKLINE>
values differ:
'id': 1 (expected) != 2 (actual)
>>> compare(
...     expected=SampleModel(id=1, value=2),
...     actual=SampleModel(id=2, value=2),
...     ignore_fields=['id'],
... )
