Temporal Mixin
==============

This mixin takes advantage of the Postgres `range types`__ in order to help store
tabular information that changes over time.


__ https://www.postgresql.org/docs/current/rangetypes.html

There's no documentation for these, but the unit tests don't provide a terrible substitute:

.. literalinclude:: ../tests/test_temporal.py
  :language: python

