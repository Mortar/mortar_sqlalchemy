from datetime import datetime
from itertools import tee
from logging import getLogger, WARNING, INFO, DEBUG
from typing import List

from psycopg.types.range import Range as PsycopgRange
from sqlalchemy import Column, CheckConstraint
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import (
    ExcludeConstraint,
    TSRANGE as Range,
)
from sqlalchemy.event import listen
from sqlalchemy.orm import has_inherited_table, Mapped

from itertools import zip_longest

logger = getLogger(__name__)

unset = object()

DateTimeRange = PsycopgRange[datetime]

def _windowed(iterable):
    a, b = tee(iterable)
    next(b, None)
    is_first = True
    for (x, y) in zip_longest(a, b):
        is_last = y is None
        yield x, is_first, is_last
        is_first = False


def starts_before(s_from, o_from):
    return s_from is None or (o_from is not None and s_from < o_from)


def ends_at_or_after(s_to, o_to):
    return s_to is None or s_to == o_to or (o_to is not None and s_to > o_to)


def ends_after(s_to, o_to):
    return o_to is not None and (s_to is None or s_to > o_to)


def earliest(x, y):
    return None if (x is None or y is None) else min(x, y)


def latest(x, y):
    return None if (x is None or y is None) else max(x, y)


def period_str(value_from, value_to) -> str:
    if value_from is None and value_to is None:
        return 'always'
    if value_from is None:
        return 'until %s' % value_to
    if value_to is None:
        return '%s onwards' % value_from
    return '%s to %s' % (value_from, value_to)


class Temporal:
    """
    This mixin takes advantage of the Postgres `range types`__ in order to help store
    tabular information that changes over time.


    __ https://www.postgresql.org/docs/current/rangetypes.html
    """

    #: The names of the columns that uniquely identify this row.
    #: In a non-temporal model, this would be the table's primary key columns.
    key_columns: List[str]

    #: The columns to consider as "values" for this type of object, by default this
    #: is all columns that are not :any:`key_columns` excluding :any:`id` and :any:`period`.
    value_columns: List[str] = None

    #: Controls whether an `exclude constraint`__ is generated for this table, such that
    #: accidental overlaps of data are prevented.
    #:
    #: __ https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRAINT
    exclude_constraint: bool = True

    def __init__(self, **kw):
        value_from = kw.pop('value_from', None)
        value_to = kw.pop('value_to', None)
        if value_from or value_to:
            if 'period' in kw:
                raise TypeError(
                    'period not allowed if value_from or value_to used'
                    )
            period = DateTimeRange(value_from, value_to)
        else:
            period = kw.pop('period', None)
        super(Temporal, self).__init__(period=period, **kw)

    #: The identifier for this row, which is an automatically generated integer
    #: that is unique throughout the whole table.
    id: Mapped[int] = Column(Integer, primary_key=True)
    #: The :class:`~sqlalchemy.schema.Column` specifying the period represented
    #: by this row. When being set, this should be a
    #: :class:`Range[datetime] <psycopg.types.range.Range>`.
    #: When being retrieved, it will likewise be a
    #: :class:`Range[datetime] <psycopg.types.range.Range>`.
    period: Mapped[DateTimeRange] = Column(Range(), nullable=False)

    @classmethod
    def value_at(cls, timestamp: datetime):
        """
        Returns a clause element that returns the current value
        """
        return cls.period.contains(timestamp)

    @property
    def value_from(self) -> datetime:
        """
        A helper for retrieving or setting the lower bound of the :attr:`period`.
        """
        return self.period.lower

    @value_from.setter
    def value_from(self, timestamp):
        if self.period is None:
            upper = None
        else:
            upper = self.period.upper
        self.period = DateTimeRange(timestamp, upper)

    @property
    def value_to(self) -> datetime:
        """
        A helper for retrieving or setting the upper bound of the :attr:`period`.
        """
        return self.period.upper

    @value_to.setter
    def value_to(self, timestamp):
        if self.period is None:
            lower = None
        else:
            lower = self.period.lower
        self.period = DateTimeRange(lower, timestamp)

    @property
    def value_tuple(self) -> tuple[..., ...]:
        """
        Return the tuple representing the :any:`value <set_for_period>` of this object.
        By default, this is constructed from the attributes for all :any:`value_columns`.
        """
        return tuple(getattr(self, col) for col in self.value_columns)

    def period_str(self) -> str:
        """
        Return a human-readable version of the :any:`period` this object is valid.
        """
        if self.period is None:
            return 'unknown'
        value_from = self.period.lower
        value_to = self.period.upper
        return period_str(value_from, value_to)

    @property
    def pretty_key(self) -> str:
        """
        Return a human-readable version of the :attr:`key <key_columns>` for this object.
        """
        return ', '.join('%s=%r' % (k, getattr(self, k))
                         for k in self.key_columns)

    @property
    def pretty_value(self) -> str:
        """
        Return a human-readable version of the :attr:`value <value_columns>` of this object.
        """
        value = self.value_tuple
        if len(value) == 1:
            return value[0]
        return ', '.join('%s=%r' % (k, getattr(self, k))
                         for k in self.value_columns)

    def overlaps(self, session):
        """
        Returns a query for all objects that overlap with this one.
        This means that they are valid for the an overlapping :any:`period`
        and share the same :any:`key <key_columns>`.
        """
        model = type(self)
        return session.query(model).filter_by(
            **{key: getattr(self, key) for key in self.key_columns}
        ).filter(
            model.period.overlaps(self.period)
        ).order_by(
            model.period
        )

    def set_for_period(self, session, coalesce=True,
                       set_logging=INFO,
                       change_logging=WARNING,
                       unchanged_logging=DEBUG):
        """
        Set the temporal value using the :any:`values <value_tuple>` in ``self`` for the
        keys specified in ``self`` over the period specified in ``self``.

        ``self`` should not be attached to any session; the session to the
        database in which changes should be made should be passed as
        ``session``.

        It is assumed that the bounds of periods in the table are ``[)``.

        If ``coalesce`` is True, then where values are the same over a period
        affected by this operation, they will be merged into one database row.

        The case where period is open ended going forwards is special cased
        such that if it would replace future records that have a different
        value, it is instead closed at the point in time where the period of
        the earliest overlapping record ends.
        """

        def log_set(value_from, value_to):
            logger.log(set_logging,
                       '%s from %s set to %s' % (
                       self.pretty_key,
                       period_str(value_from, value_to),
                       self.pretty_value
                       ))

        def log_changed_value(value_from, value_to):
            logger.log(change_logging,
                       '%s from %s changed from %s to %s' % (
                       self.pretty_key,
                       period_str(value_from, value_to),
                       existing.pretty_value,
                       self.pretty_value
                       ))

        def log_changed_period(value_from, value_to):
            logger.log(set_logging,
                       '%s changed period from %s to %s' % (
                       self.pretty_key,
                       existing.period_str(),
                       period_str(value_from, value_to)
                       ))

        def log_unchanged():
            logger.log(unchanged_logging,
                       '%s from %s left at %s' % (
                       self.pretty_key,
                       period_str(self_from, self_to),
                       self.pretty_value
                       ))

        create = True
        self_from = current_from = self.value_from
        self_to = self.value_to

        overlapping = self.overlaps(session)
        if self_to is None and not coalesce:
            overlapping = overlapping.limit(2)

        for existing, is_first, is_last in _windowed(overlapping):

            existing_from = existing.value_from
            existing_to = existing.value_to
            current_starts_before = starts_before(current_from, existing_from)

            if current_starts_before:
                log_set(current_from, existing_from)

            if (
                coalesce and
                self_from != existing_from and
                self.value_tuple == existing.value_tuple
            ):
                self_ends_after = ends_after(self_to, existing_to)
                if current_starts_before or self_ends_after:
                    self_from = earliest(self_from, existing_from)
                    self.value_from = self_from
                    self.value_to = latest(self_to, existing_to)
                    session.delete(existing)
                    session.flush()
                else:
                    log_unchanged()
                    create = False
                current_from = existing_to
                continue

            if self_to is None and (current_starts_before or not is_last):
                if starts_before(self_from, existing_from):
                    self_to = existing_from
                elif self_from == existing_from:
                    self_to = existing_to
                    if self.value_tuple == existing.value_tuple:
                        log_unchanged()
                        create = False
                    else:
                        log_changed_value(self_from, self_to)
                        session.delete(existing)
                        session.flush()
                else:
                    self_to = existing_to
                    log_changed_value(self_from, self_to)
                    existing.value_to = self_from
                self.value_to = self_to
                current_from = self_to
                break

            if current_starts_before:

                if ends_at_or_after(self_to, existing_to):
                    log_changed_value(existing_from, existing_to)
                    session.delete(existing)
                    session.flush()
                else:
                    log_changed_value(existing_from, self_to)
                    existing.value_from = self_to

            elif current_from == existing_from:

                if self_from != current_from and ends_after(existing_to, self_to):
                    existing.value_from = self_to
                else:
                    session.delete(existing)
                    session.flush()

                if self.value_tuple == existing.value_tuple:
                    if self_to == existing_to:
                        log_unchanged()
                    else:
                        log_changed_period(existing_from, self_to)
                        existing_to = self_to
                else:
                    if ends_after(self_to, existing_to):
                        log_changed_value(existing_from, existing_to)
                    else:
                        log_changed_value(existing_from, self_to)

            else:

                if existing_to is None:
                    log_set(self_from, self_to)
                    existing.value_to = self_from
                elif self.value_tuple == existing.value_tuple:
                    log_changed_period(current_from, existing_to)
                    existing.value_from = current_from
                else:
                    log_changed_value(self_from, existing_to)
                    existing.value_to = self_from

            current_from = existing_to

        if ends_after(self_to, current_from):
            log_set(current_from, self_to)

        if create:
            session.add(self)


def add_constraints_and_attributes(mapper, class_):
    if has_inherited_table(class_):
        return
    table = class_.__table__

    if getattr(class_, 'key_columns', None) is not None:
        elements = []
        for col_name in class_.key_columns:
            elements.append((getattr(class_, col_name), '='))
        elements.append(('period', '&&'))
        if class_.exclude_constraint:
            table.append_constraint(ExcludeConstraint(*elements))

        if class_.value_columns is None:
            exclude = {'id', 'period'}
            exclude.update(class_.key_columns)
            class_.value_columns = [c.name for c in table.c
                                    if c.name not in exclude]
    table.append_constraint(CheckConstraint("period != 'empty'::tsrange"))


listen(Temporal, 'instrument_class', add_constraints_and_attributes,
       propagate=True)
