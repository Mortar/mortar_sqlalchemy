from itertools import tee
from logging import getLogger, WARNING, INFO, DEBUG, ERROR

from psycopg2.extras import DateTimeRange
from sqlalchemy import Column, CheckConstraint, and_
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import (
    ExcludeConstraint,
    TSRANGE as Range,
    )
from sqlalchemy.event import listen
from sqlalchemy.ext.declarative import has_inherited_table
from .compat import zip_longest

logger = getLogger(__name__)

unset = object()


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    for (x, y) in zip_longest(a, b):
        yield x, y


def latest_ending(x, other):
    x_to = x.value_to
    y_to = other.value_to
    if x_to is None or y_to is None:
        return None
    return max(x_to, y_to)


def _unchanged(log_level, existing):
    logger.log(log_level,
               '%s from %s left at %s',
               existing.pretty_key,
               existing.period_str(),
               existing.pretty_value)
    return False


def period_str(value_from, value_to):
    if value_from is None and value_to is None:
        return 'always'
    if value_from is None:
        return 'until %s' % value_to
    if value_to is None:
        return '%s onwards' % value_from
    return '%s to %s' % (value_from, value_to)


class Temporal(object):

    key_columns = None
    value_columns = None

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

    id = Column(Integer, primary_key=True)
    period = Column(Range(), nullable=False)

    @classmethod
    def value_at(cls, timestamp):
        """
        Returns a clause element that returns the current value
        """
        return cls.period.contains(timestamp)

    @property
    def value_from(self):
        return self.period.lower

    @value_from.setter
    def value_from(self, timestamp):
        if self.period is None:
            upper = None
        else:
            upper = self.period.upper
        self.period = DateTimeRange(timestamp, upper)
        
    @property
    def value_to(self):
        return self.period.upper

    @value_to.setter
    def value_to(self, timestamp):
        if self.period is None:
            lower = None
        else:
            lower = self.period.lower
        self.period = DateTimeRange(lower, timestamp)

    @property
    def value_tuple(self):
        return tuple(getattr(self, col) for col in self.value_columns)

    def period_str(self):
        if self.period is None:
            return 'unknown'
        value_from = self.period.lower
        value_to = self.period.upper
        return period_str(value_from, value_to)

    @property
    def pretty_key(self):
        return ', '.join('%s=%r' % (k, getattr(self, k))
                         for k in self.key_columns)

    @property
    def pretty_value(self):
        value = self.value_tuple
        if len(value) == 1:
            return value[0]
        return ', '.join('%s=%r' % (k, getattr(self, k))
                         for k in self.value_columns)

    def starts_before(self, other):
        s_from = self.value_from
        o_from = other.value_from
        return s_from is None or (o_from is not None and s_from < o_from)

    def starts_at(self, other):
        return self.value_from == other.value_from

    def starts_after(self, other):
        s_from = self.value_from
        o_from = other.value_from
        return o_from is not None and (s_from is None or s_from < o_from)
    #
    # def starts_before_or_at(self, other):
    #     s_from = self.value_from
    #     o_from = other.value_from
    #     return s_from is None or (o_from is not None and s_from <= o_from)

    def ends_at(self, other):
        return self.value_to == other.value_to

    def ends_after(self, other):
        s_to = self.value_to
        o_to = other.value_to
        return s_to is None or (o_to is not None and s_to > o_to)
    #
    # def period_contains(self, other):
    #     return self.starts_before_or_at(other) and self.ends_after(other)
    #
    # def period_compatible_with(self, other):
    #     s_from = self.value_from
    #     o_from = other.value_from
    #     s_to = self.value_to
    #     o_to = other.value_to
    #     return s_from == o_from and (
    #         s_to is None or o_to is None or s_to == o_to
    #     )

    def overlaps(self, session):
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
        Set the temporal value using the values in ``self`` for the
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
                       '%s from %s set to %s',
                       self.pretty_key,
                       period_str(value_from, value_to),
                       self.pretty_value)

        def log_changed_value(value_from, value_to):
            logger.log(change_logging,
                       '%s from %s changed from %s to %s',
                       self.pretty_key,
                       period_str(value_from, value_to),
                       existing.pretty_value,
                       self.pretty_value)

        def log_changed_period(value_from, value_to):
            logger.log(set_logging,
                       '%s changed period from %s to %s',
                       self.pretty_key,
                       existing.period_str(),
                       period_str(value_from, value_to))

        def log_unchanged():
            logger.log(unchanged_logging,
                       '%s from %s left at %s',
                       self.pretty_key,
                       self.period_str(),
                       self.pretty_value)

        first = True
        create = True
        existing = None
        self_from = self.value_from
        self_to = self.value_to

        overlapping = self.overlaps(session)
        if self_to is None:
            overlapping = overlapping.limit(2)

        for existing, next_existing in pairwise(overlapping):
            last = next_existing is None

            existing_from = existing.value_from
            existing_to = existing.value_to

            if first:
                first = False

                if self.starts_before(existing):
                    if self.value_to is None:
                        self.value_to = existing_from
                        log_set(self_from, existing_from)
                        break
                    else:
                        log_set(self_from, existing_from)
                        log_changed_value(existing_from, self_to)
                        existing.value_from = self_to
                elif self.starts_at(existing):
                    if self_to is None and not last:
                        self.value_to = self_to = existing.value_to
                    if self.period == existing.period:
                        if self.value_tuple == existing.value_tuple:
                            log_unchanged()
                            create = False
                        else:
                            log_changed_value(self_from, self_to)
                            session.delete(existing)
                            session.flush()
                        break
                    else:
                        if self.value_tuple == existing.value_tuple:
                            log_changed_period(self_from, self_to)
                        else:
                            log_changed_value(self_from, self_to)
                        session.delete(existing)
                        session.flush()
                else:
                    if existing_to is None:
                        log_set(self_from, self_to)
                    else:
                        log_changed_value(self_from, existing_to)
                        if last:
                            log_set(existing_to, self_to)
                    existing.value_to = self_from

            elif self_to is None:

                self.value_to = existing_from
                break

            elif last:

                if self.ends_at(existing):
                    log_changed_value(existing_from, existing_to)
                    session.delete(existing)
                    session.flush()
                elif self.ends_after(existing):
                    log_changed_value(existing_from, existing_to)
                    log_set(existing_to, self_to)
                    session.delete(existing)
                    session.flush()
                else:
                    log_changed_value(existing_from, self_to)
                    existing.value_from = self_to

            else:

                log_changed_value(existing_from, existing_to)
                session.delete(existing)
                session.flush()

        if create:
            session.add(self)
            if existing is None:
                log_set(self_from, self_to)


def add_constraints_and_attributes(mapper, class_):
    if has_inherited_table(class_):
        return
    table = class_.__table__

    if class_.key_columns is not None:
        elements = []
        for col_name in class_.key_columns:
            elements.append((getattr(class_, col_name), '='))
        elements.append(('period', '&&'))
        table.append_constraint(ExcludeConstraint(*elements))

        if class_.value_columns is None:
            exclude = {'id', 'period'}
            exclude.update(class_.key_columns)
            class_.value_columns = [c.name for c in table.c
                                    if c.name not in exclude]
    table.append_constraint(CheckConstraint("period != 'empty'::tsrange"))


listen(Temporal, 'instrument_class', add_constraints_and_attributes,
       propagate=True)
