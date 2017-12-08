from logging import getLogger, WARNING, INFO, DEBUG

from psycopg2.extras import DateTimeRange
from sqlalchemy import Column, CheckConstraint, and_
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import (
    ExcludeConstraint,
    TSRANGE as Range,
    )
from sqlalchemy.event import listen
from sqlalchemy.ext.declarative import has_inherited_table


logger = getLogger(__name__)

unset = object()


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
        if value_from is None and value_to is None:
            return 'always'
        if self.value_from is None:
            return 'until %s' % self.value_to
        if self.value_to is None:
            return '%s onwards' % self.value_from
        return '%s to %s' % (self.value_from, self.value_to)

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
        return self.value_from is None or (
            other.value_from is not None and self.value_from < other.value_from
        )

    def starts_before_or_at(self, other):
        return self.value_from is None or (
            other.value_from is not None and self.value_from <= other.value_from
        )

    def ends_after(self, other):
        return other.value_to is not None and (
                self.value_to is None or (self.value_to > other.value_to)
        )

    def period_compatible_with(self, other):
        return self.period.lower == other.period.lower and (
            self.period.upper is None or
            self.period.upper==other.period.upper
        )

    def overlaps_filter(self):
        cls = type(self)
        # based on this instance:
        return and_(*(getattr(cls, key) == getattr(self, key)
                      for key in self.key_columns))

    def set_for_period(self, session, coalesce=True,
                       add_logging=INFO,
                       change_logging=WARNING,
                       delete_logging=WARNING,
                       unchanged_logging=DEBUG):
        create = True
        old = unset
        new_value = self.value_tuple
        model = type(self)

        for index, existing in enumerate(session.query(model).filter(
            self.overlaps_filter()
        ).filter(
            model.period.overlaps(self.period)
        ).order_by(
            model.period
        )):

            if index:
                add_logging = change_logging

            existing_value = existing.value_tuple
            value_same = existing_value == new_value

            if coalesce and value_same:
                if existing.starts_before_or_at(self):
                    existing.value_to = latest_ending(self, existing)
                    create = False
                    obj = existing
                    logger.log(add_logging,
                               '%s now from %s',
                               self.pretty_key,
                               obj.period_str())
                else:
                    # XXX coalesce multiple gets it wrong as we set
                    # self here? but existing is the one that will be used?
                    self.value_to = latest_ending(self, existing)
                    obj = self
                    change_logging = add_logging
                    session.delete(existing)
                    session.flush()
                    if index:
                        logger.log(add_logging,
                                   'deleted %s from %s, was set to %s',
                                   existing.pretty_key,
                                   existing.period_str(),
                                   existing.pretty_value)
                    else:
                        old = existing

            elif value_same and self.period_compatible_with(existing):
                create = False
                logger.log(unchanged_logging,
                           '%s from %s left at %s',
                           self.pretty_key,
                           self.period_str(),
                           self.pretty_value)
            else:
                if existing.starts_before(self):
                    existing.value_to = self.value_from
                else:
                    if self.value_to is None and self.starts_before(existing):
                        self.value_to = existing.value_from
                        add_logging = change_logging
                        break
                    elif existing.ends_after(self):
                        existing.value_from = self.value_to
                        old = existing
                    else:
                        session.delete(existing)
                        session.flush()
                        if index:
                            logger.log(delete_logging,
                                       'deleted %s from %s, was set to %s',
                                       existing.pretty_key,
                                       existing.period_str(),
                                       existing.pretty_value)
                        else:
                            old = existing

        if create:
            session.add(self)
            if old is unset:
                logger.log(add_logging,
                           '%s from %s set to %s',
                           self.pretty_key,
                           self.period_str(),
                           self.pretty_value)
            else:
                if old.value_tuple == self.value_tuple:
                    logger.log(change_logging,
                               '%s changed period from %s to %s',
                               self.pretty_key,
                               old.period_str(),
                               self.period_str())
                else:
                    logger.log(change_logging,
                               '%s from %s changed from %s to %s',
                               self.pretty_key,
                               self.period_str(),
                               old.pretty_value,
                               self.pretty_value)


def latest_ending(x, other):
    x_to = x.value_to
    y_to = other.value_to
    if x_to is None or y_to is None:
        return None
    return max(x_to, y_to)


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
