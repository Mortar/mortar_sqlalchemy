from psycopg2.extras import DateTimeRange
from sqlalchemy import Column, CheckConstraint
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import (
    ExcludeConstraint,
    TSRANGE as Range,
    )
from sqlalchemy.event import listen


class Temporal(object):

    key_columns = None

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


def add_constraints(mapper, class_):
    if mapper.polymorphic_map and not mapper.polymorphic_on:
        return
    table = class_.__table__
    if class_.key_columns is not None:
        elements = []
        for col_name in class_.key_columns:
            elements.append((getattr(class_, col_name), '='))
        elements.append(('period', '&&'))
        table.append_constraint(ExcludeConstraint(*elements))
    table.append_constraint(CheckConstraint("period != 'empty'::tsrange"))

listen(Temporal, 'instrument_class', add_constraints, propagate=True)
