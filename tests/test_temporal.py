from datetime import datetime as dt

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String
from testfixtures import ShouldRaise, compare, LogCapture

from mortar_sqlalchemy import Common, Temporal
from mortar_sqlalchemy.mixins.temporal import DateTimeRange as Range
from mortar_sqlalchemy.testing import create_tables_and_session


@pytest.fixture()
def model(base):
    class Model(Temporal, Common, base):
        key_columns = ['name']
        name = Column(String)

    return Model


class TestProperties:

    @pytest.fixture()
    def obj(self, model):
        return model(
            name='test',
            period=Range(dt(2000, 1, 1), dt(2001, 1, 1))
        )
        
    def test_get_from(self, obj):
        compare(obj.value_from, dt(2000, 1, 1))
    
    def test_set_from(self, obj):
        obj.value_from = dt(1999, 1, 1)
        compare(obj.period,
                Range(dt(1999, 1, 1), dt(2001, 1, 1)))
    
    def test_set_from_none(self, model):
        obj = model()
        obj.value_from = dt(1999, 1, 1)
        compare(obj.period,
                Range(dt(1999, 1, 1), None))
    
    def test_get_to(self, obj):
        compare(obj.value_to, dt(2001, 1, 1))
    
    def test_set_to(self, obj):
        obj.value_to = dt(2002, 1, 1)
        compare(obj.period,
                Range(dt(2000, 1, 1), dt(2002, 1, 1)))
    
    def test_set_to_none(self, model):
        obj = model()
        obj.value_to = dt(1999, 1, 1)
        compare(obj.period,
                Range(None, dt(1999, 1, 1)))


class TestConstructor:

    def test_from(self, model):
        obj = model(name = 'test', value_from = dt(2000, 1, 1))
        compare(obj.period, Range(dt(2000, 1, 1), None))

    def test_to(self, model):
        obj = model(name = 'test', value_to = dt(2000, 1, 1))
        compare(obj.period, Range(None, dt(2000, 1, 1)))

    def test_period_and_from(self, model):
        with ShouldRaise(TypeError(
                'period not allowed if value_from or value_to used'
        )):
            model(
                name = 'test',
                period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                value_from = dt(2000, 1, 1),
            )

    def test_period_and_to(self, model):
        with ShouldRaise(TypeError(
                'period not allowed if value_from or value_to used'
        )):
            model(
                name = 'test',
                period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                value_to = dt(2000, 1, 1),
            )


class TestValueAt:

    @pytest.fixture(autouse=True)
    def objects(self, connection, base, model):
        with create_tables_and_session(connection, base) as session:
            session.add(model(
                    period=Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                    name='Name 1-1'
                    ))
            session.add(model(
                    period=Range(dt(2001, 1, 1), dt(2002, 1, 1)),
                    name='Name 1-2'
                    ))
            session.add(model(
                    period=Range(dt(2002, 1, 1), None),
                    name='Name 1-3'
                    ))
            self.Model = model
            self.session = session
            yield

    def _check(self, dt, expected):
        objs = self.session.query(self.Model).filter(
            self.Model.value_at(dt)
            ).all()
        actual = [o.name for o in objs]
        compare(expected, actual)
        
    def test_nothing(self):
        self._check(dt(1999, 2, 3), [])
        
    def test_middle(self):
        self._check(dt(2000, 2, 3), ['Name 1-1'])
        self._check(dt(2001, 2, 3), ['Name 1-2'])
        self._check(dt(2002, 2, 3), ['Name 1-3'])

    def test_boundaries(self):
        self._check(dt(2000, 1, 1), ['Name 1-1'])
        self._check(dt(2001, 1, 1), ['Name 1-2'])
        self._check(dt(2002, 1, 1), ['Name 1-3'])


class TestConstraints:

    @pytest.fixture(autouse=True)
    def setup(self, connection, base, model):
        self.Model = model
        with create_tables_and_session(connection, base) as session:
            self.session = session
            yield

    def _check_valid(self, existing=(), new=(), exception=None):
        # both existing and new should be sequences of two-tuples
        # of the form (value_from,value_to)
        for value_from, value_to in existing:
            self.session.add(self.Model(
                    period=Range(value_from, value_to),
                    name='Name'
                    ))

        for value_from, value_to in new:
            self.session.add(self.Model(
                    period=Range(value_from, value_to),
                    name='Name'
                    ))
        if exception:
            with ShouldRaise(exception):
                self.session.flush()
        else:
            self.session.flush()

    def test_invalid_1(self):
        # existing:     |---->
        #      new:  |------->
        self._check_valid(
            existing = [(dt(2001, 1, 1), None)],
            new =      [(dt(2002, 1, 1), None)],
            exception = IntegrityError
            )

    def test_invalid_2(self):
        # existing:  |------->
        #      new:     |---->
        self._check_valid(
            existing = [(dt(2001, 1, 1), None)],
            new =      [(dt(2002, 1, 1), None)],
            exception = IntegrityError
            )

    def test_invalid_3(self):
        # existing:  |------->
        #      new:  |------->
        self._check_valid(
            existing = [(dt(2002, 1, 1), None)],
            new =      [(dt(2002, 1, 1), None)],
            exception = IntegrityError
            )

    def test_invalid_4(self):
        # existing:  |-------|
        #      new:  |-------|
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2002, 1, 1))],
            new =      [(dt(2001, 1, 1), dt(2002, 1, 1))],
            exception = IntegrityError
            )
        
    def test_invalid_5(self):
        # existing:  |-------|
        #      new:  |-----|
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2001, 1, 1), dt(2002, 1, 1))],
            exception = IntegrityError
            )

    def test_invalid_6(self):
        # existing:  |-------|
        #      new:    |-----|
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2002, 1, 1), dt(2003, 1, 1))],
            exception=IntegrityError
            )

    def test_invalid_7(self):
        # existing:  |-------|
        #      new:    |---|
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2004, 1, 1))],
            new =      [(dt(2002, 1, 1), dt(2003, 1, 1))],
            exception = IntegrityError
            )

    def test_invalid_8(self):
        # existing:    |---|
        #      new:  |-------|
        self._check_valid(
            existing = [(dt(2002, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2001, 1, 1), dt(2004, 1, 1))],
            exception=IntegrityError
            )

    def test_invalid_9(self):
        #      new:  |<------|
        self._check_valid(
            new = [(dt(2004, 1, 1), dt(2001, 1, 1))],
            exception=DataError
            )

    def test_invalid_11(self):
        # existing:     |---->
        #      new:  |-------|
        self._check_valid(
            existing = [(dt(2001, 1, 1), None)],
            new =      [(dt(2000, 1, 1), dt(2002, 1, 1))],
            exception = IntegrityError
            )

    def test_invalid_12(self):
        # existing:  |-------|
        #      new:     |---->
        self._check_valid(
            existing = [(dt(2000, 1, 1), dt(2002, 1, 1))],
            new =      [(dt(2001, 1, 1), None)],
            exception = IntegrityError
            )

    def test_invalid_13(self):
        #      new:  |
        self.session.add(self.Model(
                period = Range(dt(2004, 1, 1), dt(2004, 1, 1), '()'),
                name = 'Name'
                ))
        with ShouldRaise(IntegrityError):
            self.session.flush()
        
    def test_invalid_14(self):
        #      new:  null
        self.session.add(self.Model(
                name = 'Name'
                ))
        # SQLAlchemy used to raise an IntegrityError here
        # 1.10 onwards raises a CompileError
        with ShouldRaise():
            self.session.flush()
        
    def test_ok_1(self):
        # existing:  |---|
        #      new:      |--->
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2002, 1, 1))],
            new =      [(dt(2002, 1, 1), None)],
            )

    def test_ok_2(self):
        # existing:  |---|
        #      new:      |---|
        self._check_valid(
            existing = [(dt(2001, 1, 1), dt(2002, 1, 1))],
            new =      [(dt(2002, 1, 1), dt(2003, 1, 1))],
            )

    def test_ok_3(self):
        # existing:      |---|
        #      new:  |---|
        self._check_valid(
            existing = [(dt(2002, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2001, 1, 1), dt(2002, 1, 1))],
            )

    def test_ok_4(self):
        # existing:        |---|
        #      new:  |---|
        self._check_valid(
            existing = [(dt(2002, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2000, 1, 1), dt(2001, 1, 1))],
            )

    def test_ok_5(self):
        # existing:  |---|
        #      new:        |---|
        self._check_valid(
            existing = [(dt(2000, 1, 1), dt(2001, 1, 1))],
            new =      [(dt(2002, 1, 1), dt(2003, 1, 1))],
            )

    def test_ok_6(self):
        # existing:  |---|   |---|
        #      new:      |---|
        self._check_valid(
            existing = [(dt(2000, 1, 1), dt(2001, 1, 1)),
                        (dt(2002, 1, 1), dt(2003, 1, 1))],
            new =      [(dt(2001, 1, 1), dt(2002, 1, 1))],
            )

    def test_ok_7(self):
        #      new:  <-------|
        self._check_valid(new=[(None, dt(2004, 1, 1))])

    def test_invalid_update_instance(self):
        # add
        obj1 = self.Model(
            period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
            name = 'Name'
            )
        obj2 = self.Model(
            period = Range(dt(2001, 1, 1), None),
            name = 'Name'
            )
        self.session.add(obj1)
        self.session.add(obj2)
        self.session.flush()
        # change to be invalid
        obj1.period = Range(dt(2000, 1, 1), dt(2002, 1, 1))
        # flush, which should raise exception
        with ShouldRaise(IntegrityError):
            self.session.flush()


class TestExcludeConstraintConstruction:

    @pytest.fixture()
    def booking(self, base):
        class Booking(Temporal, Common, base):
            key_columns = ('hotel', 'room')
            hotel = Column(String)
            room = Column(Integer)
        return Booking

    @pytest.fixture()
    def session(self, connection, base, booking):
        with create_tables_and_session(connection, base) as session:
            yield session

    def test_valid(self, booking, session):
        session.add(booking(
            hotel='h1', room=1, period=Range(dt(2001, 1, 1),)
        ))
        session.add(booking(
            hotel='h1', room=2, period=Range(dt(2001, 1, 1),)
        ))
        session.add(booking(
            hotel='h2', room=1, period=Range(dt(2001, 1, 1),)
        ))
        session.add(booking(
            hotel='h2', room=2, period=Range(dt(2001, 1, 1),)
        ))
        session.flush()

    def test_invalid(self, booking, session):
        session.add(booking(
            hotel='h1', room=1, period=Range(dt(2001, 1, 1), None)
        ))
        session.add(booking(
            hotel='h1', room=1, period=Range(None, dt(2001, 1, 2))
        ))
        with ShouldRaise(IntegrityError):
            session.flush()


class TestExcludeConstraintTurnedOff:

    @pytest.fixture()
    def model(self, base):
        class Model(Temporal, Common, base):
            key_columns = ['name']
            exclude_constraint = False
            name = Column(String)

        return Model

    @pytest.fixture()
    def session(self, connection, base, model):
        with create_tables_and_session(connection, base) as session:
            yield session

    def test_invalid(self, model, session):
        # Check that the exclude constraint isn't present, so
        # we don't get an exception:
        session.add(model(name='foo', period=Range(dt(2001, 1, 1), None)))
        session.add(model(name='foo', period=Range(dt(2002, 1, 1), None)))
        session.flush()


class TestValueColumnGuessing:

    def test_no_key_columns(self, base):
        class Booking(Temporal, Common, base):
            hotel = Column(String)
            room = Column(Integer)
        compare(Booking.value_columns, expected=None)
        with ShouldRaise(TypeError):
            Booking().value_tuple

    def test_value_column(self, base):
        class Symbol(Temporal, Common, base):
            key_columns = ['type']
            type = Column(String)
            value = Column(Integer)
        compare(Symbol.value_columns, expected=['value'])
        compare(Symbol(type='foo', value='bar').value_tuple,
                expected=('bar',), strict=True)

    def test_multiple_value_columns(self, base):
        class It(Temporal, Common, base):
            key_columns = ['key']
            key = Column(String)
            a = Column(Integer)
            b = Column(Integer)
        compare(It.value_columns, expected=['a', 'b'])
        compare(It(key='kv', a='av', b='bv').value_tuple,
                expected=('av', 'bv'), strict=True)

    def test_explicit_value_tuple(self, base):
        class It(Temporal, Common, base):
            key = Column(String)
            a = Column(Integer)
            b = Column(Integer)

            @property
            def value_tuple(self):
                return self.a
        compare(It.value_columns, expected=None)
        compare(It(key='kv', a='av', b='bv').value_tuple,
                expected='av', strict=True)


class TestTemporalMethods:

    def test_period_str_end(self, model):
        compare(model(value_to=dt(2001, 1, 1)).period_str(),
                'until 2001-01-01 00:00:00')
    
    def test_period_str_start(self, model):
        compare(model(value_from=dt(2001, 1, 1)).period_str(),
                '2001-01-01 00:00:00 onwards')
    
    def test_period_str_both(self, model):
        compare(model(
                period=Range(dt(2000, 1, 1), dt(2001, 1, 1))
                ).period_str(),
                '2000-01-01 00:00:00 to 2001-01-01 00:00:00')

    def test_period_str_both_with_time(self, model):
        compare(model(
                period=Range(dt(2000, 1, 1, 16), dt(2001, 1, 1, 15))
                ).period_str(),
                '2000-01-01 16:00:00 to 2001-01-01 15:00:00')

    def test_period_str_neither(self, model):
        compare(model().period_str(),
                'unknown')
    
    def test_period_both_none(self, model):
        compare(model(period=Range(None, None)).period_str(),
                'always')


class TestNoKeyColumns:

    @pytest.fixture()
    def model(self, base):
        class NoKeys(Temporal, Common, base):
            col = Column(String)
        return NoKeys

    @pytest.fixture()
    def session(self, connection, base, model):
        with create_tables_and_session(connection, base) as session:
            yield session

    def test_invalid(self, model, session):
        session.add(model(col='a', period=Range(dt(2001, 1, 1))))
        session.add(model(col='b', period=Range(dt(2001, 1, 1))))
        # you get the helper methods, but no exclude constraint
        session.flush()


class TestSingleTableInheritance:

    @pytest.fixture()
    def table(self, base):
        class TheTable(Temporal, base):
            __tablename__ = 'stuff'
            __mapper_args__ = dict(
                polymorphic_on='type',
                polymorphic_identity='base',
            )
            key_columns = ['type', 'col']
            type = Column(String)
            col = Column(String)
        return TheTable

    @pytest.fixture()
    def model1(self, table):
        class Model1(table):
            __mapper_args__ = dict(
                polymorphic_identity='model1',
            )
        return Model1

    @pytest.fixture()
    def model2(self, table):
        class Model2(table):
            __mapper_args__ = dict(
                polymorphic_identity='model2',
            )
        return Model2

    @pytest.fixture(autouse=True)
    def session(self, connection, base, model1, model2):
        with create_tables_and_session(connection, base) as session:
            yield session

    def test_only_two_constraints_created(self, base, table):
        # paranoid: only one table
        compare(base.metadata.tables.keys(), expected=['stuff'])
        compare(
            sorted(c.__class__.__name__
                   for c in table.__table__.constraints),
            expected=(
                'CheckConstraint', 'ExcludeConstraint', 'PrimaryKeyConstraint'
            )
        )

    def test_add_one_of_each(self, model1, model2, session):
        session.add(model1(col='a', period=Range(dt(2001, 1, 1))))
        session.add(model2(col='b', period=Range(dt(2001, 1, 1))))
        # you get the helper methods, but no exclude constraint
        session.flush()


class SetForPeriodHelpers:

    @pytest.fixture(autouse=True)
    def model(self, base):

        class Model(Temporal, Common, base):
            key_columns = ['key']
            key = Column(String)
            value = Column(String)

        self.Model = Model

        return Model

    @pytest.fixture(autouse=True)
    def session_(self, connection, base):
        with create_tables_and_session(connection, base) as session:
            self.session = session
            yield session

    @pytest.fixture(autouse=True)
    def log_(self):
        with LogCapture(recursive_check=True) as log:
            self.log = log
            yield log

    def check(self, *expected):
        self.session.flush()
        compare(expected, actual=[
            (row.key, row.value, row.l, row.u)
            for row in self.session.execute(text(
                'select key, value, lower(period) as l, upper(period) as u '
                'from model order by key, l'
            ))])


class TestCoalesceSetForPeriod(SetForPeriodHelpers):

    def test_simple(self):
        # existing:
        #      new:  |(n)---->
        #   stored:  |(n)---->
        m = self.Model(key='k', value='n', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'n', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 onwards set to n")
        )

    def test_change(self):
        # existing:  |(o)-------->
        #      new:        |-(n)->
        #   stored:  |(o)--|-(n)->
        self.session.add(
            self.Model(key='k', value='o', value_from=dt(2001, 1, 1))
        )
        m = self.Model(key='k', value='n', value_from=dt(2002, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'o', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'n', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2002-01-01 00:00:00 onwards set to n")
        )

    def test_start_earlier(self):
        # existing:        |-(v)->
        #      new:  |(v)-------->
        #   stored:  |(v)-------->
        self.session.add(
            self.Model(key='k', value='v', value_from=dt(2002, 1, 1))
        )
        m = self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 set to v")
        )

    def test_multiple_open(self):
        # existing:        |-(v)--|-(v)--|
        #      new:  |(v)---------------->
        #   stored:  |(v)---------------->
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='v',
                       value_from=dt(2003, 1, 1), value_to=dt(2004, 1, 1)),
        ))
        m = self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to v"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2004-01-01 00:00:00 onwards "
             "set to v",),
        )

    def test_multiple_open_gaps(self):
        # existing:        |-(v)--|  |-(v)-->
        #      new:  |(v)------------------->
        #   stored:  |(v)------------------->
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='v',
                       value_from=dt(2004, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to v"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2003-01-01 00:00:00 to 2004-01-01 00:00:00 "
             "set to v"),
        )

    def test_multiple_closed(self):
        # existing:        |-(v)-|-(v)-|
        #      new:  |(v)-------------------|
        #   stored:  |(v)-------------------|
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='v',
                       value_from=dt(2003, 1, 1), value_to=dt(2004, 1, 1)),
        ))
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2005, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2005, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to v"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2004-01-01 00:00:00 to 2005-01-01 00:00:00 "
             "set to v"),
        )

    def test_multiple_open_not_same(self):
        # existing:    |-(v)->|-(o)->|-(v)->|
        #      new:  |(v)-------------------->
        #   stored:  |(v)-----|-(o)->|-(v)->|
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='o',
                       value_from=dt(2003, 1, 1), value_to=dt(2004, 1, 1)),
            self.Model(key='k', value='v',
                       value_from=dt(2004, 1, 1), value_to=dt(2005, 1, 1)),
        ))
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None)
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2003, 1, 1)),
            ('k', 'o', dt(2003, 1, 1), dt(2004, 1, 1)),
            ('k', 'v', dt(2004, 1, 1), dt(2005, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 set to v")
        )

    def test_multiple_closed_not_same(self):
        # existing:    |-(v)->|-(o)->|-(v)->|
        #      new:  |(v)---------------------|
        #   stored:  |(v)---------------------|
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='o',
                       value_from=dt(2003, 1, 1), value_to=dt(2004, 1, 1)),
            self.Model(key='k', value='v',
                       value_from=dt(2004, 1, 1), value_to=dt(2005, 1, 1)),
        ))
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2006, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2006, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal',
             'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to v"),
            ('mortar_sqlalchemy.mixins.temporal',
             'WARNING',
             "key='k' from 2003-01-01 00:00:00 to 2004-01-01 00:00:00 "
             "changed from o to v"),
            ('mortar_sqlalchemy.mixins.temporal',
             'INFO',
             "key='k' from 2005-01-01 00:00:00 to 2006-01-01 00:00:00 "
             "set to v")
        )

    def test_no_change(self):
        # existing:  |(v)-------->
        #      new:        |-(v)->
        #   stored:  |(v)-------->
        self.session.add(
            self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        )
        m = self.Model(key='k', value='v', value_from=dt(2002, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'DEBUG',
             "key='k' from 2002-01-01 00:00:00 onwards left at v")
        )

    def test_change_past(self):
        # existing:       |--(o)->
        #      new:  |(n)--|
        #   stored:  |(n)--|(o)-->
        self.session.add(
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1))
        )
        m = self.Model(key='k', value='n',
                       value_from=dt(2001, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'n', dt(2001, 1, 1), dt(2003, 1, 1)),
            ('k', 'o', dt(2003, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o to n"),
        )

    def test_set_period_end(self):
        # existing:  |(v)-------->
        #      new:  |-(v)-|
        #   stored:  |-(v)-|
        self.session.add(
            self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None))
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2002, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period "
             "from 2001-01-01 00:00:00 onwards "
             "to 2001-01-01 00:00:00 to 2002-01-01 00:00:00")
        )

    def test_clear_period_end(self):
        # existing:  |-(v)-|
        #      new:  |-(v)------->
        #   stored:  |-(v)------->
        self.session.add(
            self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1))
        )
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None)
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period "
             "from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "to 2001-01-01 00:00:00 onwards")
        )

    def test_open_ended_left(self):
        # existing:  <-(o1)----|  |-(o2)-|
        #      new:         |-(v)-------->
        #   stored:  <-(o1)-|-(v)-|-(o2)-|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=None, value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2003, 1, 1), value_to=dt(2004, 1, 1)),
        ))
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None)
        m.set_for_period(self.session)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2003, 1, 1), dt(2004, 1, 1)),
            ('k', 'o1', None, dt(2001, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o1 to v")
        )


class TestNoCoalesceSetForPeriod(SetForPeriodHelpers):

    def test_simple(self):
        # existing:
        #      new:  |(n)---->
        #   stored:  |(n)---->
        m = self.Model(key='k', value='n', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 onwards set to n")
        )

    def test_first_single_existing_starts_before_open(self):
        # existing:        |-(o)->
        #      new:  |(n)-------->
        #   stored:  |(n)--|-(o)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(1999, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(1999, 1, 1), dt(2002, 1, 1)),
            ('k', 'o', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 1999-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to n")
        )

    def test_first_single_existing_starts_before_multiple(self):
        # existing:        |-(o1)-|-(o2)->
        #      new:  |(n)---------------->
        #   stored:  |(n)--|-(o1)-|-(o2)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o1', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "set to n")
        )

    def test_first_single_existing_starts_at_open(self):
        # existing:  |-(o)->
        #      new:  |-(n)->
        #   stored:  |-(n)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 onwards "
             "changed from o to n")
        )

    def test_first_single_existing_starts_after_open(self):
        # existing:  |(o)------->
        #      new:       |-(n)->
        #   stored:  |(o)-|-(n)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(1999, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2000-01-01 00:00:00 onwards "
             "set to n")
        )

    def test_first_single_existing_starts_after_closed_new_open(self):
        # existing:  |(o)-------|
        #      new:       |-(n)---->
        #   stored:  |(o)-|-(n)---->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 onwards "
             "set to n")
        )

    def test_first_single_existing_starts_before_finishes_before_closed(self):
        # existing:        |-(o)->
        #      new:  |(n)|
        #   stored:  |(n)| |-(o)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(1999, 1, 1), value_to=dt(2000, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'o', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 1999-01-01 00:00:00 to 2000-01-01 00:00:00 "
             "set to n")
        )

    def test_first_single_existing_starts_before_finishes_at_closed(self):
        # existing:        |-(o)->
        #      new:  |(n)--|
        #   stored:  |(n)--|-(o)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(1999, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(1999, 1, 1), dt(2002, 1, 1)),
            ('k', 'o', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 1999-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to n")
        )

    def test_first_single_existing_starts_before_finishes_after_closed(self):
        # existing:      |---(o)->
        #      new:  |(n)--|
        #   stored:  |(n)--|-(o)->
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2001, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(1999, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(1999, 1, 1), dt(2002, 1, 1)),
            ('k', 'o', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 1999-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "set to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o to n"),
        )

    def test_first_single_existing_starts_at_closed(self):
        # existing:  |-(o)->
        #      new:  |-(n)-|
        #   stored:  |-(n)-|
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2002, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o to n")
        )

    def test_first_single_existing_starts_after_closed_new_closed(self):
        # existing:  |(o)------->
        #      new:       |-(n)-|
        #   stored:  |(o)-|-(n)-|
        self.session.add_all((
            self.Model(key='k', value='o',
                       value_from=dt(1999, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2000-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "set to n")
        )

    def test_middle_one_closed(self):
        # existing:  |(o1)--|-(o2)-|---(o3)->
        #      new:        |--(n)----|
        #   stored:  |(o1)-|--(n)----|-(o3)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o3',
                       value_from=dt(2002, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2003, 1, 1)),
            ('k', 'o3', dt(2003, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o2 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o3 to n"),
        )

    def test_middle_two_closed(self):
        # existing:  |(o1)--|-(o2)-|-(o3)-|-(o4)->
        #      new:        |--(n)-----------|
        #   stored:  |(o1)-|--(n)-----------|-(o4)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o3',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='o4',
                       value_from=dt(2003, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2004, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2004, 1, 1)),
            ('k', 'o4', dt(2004, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o2 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o3 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2003-01-01 00:00:00 to 2004-01-01 00:00:00 "
             "changed from o4 to n"),
        )

    def test_middle_one_open(self):
        # existing:  |(o1)-------|-(o2)-|---(o3)->
        #      new:        |-(n)---------------->
        #   stored:  |(o1)-|-(n)-|-(o2)-|---(o3)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o3',
                       value_from=dt(2002, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=None)
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o2', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'o3', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
        )

    def test_middle_two_open(self):
        # existing:  |(o1)-------|-(o2)-|-(o3)-|-(o4)->
        #      new:        |-(n)---------------------->
        #   stored:  |(o1)-|-(n)-|-(o2)-|-(o3)-|-(o4)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o3',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
            self.Model(key='k', value='o4',
                       value_from=dt(2003, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=None)
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o2', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'o3', dt(2002, 1, 1), dt(2003, 1, 1)),
            ('k', 'o4', dt(2003, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
        )

    def test_last_new_closed_existing_closed_before(self):
        # existing:  |-(o1)---|-(o2)--------|
        #      new:         |--(n)---|
        #   stored:  |(o1)--|--(n)---|-(o2)-|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o2 to n"),
        )

    def test_last_new_closed_existing_closed_at(self):
        # existing:  |-(o1)---|-(o2)-|
        #      new:         |--(n)---|
        #   stored:  |(o1)--|--(n)---|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o2 to n"),
        )

    def test_last_new_closed_existing_closed_after(self):
        # existing:  |-(o1)---|-(o2)-|
        #      new:         |--(n)-----|
        #   stored:  |(o1)--|--(n)-----|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2004, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2004, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o2 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2003-01-01 00:00:00 to 2004-01-01 00:00:00 "
             "set to n"),
        )

    def test_last_new_open_existing_closed(self):
        # existing:  |-(o1)-------|-(o2)-|
        #      new:         |-(n)-------->
        #   stored:  |(o1)--|-(n)-|-(o2)-|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=None)
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o2', dt(2001, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
        )

    def test_last_new_closed_existing_open(self):
        # existing:  |-(o1)---|-(o2)-------->
        #      new:         |--(n)---|
        #   stored:  |(o1)--|--(n)---|-(o2)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o2 to n"),
        )

    def test_last_new_open_existing_open(self):
        # existing:  |-(o1)-------|-(o2)->
        #      new:         |-(n)-->
        #   stored:  |(o1)--|-(n)-|-(o2)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2001, 1, 1), value_to=None),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=None)
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2000, 1, 1)),
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o2', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
        )

    def test_same_value(self):
        # existing:  |(v)-------->
        #      new:        |-(v)->
        #   stored:  |(v)--|-(v)->
        self.session.add(
            self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        )
        m = self.Model(key='k', value='v', value_from=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2002, 1, 1)),
            ('k', 'v', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2002-01-01 00:00:00 onwards set to v")
        )

    def test_no_change(self):
        # existing:  | (v)->
        #      new:  |-(v)->
        #   stored:  | (v)->
        self.session.add(
            self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        )
        m = self.Model(key='k', value='v', value_from=dt(2001, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'DEBUG',
             "key='k' from 2001-01-01 00:00:00 onwards left at v")
        )

    def test_period_end(self):
        # existing:  |-(v)-------->
        #      new:  |-(v)-|
        #   stored:  |-(v)-|
        self.session.add(
            self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None)
        )
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'v', dt(2001, 1, 1), dt(2002, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period from 2001-01-01 00:00:00 onwards to "
             "2001-01-01 00:00:00 to 2002-01-01 00:00:00")
        )

    def test_period_open(self):
        # existing:  |-(v)-|
        #      new:  |-(v)----->
        #   stored:  |-(v)----->
        self.session.add(
            self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=dt(2002, 1, 1))
        )
        m = self.Model(key='k', value='v',
                       value_from=dt(2001, 1, 1), value_to=None)
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'v', dt(2001, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period from "
             "2001-01-01 00:00:00 to 2002-01-01 00:00:00 to "
             "2001-01-01 00:00:00 onwards")
        )

    def test_replace_non_last_with_same(self):
        # existing:  |(o1)-|-(o2)->
        #      new:  |(o1)-------->
        #   stored:  |(o1)-|-(o2)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'o1', dt(1999, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'DEBUG',
             "key='k' from 1999-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "left at o1")
        )

    def test_replace_non_last_with_different(self):
        # existing:  |(o1)-|-(o2)->
        #      new:  |(o1)-------->
        #   stored:  |(o1)-|-(o2)->
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(1999, 1, 1), value_to=dt(2002, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2002, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(1999, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(1999, 1, 1), dt(2002, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), None),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 1999-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "changed from o1 to n")
        )

    def test_overlap_with_gap_open(self):
        # existing:  |-(o1)-| |-(o2)-|
        #      new:  |-(n)----------->
        #   stored:  |-(n)--| |-(o2)-|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(2000, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2000, 1, 1), dt(2001, 1, 1)),
            ('k', 'o2', dt(2002, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n")
        )

    def test_overlap_with_gap_closed(self):
        # existing:  |-(o1)-| |-(o2)-|
        #      new:  |-(n)-----------|
        #   stored:  |-(n)-----------|
        self.session.add_all((
            self.Model(key='k', value='o1',
                       value_from=dt(2000, 1, 1), value_to=dt(2001, 1, 1)),
            self.Model(key='k', value='o2',
                       value_from=dt(2002, 1, 1), value_to=dt(2003, 1, 1)),
        ))
        m = self.Model(key='k', value='n',
                       value_from=dt(2000, 1, 1), value_to=dt(2003, 1, 1))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'n', dt(2000, 1, 1), dt(2003, 1, 1)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2000-01-01 00:00:00 to 2001-01-01 00:00:00 "
             "changed from o1 to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' from 2001-01-01 00:00:00 to 2002-01-01 00:00:00 "
             "set to n"),
            ('mortar_sqlalchemy.mixins.temporal', 'WARNING',
             "key='k' from 2002-01-01 00:00:00 to 2003-01-01 00:00:00 "
             "changed from o2 to n"),
        )

    def test_explicit_change_value_period(self):
        # existing:  |-(v)--|--(v)--|
        #      new:  |-(v)----|
        #   stored:  |-(v)----|-(v)-|
        self.session.add_all((
            self.Model(key='k', value='v',
                       value_from=dt(2010, 4, 28, 0, 0), value_to=dt(2010, 4, 28, 12, 0)),
            self.Model(key='k', value='v',
                       value_from=dt(2010, 4, 28, 12, 0), value_to=dt(2010, 4, 29, 12, 0)),
        ))
        self.session.flush()
        m = self.Model(key='k', value='v',
                       value_from=dt(2010, 4, 28, 0, 0), value_to=dt(2010, 4, 29, 0, 0))
        m.set_for_period(self.session, coalesce=False)
        self.check(
            ('k', 'v', dt(2010, 4, 28, 0, 0), dt(2010, 4, 29, 0, 0)),
            ('k', 'v', dt(2010, 4, 29, 0, 0), dt(2010, 4, 29, 12, 0)),
        )
        self.log.check(
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period from 2010-04-28 00:00:00 to 2010-04-28 12:00:00 to "
             '2010-04-28 00:00:00 to 2010-04-29 00:00:00'),
            ('mortar_sqlalchemy.mixins.temporal', 'INFO',
             "key='k' changed period from 2010-04-28 12:00:00 to 2010-04-29 12:00:00 to "
             '2010-04-29 00:00:00 to 2010-04-29 12:00:00'),
        )


class TestSetForPeriodMultiValue:

    @pytest.fixture()
    def model(self, base):

        class Model(Temporal, Common, base):
            key_columns = ['key']
            key = Column(String)
            value1 = Column(String)
            value2 = Column(String)

        return Model

    def test_pretty_value(self, model):
        compare(model(key='k', value1='v1', value2='v2').pretty_value,
                expected="value1='v1', value2='v2'")
