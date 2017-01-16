from datetime import datetime as dt
from unittest import TestCase

from mortar_rdb.testing import get_session, register_session
from psycopg2.extras import DateTimeRange as Range
from sqlalchemy import CheckConstraint, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String
from testfixtures import ShouldRaise, compare

from mortar_mixins.common import Common
from mortar_mixins.temporal import Temporal


class Helper(object):

    @classmethod
    def setUpClass(cls):
        register_session(transactional=False)

    def setUp(self):
        b = declarative_base()
        class Model(Temporal, Common, b):
            key_columns = ['name']
            name = Column(String)
        self.Model = Model
        self.session = get_session()
        self.addCleanup(self.session.rollback)
        b.metadata.create_all(self.session.bind)


class PropertyTests(Helper, TestCase):

    def setUp(self):
        super(PropertyTests, self).setUp()
        self.obj = self.Model(
            name = 'test',
            period = Range(dt(2000, 1, 1), dt(2001, 1, 1))
            )
        
    def test_get_from(self):
        compare(self.obj.value_from, dt(2000, 1, 1))
    
    def test_set_from(self):
        self.obj.value_from = dt(1999, 1, 1)
        compare(self.obj.period,
                Range(dt(1999, 1, 1), dt(2001, 1, 1)))
    
    def test_set_from_none(self):
        self.obj = self.Model()
        self.obj.value_from = dt(1999, 1, 1)
        compare(self.obj.period,
                Range(dt(1999, 1, 1), None))
    
    def test_get_to(self):
        compare(self.obj.value_to, dt(2001, 1, 1))
    
    def test_set_to(self):
        self.obj.value_to = dt(2002, 1, 1)
        compare(self.obj.period,
                Range(dt(2000, 1, 1), dt(2002, 1, 1)))
    
    def test_set_to_none(self):
        self.obj = self.Model()
        self.obj.value_to = dt(1999, 1, 1)
        compare(self.obj.period,
                Range(None, dt(1999, 1, 1)))


class ConstructorTests(Helper, TestCase):

    def test_from(self):
        obj = self.Model(name = 'test', value_from = dt(2000, 1, 1))
        compare(obj.period, Range(dt(2000, 1, 1), None))

    def test_to(self):
        obj = self.Model(name = 'test', value_to = dt(2000, 1, 1))
        compare(obj.period, Range(None, dt(2000, 1, 1)))

    def test_period_and_from(self):
        with ShouldRaise(TypeError(
                'period not allowed if value_from or value_to used'
                )):
            self.Model(
                name = 'test',
                period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                value_from = dt(2000, 1, 1),
                )

    def test_period_and_to(self):
        with ShouldRaise(TypeError(
                'period not allowed if value_from or value_to used'
                )):
            self.Model(
                name = 'test',
                period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                value_to = dt(2000, 1, 1),
                )


class ValueAtTests(Helper, TestCase):

    def setUp(self):
        super(ValueAtTests, self).setUp()
        # some Models
        self.session.add(self.Model(
                period = Range(dt(2000, 1, 1), dt(2001, 1, 1)),
                name = 'Name 1-1'
                ))
        self.session.add(self.Model(
                period = Range(dt(2001, 1, 1), dt(2002, 1, 1)),
                name = 'Name 1-2'
                ))
        self.session.add(self.Model(
                period = Range(dt(2002, 1, 1), None),
                name = 'Name 1-3'
                ))

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


class ConstraintTests(Helper, TestCase):
    
    def _check_valid(self, existing=(), new=(), exception=False):
        # both existing and new should be sequences of two-tuples
        # of the form (value_from,value_to)
        for value_from, value_to in existing:
            self.session.add(self.Model(
                    period = Range(value_from, value_to),
                    name = 'Name'
                    ))

        for value_from, value_to in new:
            self.session.add(self.Model(
                    period = Range(value_from, value_to),
                    name = 'Name'
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


class TestExcludeConstraintConstruction(Helper, TestCase):

    def setUp(self):
        Base = declarative_base()
        class Booking(Temporal, Common, Base):
            key_columns = ('hotel', 'room')
            hotel =  Column(String)
            room =  Column(Integer)
        self.Booking = Booking
        self.session = get_session()
        self.addCleanup(self.session.rollback)
        Base.metadata.create_all(self.session.bind)

    def test_valid(self):
        self.session.add(self.Booking(
                hotel='h1', room=1, period=Range(dt(2001, 1, 1),)
                ))
        self.session.add(self.Booking(
                hotel='h1', room=2, period=Range(dt(2001, 1, 1),)
                ))
        self.session.add(self.Booking(
                hotel='h2', room=1, period=Range(dt(2001, 1, 1),)
                ))
        self.session.add(self.Booking(
                hotel='h2', room=2, period=Range(dt(2001, 1, 1),)
                ))
        self.session.flush()

    def test_invalid(self):
        self.session.add(self.Booking(
                hotel='h1', room=1, period=Range(dt(2001, 1, 1), None)
                ))
        self.session.add(self.Booking(
                hotel='h1', room=1, period=Range(None, dt(2001, 1, 2))
                ))


class MethodTests(Helper, TestCase):

    def test_period_str_end(self):
        compare(self.Model(value_to=dt(2001, 1, 1)).period_str(),
                'until 2001-01-01 00:00:00')
    
    def test_period_str_start(self):
        compare(self.Model(value_from=dt(2001, 1, 1)).period_str(),
                '2001-01-01 00:00:00 onwards')
    
    def test_period_str_both(self):
        compare(self.Model(
                period=Range(dt(2000, 1, 1), dt(2001, 1, 1))
                ).period_str(),
                '2000-01-01 00:00:00 to 2001-01-01 00:00:00')

    def test_period_str_both_with_time(self):
        compare(self.Model(
                period=Range(dt(2000, 1, 1, 16), dt(2001, 1, 1, 15))
                ).period_str(),
                '2000-01-01 16:00:00 to 2001-01-01 15:00:00')

    def test_period_str_neither(self):
        compare(self.Model().period_str(),
                'unknown')
    
    def test_period_both_none(self):
        compare(self.Model(period=Range(None, None)).period_str(),
                'always')


class TestNoKeyColumns(Helper, TestCase):

    def setUp(self):
        Base = declarative_base()
        class NoKeys(Temporal, Common, Base):
            col = Column(String)
        self.Model = NoKeys
        self.session = get_session()
        self.addCleanup(self.session.rollback)
        Base.metadata.create_all(self.session.bind)

    def test_valid(self):
        self.session.add(self.Model(col='a', period=Range(dt(2001, 1, 1))))
        self.session.add(self.Model(col='b', period=Range(dt(2001, 1, 1))))
        # you get the helper methods, but no exclude constraint
        self.session.flush()


class TestSingleTableInheritance(Helper, TestCase):

    def setUp(self):
        self.Base = declarative_base()
        class TheTable(Temporal, self.Base):
            __tablename__ = 'stuff'
            __mapper_args__ = dict(
                polymorphic_on='type',
                polymorphic_identity='base',
            )
            key_columns = ['type', 'col']
            type = Column(String)
            col = Column(String)
        self.TheTable = TheTable
        class Model1(TheTable):
            __mapper_args = dict(
                polymorphic_identity='model1',
            )
        class Model2(TheTable):
            __mapper_args = dict(
                polymorphic_identity='model2',
            )
        self.Model1 = Model1
        self.Model2 = Model2
        self.session = get_session()
        self.addCleanup(self.session.rollback)
        self.Base.metadata.create_all(self.session.bind)

    def test_only_two_constraints_created(self):
        # paranoid: only one table
        compare(self.Base.metadata.tables.keys(), expected=['stuff'])
        compare(
            sorted(c.__class__.__name__
                   for c in self.TheTable.__table__.constraints),
            expected=(
                'CheckConstraint', 'ExcludeConstraint', 'PrimaryKeyConstraint'
            )
        )

    def test_add_one_of_each(self):
        self.session.add(self.Model1(col='a', period=Range(dt(2001, 1, 1))))
        self.session.add(self.Model2(col='b', period=Range(dt(2001, 1, 1))))
        # you get the helper methods, but no exclude constraint
        self.session.flush()
