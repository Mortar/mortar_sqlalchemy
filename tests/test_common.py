from unittest import TestCase
from mortar_mixins.common import Common
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from testfixtures import compare

class TestCommon(TestCase):

    def test_tablename(self):

        class FooBar(Common): pass

        compare(FooBar.__tablename__, 'foo_bar')

    def test_wrong_type(self):
        class Foo(Common):
            def __int__(self):
                self.x = 1
        class Bar(Common):
            def __int__(self):
                self.x = 1
        self.assertFalse(Foo()==Bar())
        self.assertTrue(Foo()!=Bar())

    def test_same(self):
        class Foo(Common):
            def __init__(self, x):
                self.x = x
                self._sa_instance_state = id(self)
        self.assertTrue(Foo(1)==Foo(1))
        self.assertFalse(Foo(1)!=Foo(1))

    def test_different(self):
        class Foo(Common):
            def __init__(self, x):
                self.x = x
        self.assertFalse(Foo(1)==Foo(2))
        self.assertTrue(Foo(1)!=Foo(2))

    def test_different_keys(self):
        class Foo(Common):
            def __init__(self):
                pass
        a = Foo()
        a.x = 1
        b = Foo()
        b.y = 1
        self.assertFalse(a==b)
        self.assertTrue(a!=b)

    def test_repr(self):
        class Foo(Common):
            def __init__(self, x):
                self.x = x
                self._sa_instance_state = id(self)
        compare(repr(Foo(1)), 'Foo(x=1)')

class MoreTests(TestCase):

    def setUp(self):
        class Model(Common, declarative_base()):
            id =  Column(Integer, primary_key=True)
        self.Model = Model

    def test_table_name(self):
        compare(self.Model.__table__.name, 'model')

    def test_eq_wrong_type(self):
        self.assertFalse(self.Model() == object())

    def test_eq_different(self):
        self.assertFalse(self.Model(id=1) == self.Model(id=2))

    def test_eq_different_keys(self):
        self.assertFalse(self.Model() == self.Model(id=2))

    def test_eq_same(self):
        self.assertTrue(self.Model(id=1) == self.Model(id=1))

    def test_ne_wrong_type(self):
        self.assertTrue(self.Model() != object())

    def test_ne_different(self):
        self.assertTrue(self.Model(id=1) != self.Model(id=2))

    def test_ne_same(self):
        self.assertTrue(self.Model(id=1) != self.Model(id=2))

    def test_compare(self):
        compare(
            [self.Model(id=1), self.Model(id=2)],
            [self.Model(id=1), self.Model(id=2)]
            )

    def test_repr(self):
        compare('Model(id=1)', repr(self.Model(id=1)))

    def test_str(self):
        compare('Model(id=1)', str(self.Model(id=1)))
