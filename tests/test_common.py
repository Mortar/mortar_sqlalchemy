from unittest import TestCase
from mortar_mixins.common import Common
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
