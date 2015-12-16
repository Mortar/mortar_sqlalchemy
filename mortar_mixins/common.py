import re
from sqlalchemy import inspect
from sqlalchemy.util import classproperty

name_re = re.compile('([a-z]|^)([A-Z])')


def name_subber(match):
    if match.group(1):
        start = match.group(1)+'_'
    else:
        start = ''
    return start+match.group(2).lower()


def de_hump(cls):
    "SomeThing -> some_thing"
    return name_re.sub(name_subber,cls.__name__)


class Common(object):
    
    @classproperty
    def __tablename__(cls):
        return de_hump(cls)
    
    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        self_attrs = inspect(self).attrs
        other_attrs = inspect(other).attrs
        for name, attr in self_attrs.items():
            if attr.value != other_attrs[name].value:
                return False
        return True

    def __ne__(self, other):
        return not (self==other)

    def __repr__(self):
        content = []
        for key, val in sorted(self.__dict__.items()):
            if key == '_sa_instance_state':
                continue
            content.append('%s=%r' % (key, val))
        return '%s(%s)' % (self.__class__.__name__, ', '.join(content))

    __str__ = __repr__

