import sys

PY3 = sys.version_info[0] == 3

if PY3:
    from itertools import zip_longest
else:
    from itertools import izip_longest as zip_longest
