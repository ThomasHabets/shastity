#!/usr/bin/python
#
from __future__ import absolute_import
from __future__ import with_statement

import os
import time
import fcntl

from shastity.util import AutoClose

class Benchmark(object):
    """class Benchmark

    Decorator class. Writes call time to file.

    @Benchmark('/tmp/logfile', 'foobar')
    def foobar():
        [...]

    TODO: protect against hardlink attacks
    """
    def __init__(self, filename, name):
        self.filename = filename
        self.name = name
    def log(self, ts, tts):
        t = time.time() - ts
        tt = [x[1] - x[0] for x in zip(tts, os.times())]
        str = "%s %s real=%.5f user=%.5f sys=%.5f\n" % (time.ctime(),
                                                        self.name,
                                                        time.time()-ts,
                                                        tt[0],
                                                        tt[1])
        fd = AutoClose(os.open(self.filename,
                               os.O_CREAT | os.O_APPEND
                               | os.O_WRONLY | os.O_NOFOLLOW,
                               0644))
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.write(fd.fileno(), str)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            # make sure the file is closed *now*, not delayed
            del fd

    def __call__(self, func, *args, **kw):
        def wrap(*args, **kw):
            ts = time.time()
            tts = os.times()
            try:
                ret = func(*args, **kw)
            finally:
                try:
                    self.log(ts, tts)
                except OSError, e:
                    pass              # fail to log is OK
            return ret
        return wrap
