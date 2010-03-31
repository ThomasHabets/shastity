# -*- coding: utf-8 -*-

"""
Crypto wrappers for backends.

Backends are modified like so:
    be = s3backend('mybucket')
    be = DataCryptoGPG(be, 'data content password')
    be = NameCrypto(be, 'file name password')
    be.put('foo', 'bar')
"""
from __future__ import absolute_import
from __future__ import with_statement

import subprocess
import os
import struct
import re
from Crypto.Cipher import AES

import shastity.backend as backend
import shastity.hash as hash
from shastity.util import AutoClose

def pipeWrap():
    return [AutoClose(x) for x in os.pipe()]

def enc(key, data):
    return encDec(key,data,extra='-c --force-mdc')

def dec(key, data):
    return encDec(key,data,extra='')

def encDec(key, data, extra):
    def doClose(*fds):
        """doClose()
        Write end of password pipe must be closed in child process.
        """
        [os.close(x.fd) for x in fds]

    # password pipe
    pass_r, pass_w = pipeWrap()
    # TODO: do not assume location of gpg
    p = subprocess.Popen(("/usr/bin/gpg -q --batch %s "
                          + " --compress-level 0 --passphrase-fd %d")
                         % (extra, pass_r.fileno()),
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=None,
                         preexec_fn=lambda: doClose(pass_w),
                         shell=True)
    del pass_r

    # write password & close
    with pass_w.fdopen('w') as f:
        f.write(key)

    ret = p.communicate(input=data)[0]
    if p.wait():
        raise "GPG failed"
    return ret

class BackendWrapper(backend.Backend):
    """BackendWrapper(backend.Backend)

    Base class for backend wrappers. By default changes nothing.
    """
    def __init__(self, next):
        self.next = next

    def put(self, *args):
        return self.next.put(*args)

    def get(self, *args):
        return self.next.put(*args)

    def list(self, *args):
        return self.next.list(*args)
    
class DataCryptoGPG(BackendWrapper):
    """DataCryptoGPG(BackendWrapper)

    Encrypt backend 'file' data using GPG. Does not change 'file' name.
    """
    def __init__(self, next, cryptoKey):
        BackendWrapper.__init__(self, next)
        self.cryptoKey = cryptoKey

    def put(self, key, data):
        return self.next.put(key, enc(self.cryptoKey, data))

    def get(self, key):
        return dec(self.cryptoKey, self.next.get(key))

class NameCrypto(BackendWrapper):
    """NameCrypto(BackendWrapper)

    Encrypt backend 'file' names. Not content.

    new name = aes(sha512('key'), 'length of old name' + 'old name' + padding)

    Since all names (except manifests) are hashes to begin with, there
    should be no problem with related plaintext attacks.
    """
    def __init__(self, next, cryptoKey):
        BackendWrapper.__init__(self, next)
        self.cryptoKey = hash.make_hasher('sha512')(cryptoKey)[1]

    def put(self, key, data):
        return self.next.put(self.__enc(key), data)

    def get(self, key):
        return self.next.get(self.__enc(key))

    def list(self):
        return [self.__dec(x) for x in self.next.list()]

    def __enc(self, name):
        crypt = AES.new(self.cryptoKey[:16], AES.MODE_CBC)
        s = struct.pack("!l", len(name)) + name
        if len(s) % 16:
            s = s + " " * (16 - len(s) % 16)
        ret = crypt.encrypt(s)
        return ''.join(["%.2x" % (ord(x)) for x in ret])

    def __dec(self, cfn):
        crypt = AES.new(self.cryptoKey[:16], AES.MODE_CBC)
        s = ''.join([chr(int(x,16)) for x in re.findall('(..)', cfn)])
        dec = crypt.decrypt(s)
        l = struct.unpack("!l", dec[:4])[0]
        if not (0 < l < len(dec)):
            raise "TODO: I don't think your crypto key is correct"
        return dec[4:4+l]
