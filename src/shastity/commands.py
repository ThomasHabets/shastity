# -*- coding: utf-8 -*-

# Copyright (c) 2009 Peter Schuller <peter.schuller@infidyne.com>

"""
Operational command interface to shastity.

Provides an easy-to-use high-level library interface to invoking all
of shastities main functions, de-coupled from the normal command line
interface.

This is the public interface intended for other software to use for
invoking shastity as a library rather than a shell tool, for tighter
integration.

Commands
========

Commands are named operation that can be executed. A command takes
some number (possibly zero) of positional arguments, and potentially
some number of options on a key/value basis.

In concrete terms, each command will have associated with it::

  - Its name.
  - Information about positional arguments for purpose of display to humans.
  - Information about options that may apply to the command.

In plain python, a command C with positional arguments pos1, pos2,
... poN and a set of options O (in the form of a Configuration
instance) translates to a function call on this module of the form:

  C(options=O, pos1, pos2, ..., posN)

The concept is specifically meant to translate well into a command
line interface while still being fairly idiomatic and usable as a
library interface, while keeping the implementation identical.
"""

from __future__ import absolute_import
from __future__ import with_statement

import re
import locale

import shastity.options as options
import shastity.config as config
import shastity.benchmark as benchmark
import shastity.traversal as traversal
import shastity.logging as logging
import shastity.manifest as manifest
import shastity.filesystem as filesystem
import shastity.persistence as persistence
import shastity.materialization as materialization
import shastity.storagequeue as storagequeue
import shastity.backends.s3backend as s3backend
import shastity.backends.gpgcrypto as gpgcrypto

log = logging.get_logger(__name__)

# In the future we'll have groups of commands too, or else command
# listings to the user become too verbose.

class Command(object):
    def __init__(self, name, args, options, description=None, long_help=None):
        """
        @param name: Name - string.
        @param args: List of arguments (list of name strings for human use).
        @param options: Configuration instance for the command.
        @param description: Short one-liner description, if given.
        @param long_help: Long potentially multi-line description, if given.
        """
        self.name = name
        self.args = args
        self.options = options
        self.description = description
        self.long_help = long_help

_all_commands = [ Command('persist',
                          ['src-path', 'dst-uri'],
                          options.GlobalOptions(),
                          description='Persist (backup) a directory tree.'),
                  Command('materialize',
                          ['src-uri', 'dst-path'],
                          options.GlobalOptions(),
                          description='Materialize (restore) a directory tree.'),
                  Command('verify',
                          ['src-path', 'dst-uri'],
                          options.GlobalOptions(),
                          description='Verify that a directory tree matches that which has previously been persisted.'),
                  Command('garbage-collect',
                          ['dst-uri'],
                          options.GlobalOptions(),
                          description='Garbage collect backend, removing unreferenced data (thus reclaiming space).'),
                  Command('test-backend',
                          ['dst-uri'],
                          options.GlobalOptions(),
                          description='Perform tests on the backend to confirm it works.'),
                  Command('list-manifest',
                          ['uri'],
                          options.GlobalOptions(),
                          description='List names of manifests'),
                  Command('list-files',
                          ['uri'],
                          options.GlobalOptions(),
                          description='List names of blocks or manifests'),
                  Command('common-blocks',
                          ['uri'],
                          options.GlobalOptions(),
                          description='Find common blocks in two or more manifests'),
                  Command('get-block',
                          ['uri', 'block-name', 'local-name'],
                          options.GlobalOptions(),
                          description='Get a backend block by its plaintext name'),
                  Command('show-manifest',
                          ['uri', 'label'],
                          options.GlobalOptions(),
                          description='Show manifest in readable format'),
                  Command('list-blocks',
                          ['uri'],
                          options.GlobalOptions(),
                          description='List blocks in data store'),
                  Command('list-orphans',
                          ['uri'],
                          options.GlobalOptions(),
                          description='List blocks that are not in any manifest'),
                  ]

def all_commands():
    """
    Returns a list of all commands. The order of the list is significant.
    """
    return _all_commands

def has_command(name):
    """
    Convenience function to check whether there is a command by the
    given name.
    """
    return (len([ cmd for cmd in all_commands() if cmd.name == name]) > 0)

def get_command(name):
    matching = [ cmd for cmd in _all_commands if cmd.name == name]

    assert len(matching) == 1

    return matching[0]

CONCURRENCY = 10 # TODO: hard-coded
def flatten(z):
    return reduce(lambda x,y: x + y, z)

def get_all_manifests(be):
    return [(x, list(manifest.read_manifest(be, x)))
            for x in manifest.list_manifests(be)]

def get_all_blockhashes(mfs, unique = True):
    ret = flatten([x[2] for x in flatten(mfs)])
    if unique:
        ret = list(set(ret))
    return ret

def persist(conf, src_path, dst_uri):
    mpath, label, dpath = dst_uri.split(',')
    blocksize = conf.get_option('block-size').get_required()

    bf_manifest = get_backend_factory(mpath, conf)
    b_manifest = bf_manifest()
    bf_data = get_backend_factory(dpath, conf)

    uploaded = []

    try:
        f = open(conf.get_option('skip-blocks').get_required())
        log.info("loading skip-blocks file...")
        for hash in f.readlines():
            hash = hash.strip()
            if len(hash) == 512/4:
                alg = 'sha512' # TODO: assume this?
            uploaded.append( (alg, hash) )
        f.close()
    except config.RequiredOptionMissingError, e:
        pass

    if not conf.get_option('skip-blocks').get():
        log.info("checking old manifests...")
        mfs = get_all_manifests(b_manifest)
        if len(mfs) != 0:
            mfs = zip(*mfs)[1]
            uploaded.extend(get_all_blockhashes(mfs))

    if conf.get_option('continue').get_required():
        log.info("checking for previously upped blocks...")
        for hash in bf_data().list():
            hash = hash.strip()
            if len(hash) == 512/4:
                alg = 'sha512' # TODO: assume this?
            uploaded.append( (alg, hash) )

    # run persist
    fs = filesystem.LocalFileSystem()
    traverser = traversal.traverse(fs, src_path)
    sq = storagequeue.StorageQueue(bf_data,
                                   CONCURRENCY)
    mf = list(persistence.persist(fs,
                                  traverser,
                                  None,
                                  src_path,
                                  sq,
                                  blocksize=blocksize,
                                  skip_blocks=uploaded))
    manifest.write_manifest(b_manifest, label, mf)

def materialize(config, src_uri, dst_path, *files):
    if len(files) == 0:
        files = None
    mpath, label, dpath = src_uri.split(',')
    fs = filesystem.LocalFileSystem()
    fs.mkdir(dst_path)
    mf = list(manifest.read_manifest(get_backend_factory(mpath, config)(),
                                     label))
    sq = storagequeue.StorageQueue(get_backend_factory(dpath, config),
                                   CONCURRENCY)
    materialization.materialize(fs, dst_path, mf, sq, files)

def get_backend_factory(uri, config):
    """get_backend_factory(uri, config)

    Parses a URI and creates the factory.

    TODO: crypto stuff are added by magic, and only s3 is supported.
          This should be configurable
    """
    type,ident = uri.split(':',1)
    if type == 's3':
        ret = lambda: s3backend.S3Backend(ident, config.to_dict())
        crypto_key = config.get_option('crypto-key').get_required()

        ret2 = lambda: gpgcrypto.DataCryptoGPG(ret(), crypto_key)
        ret3 = lambda: gpgcrypto.NameCrypto(ret2(), crypto_key)
        return ret3
    raise NotImplementedError('backend type %s not implemented' % (type))

def show_manifest(config, uri, label):
    def number_group(n, sep):
        # TODO: if boto didn't bork out we would use locale instead of re:
        # return locale.format('%d', attr.size, grouping=True)
        return re.sub(r'(\d)(?=(\d{3})+$)',
                      r"\1" + sep,
                      str(n))

    b = get_backend_factory(uri, config)()
    print "%10s %7s %14s %s" % ('Attr','Blocks','Bytes','Name')
    print "-" * (10 + 7 + 14 + 2 + 10)
    totblocks = 0
    totsize = 0
    for name,attr,sums in manifest.read_manifest(b, label):
        totblocks += len(sums)
        totsize += attr.size
        print "%10s %7s %14s %s" % (
            str(attr).split(' ',1)[0],
            number_group(len(sums), "'"),
            number_group(attr.size, "'"),
            name,
            )
    print "-" * (10 + 7 + 14 + 2 + 10)
    print "%10s %7s %14s %s" % ('',
                                number_group(totblocks, "'"),
                                number_group(totsize, "'"),
                                'Total'
                                )

def uniq_c(arr):
    first = True
    count = 1
    ret = []
    for x in arr:
        if first:
            last = x
            first = False
            continue
        if x == last:
            count += 1
        else:
            ret.append( (count, last) )
            last = x
            count = 1
    if not first:
        ret.append( (count, last) )
    return ret

def list_files(config, uri):
    b = get_backend_factory(uri, config)()
    fs = b.list()
    fs.sort()
    for f in fs:
        print f

def list_manifest(config, uri):
    b = get_backend_factory(uri, config)()

    lmfs = list(get_all_manifests(b))
    lmfs.sort()

    if not len(lmfs):
        print "Found no manifests"
        return

    labels,mfs = zip(*lmfs)
    uploaded = get_all_blockhashes(mfs, unique=False)
    nuploaded = len(set(uploaded))

    uploaded.sort()
    dups = [y for x,y in uniq_c(uploaded) if x > 1]
    dups = dict(zip(dups, [1] * len(uploaded)))

    # TODO: not really happy with the speed of this
    #@benchmark.Benchmark('/tmp/shastity.txt', 'persist.print_it')
    def print_it():
        totfiles = 0
        totblocks = 0
        totsize = 0
        shead = "%-30s %6s %7s %7s %7s"
        sdata = "%-30s %6d %7d %7d %7d"
        print shead % ('Manifest', 'Files', 'Blocks', 'Shared', 'MB')
        print "-" * 79
        for label,mf in lmfs:
            shared = 0
            size = sum([x[1].size for x in mf])
            blocks = flatten([x[2] for x in mf])
            shared = sum([x in dups for x in blocks])
            totfiles += len(mf)
            totblocks += len(blocks)
            totsize += size
            print sdata % (label,
                           len(mf),
                           len(blocks),
                           shared,
                           size / 1000000)
        print "-" * 79
        print (sdata % ('Total',
                        totfiles,
                        nuploaded,
                        totblocks-nuploaded,
                        totsize/1000000)) + ' unpacked/unshared'
    print_it()



def common_blocks(config, uri, *mf_names):
    b = get_backend_factory(uri, config)()
    mfs = [manifest.read_manifest(b, x) for x in mf_names]
    blocks = [get_all_blockhashes([x]) for x in mfs]
    all_blocks = flatten(blocks)
    before = [len(x) for x in blocks]
    [ [bl.remove(x) for bl in blocks]
       for x in list(set(all_blocks))
       if all_blocks.count(x) == len(mf_names)
       ]
    after = [len(x) for x in blocks]
    for nm,bf,af in zip(mf_names,before,after):
        print '%d unique in %s' % (af, nm)
    print '%d in common' % (before[0] - after[0])


def get_block(config, uri, block_name, local_name=None):
    if local_name is None:
        local_name = block_name
    b = get_backend_factory(uri, config)()
    open(local_name, 'w').write(b.get(block_name))

def list_blocks(config, uri):
    b = get_backend_factory(uri, config)()
    for name in b.list():
        print name

def list_orphans(config, uri):
    mpath, dpath = uri.split(',')

    b_data = get_backend_factory(dpath, config)()
    all_blocks = b_data.list()

    b_manifest = get_backend_factory(mpath, config)()
    lmfs = list(get_all_manifests(b_manifest))
    labels,mfs = zip(*lmfs)
    mf_blocks = get_all_blockhashes(mfs, unique=False)

    for hash in all_blocks:
        algo = 'sha512' # TODO: assumed this
        if (algo, hash) not in all_blocks:
            print hash

def verify(config, src_path, dst_uri):
    raise NotImplementedError('very not implemented')

def garbage_collect(config, dst_uri):
    raise NotImplementedError('garbage_collect not implemented')

def test_backend(config, dst_uri):
    raise NotImplementedError('test-backend not implemented')
