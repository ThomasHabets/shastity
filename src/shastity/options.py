# -*- coding: utf-8 -*-

# Copyright (c) 2009 Peter Schuller <peter.schuller@infidyne.com>

"""
Configuration options for shastity.

Contains global and command-specific options.
"""

from __future__ import absolute_import
from __future__ import with_statement

import shastity.config as config
import shastity.logging as logging
import shastity.verbosity as verbosity

DEFAULT_BLOCK_SIZE = 1*1024*1024

def _config(opts):
    """
    @param opts: List of Option:s.
    @return A corresponding Configuration instance.
    """
    return config.DefaultConfiguration(dict([(opt.name(), opt) for opt in opts]))

def GlobalOptions():
    """
    Instantiate a Configuration of global options that apply to
    shastity regardless of which command is running.
    """
    return _config([ config.VerboseOption('verbosity', 'v', verbosity.to_verbosity(logging.DEBUG)),
                     config.IntOption('block-size', None, DEFAULT_BLOCK_SIZE,
                                      short_help='The size in bytes of storage blocks.'),
                     config.StringOption('config', 'c', '~/.shastity',
                                         short_help='Config file.'),
                     ]
                   )

def EncryptionOptions():
    return _config([config.StringOption('crypto-key', 'k', None,
                                        short_help='Encryption key')])

def S3Options():
    return _config([
            config.StringOption('aws_secret_access_key', None, None,
                                short_help='S3 Secret key'),
            config.StringOption('aws_access_key_id', None, None,
                                short_help='S3 Access key'),
            config.StringOption('proxy', None, None,
                                short_help='HTTP Proxy address'),
            config.StringOption('proxy_port', None, None,
                                short_help='HTTP Proxy port'),
            config.StringOption('https', None, True,
                                short_help='Use HTTPS with S3'),
                    ])


def PersistOptions():
    return _config([
            config.StringOption('skip-blocks', None, None,
                                short_help='File containing blocks to skip'),
            config.BoolOption('continue', None, None,
                              short_help='Check list of blocks uploaded'),
                    ])
