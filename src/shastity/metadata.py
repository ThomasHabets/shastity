# -*- coding: utf-8 -*-

# Copyright (c) 2009 Peter Schuller <peter.schuller@infidyne.com>

'''
File meta data handling.
'''

def mode_to_str(propdict):
    '''Internal helper similar to strmode(3). Produces
    'drwxr-xr-x' style (like ls -l) mode strings from the
    permission/sticky/setuid attributes.'''
    d = propdict

    chars = [] # list of characters later to be joined into string

    if d['is_regular']:
        chars.append('-')
    elif d['is_block_device']:
        chars.append('b')
    elif d['is_character_device']:
        chars.append('c')
    elif d['is_directory']:
        chars.append('d')
    elif d['is_symlink']:
        chars.append('l')
    elif d['is_fifo']:
        chars.append('p')
    else:
        raise AssertionError('should not be reachable')

    # symlinks are a special case; individual modes etc don't make sense
    if d['is_symlink']:
        return ''.join(chars) + 'rwxr-xr-x'

    if d['user_read']:
        chars.append('r')
    else:
        chars.append('-')

    if d['user_write']:
        chars.append('w')
    else:
        chars.append('-')

    if d['user_execute']:
        if d['is_setuid']:
            chars.append('s')
        else:
            chars.append('x')
    else:
        if d['is_setuid']:
            chars.append('S')
        else:
            chars.append('-')
        
    if d['group_read']:
        chars.append('r')
    else:
        chars.append('-')

    if d['group_write']:
        chars.append('w')
    else:
        chars.append('-')

    if d['group_execute']:
        if d['is_setgid']:
            chars.append('s')
        else:
            chars.append('x')
    else:
        if d['is_setgid']:
            chars.append('S')
        else:
            chars.append('-')

    if d['other_read']:
        chars.append('r')
    else:
        chars.append('-')

    if d['other_write']:
        chars.append('w')
    else:
        chars.append('-')

    if d['other_execute']:
        if d['is_sticky']:
            chars.append('t')
        else:
            chars.append('x')
    else:
        if d['is_sticky']:
            chars.append('T')
        else:
            chars.append('-')

    return ''.join(chars)

class FileMetaData(object):
    '''Represents meta-data about files, including any and all
    meta-data that are to be preserved on backup/restore.
    
    We make a deliberate effort to provide a high-level abstraction
    rather than POSIX style bit twiddling. We essentially provide a
    dedicated accessor for every relevant piece of information, along
    with methodsfor converting to/from a text format readable both by
    humans (sort of) and ourselves.

    Instances of this class only care about meta data; they know
    nothing of the files to which the meta data appliies, or
    underlying file system operations (construction of FileMetaData
    instances from some actual real file is done by the appropriate
    file system backend).

    Instances of this class are to be treated as read-only, and the
    public interface deliberately makes it difficult to make changes.

    @ivar is_directory        Is the file a directory?
    @ivar is_character_device Is the file a special character device?
    @ivar is_block_device     Is the file a block device?
    @ivar is_regular          Is the file a regularfile?
    @ivar is_fifo             Is the file a FIFO/named pipe?
    @ivar is_symlink          Is the file a symbolic link?
    @ivar uid                 UID of the owner.
    @ivar gid                 GID of the group owner.
    @ivar size                Size of file (if regular, else None).
    @ivar atime               Access time of the file (seconds since epoch).
    @ivar mtime               Modification time of the file (seconds since epoch).
    @ivar ctime               ctime, whatever the platform feels that means (secondssince epoch).
    @ivar is_setuid           Is the setuid bit set?
    @ivar is_setgid           Is the setgid bit set?
    @ivar is_sticky           Is the sticky bit set?
    @ivar yser_read
    @ivar group_read
    @ivar other_read
    @ivar user_write
    @ivar group_write
    @ivar other_write
    @ivar user_execute
    @ivar group_execute
    @ivar other_execute
    '''

    # for introspection and automation purposes.
    propnames = [ 'is_directory',
                  'is_character_device',
                  'is_block_device',
                  'is_regular',
                  'is_fifo',
                  'is_symlink',
                  'uid',
                  'gid',
                  'size',
                  'atime',
                  'mtime',
                  'ctime',
                  'is_setuid',
                  'is_setgid',
                  'is_sticky',
                  'yser_read',
                  'group_read',
                  'other_read',
                  'user_write',
                  'group_write',
                  'other_write',
                  'user_execute',
                  'group_execute',
                  'other_execute' ]

    def __init__(self, props=None, other=None):
        '''
        @param props: Dict of properties that match those of the instance to be created.
        @param other: Other instance on which to base the values of any properties that
                      do not appear in props.
        '''
        self.__write_protected = False

        if other: # initialize from other instance
            for prop in self.propnames:
                setattr(self, prop, getattr(other, prop))
        else:     # else initialize all to None
            for prop in self.propnames:
                setattr(self, prop, None)

        if props:
            for prop, val in props.iteritems():
                setattr(self, prop, val)

        self.__write_protected = True

    def __setattr__(self, key, value):
        # implement trivial write protection scheme
        if (not key.startswith('_')) and self.__write_protected:
            raise AssertionError('setting a property on FileMetaData is not allowed - we are read-only!')
        else:
            self.__dict__[key] = value

    def __getitem__(self, key):
        if key in self.propnames:
            return getattr(self, key)
        else:
            raise KeyError(key)
        
    @classmethod
    def from_string(cls, s):
        '''Given a string in the format produced by to_string(), parse
        it and return the resulting instance.'''
        raise NotImplementedError

    def to_string(self):
        '''Produce a string encoding of this meta data.

        TODO: define format characteristics'''
        raise NotImplementedError
