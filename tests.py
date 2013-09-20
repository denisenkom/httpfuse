from datetime import datetime, timedelta
import unittest
import stat
import logging
import xml.etree.ElementTree
import httpfuse
import tempfile
from fuse import FuseOSError
import errno

logging.basicConfig(level='INFO')


class TestCase(unittest.TestCase):
    def test_resolve(self):
        testfile = {'type': 'file', 'url': 'http://example.com/testfile'}
        testfile2 = {'type': 'file', 'url': 'http://example.com/testfile2'}
        dir = {'type': 'directory', 'contents': {'testfile': testfile,
                                                 'testfile2': testfile2}}

        root = {'type': 'directory', 'contents': {'testfile': testfile}}
        fuse = httpfuse.Fuse(root)
        self.assertEqual(root, fuse._resolve_path('/'))
        self.assertEqual(testfile, fuse._resolve_path('/testfile'))

        # directory
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'dir': dir}})
        self.assertEqual(dir, fuse._resolve_path('/dir'))
        self.assertEqual(testfile, fuse._resolve_path('/dir/testfile'))
        self.assertEqual(testfile2, fuse._resolve_path('/dir/testfile2'))

        # not exist
        with self.assertRaises(FuseOSError) as cm:
            fuse._resolve_path('/notexist')
        self.assertEqual(errno.ENOENT, cm.exception.errno)

        # not directory
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'testfile': testfile}})
        with self.assertRaises(FuseOSError) as cm:
            fuse._resolve_path('/testfile/abc')
        self.assertEqual(errno.ENOTDIR, cm.exception.errno)
        #with self.assertRaises(FuseOSError) as cm:
        #    fuse._resolve_path('/testfile/')
        #self.assertEqual(errno.ENOTDIR, cm.exception.errno)

        # invalid paths
        testfile = {'type': 'file', 'url': 'http://example.com/testfile'}
        testfile2 = {'type': 'file', 'url': 'http://example.com/testfile2'}
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'dir': dir}})
        with self.assertRaises(AssertionError):
            fuse._resolve_path('//dir/testfile')
        with self.assertRaises(AssertionError):
            fuse._resolve_path('dir/testfile')
        with self.assertRaises(AssertionError):
            fuse._resolve_path('/dir//testfile')

    def test_open(self):
        testfile = {'type': 'file', 'url': 'http://example.com/testfile'}
        testfile2 = {'type': 'file', 'url': 'http://example.com/testfile2'}
        dir = {'type': 'directory', 'contents': {'testfile': testfile,
                                                 'testfile2': testfile2}}

        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'dir': dir}})
        with self.assertRaises(FuseOSError) as cm:
            fuse.open('/dir', 0)
        self.assertEqual(errno.EISDIR, cm.exception.errno, 'open on directory should raise EISDIR')

        fd = fuse.open('/dir/testfile', 0)
        self.assertIsInstance(fd, int)
        fuse.release('/dir/testfile', fd)

    def test_getattr_file(self):
        atime = datetime(2010, 1, 2, 20, 12, 11)
        ctime = datetime(2010, 1, 2, 20, 12, 12)
        mtime = datetime(2010, 1, 2, 20, 12, 13)
        testfile = {'type': 'file',
                    'url': 'http://example.com/testfile',
                    'stat': {
                        'st_size': 100,
                        'st_atime': atime,
                        'st_ctime': ctime,
                        'st_mtime': mtime,
                        },
                    }
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'testfile': testfile}},
                {'gid': 1, 'uid': 2, 'mode': 0222})
        st = fuse.getattr('/testfile', 0)
        ref = {
            'st_size': 100,
            'st_atime': atime,
            'st_ctime': ctime,
            'st_mtime': mtime,
            'st_gid': 1,
            'st_uid': 2,
            'st_mode': 0222 | stat.S_IFREG,
            'st_nlink': 1,
            'st_blksize': 128 * 1024,
            'st_blocks': 1,
            }
        self.assertDictEqual(ref, st)

    def test_getattr_dir(self):
        dir_time = datetime(2010, 1, 2, 20, 12, 11)
        testdir  = {'type': 'directory',
                    'contents': {},
                    }
        conf = {'gid': 1,
                'uid': 2,
                'mode': 0222,
                'dir_mode': 0666,
                'dir_time': dir_time,
                }
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'dir': testdir}},
                conf)
        st = fuse.getattr('/dir', 0)
        self.assertEqual(st['st_size'], 0)
        self.assertEqual(st['st_atime'], dir_time)
        self.assertEqual(st['st_ctime'], dir_time)
        self.assertEqual(st['st_mtime'], dir_time)
        self.assertEqual(st['st_gid'], 1)
        self.assertEqual(st['st_uid'], 2)
        self.assertEqual(st['st_mode'], 0666 | stat.S_IFDIR)
        self.assertEqual(st['st_nlink'], 1)

    def test_read(self):
        testfile = {'type': 'file', 'url': 'http://ftp-trace.ncbi.nlm.nih.gov/1000genomes/ftp/CHANGELOG'}
        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'testfile': testfile}})
        fd = fuse.open('/testfile', 0)
        fuse.read('/testfile', 100, 0, fd)
        fuse.release('/testfile', fd)

    def test_readdir(self):
        testfile = {'type': 'file', 'url': 'http://example.com/testfile'}
        testfile2 = {'type': 'file', 'url': 'http://example.com/testfile2'}
        dir = {'type': 'directory', 'contents': {'testfile': testfile,
                                                 'testfile2': testfile2}}

        fuse = httpfuse.Fuse({'type': 'directory', 'contents': {'dir': dir}})
        self.assertListEqual(['.', '..', 'dir'], fuse.readdir('/', 0))
        self.assertListEqual(['.', '..', 'testfile', 'testfile2'], fuse.readdir('/dir', 0))
        with self.assertRaises(FuseOSError) as cm:
            fuse.readdir('/dir/testfile', 0)
        self.assertEqual(errno.ENOTDIR, cm.exception.errno)
