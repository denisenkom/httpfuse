import argparse
import logging
import logging.config
import os
import fuse
import errno
import logging
import stat
from fuse import FuseOSError
import threading
import requests

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='TODO')
parser.add_argument('--conf',
                    dest='conf',
                    metavar='CONFIG',
                    required=True)
parser.add_argument('--pidfile')


uid = os.getuid()
gid = os.getgid()
conf = None


class Fuse(fuse.Operations):
    def __init__(self, root_node, conf={}):
        self._cntr = 0
        self._cntr_lock = threading.Lock()
        self._root_node = root_node
        self._conf = conf
        self._blksize = 128 * 1024

    def _resolve_path(self, path):
        assert os.path.isabs(path)
        assert os.path.normpath(path) == path, 'should be normalized'
        assert not path.startswith('//')
        path = path[1:]  # strip root /
        node = self._root_node
        if not path:
            return node
        for name in path.split('/'):
            if node['type'] != 'directory':
                raise FuseOSError(errno.ENOTDIR)
            node = node['contents'].get(name)
            if node is None:
                raise FuseOSError(errno.ENOENT)
        return node

    def getattr(self, path, fh):
        node = self._resolve_path(path)
        if node['type'] == 'directory':
            return {'st_atime': self._conf.get('dir_time', 0),
                    'st_mtime': self._conf.get('dir_time', 0),
                    'st_ctime': self._conf.get('dir_time', 0),
                    'st_gid': self._conf.get('gid', gid),
                    'st_uid': self._conf.get('uid', uid),
                    'st_mode': self._conf.get('dir_mode', 0555) | stat.S_IFDIR,
                    'st_nlink': 1,
                    'st_size': 0,
                    'st_blksize': self._blksize,
                    'st_blocks': 0,
                    }
        else:
            res = node['stat'].copy()
            res.update({'st_gid': self._conf.get('gid', gid),
                        'st_uid': self._conf.get('uid', uid),
                        'st_mode': self._conf.get('mode', 0444) | stat.S_IFREG,
                        'st_nlink': 1,
                        'st_blksize': self._blksize,
                        'st_blocks': (node['stat']['st_size'] + self._blksize - 1) // self._blksize,
                        })
            return res

    def open(self, path, flags):
        node = self._resolve_path(path)
        if node['type'] == 'directory':
            raise FuseOSError(errno.EISDIR)
        return 0

    def read(self, path, size, offset, fd):
        node = self._resolve_path(path)
        headers = {'Range': 'bytes=%d-%d' % (offset, offset + size - 1)}
        r = requests.get(node['url'], headers=headers)
        if 200 <= r.status_code < 300:
            return r.content[:size]
        logger.error('Failed to read from %s: (%d) %s' % (path, r.status_code, r.text))
        raise FuseOSError(errno.EIO)

    def release(self, path, fd):
        pass

    def readdir(self, path, fd):
        node = self._resolve_path(path)
        if node['type'] != 'directory':
            raise FuseOSError(errno.ENOTDIR)
        return ['.', '..'] + sorted(node['contents'].keys())


def main():
    args = parser.parse_args()

    #global conf
    #conf = imp.load_source('conf', args.conf)

    if args.pidfile:
        with open(args.pidfile, 'w') as f:
            f.write(str(os.getpid()))

    #logging.config.dictConfig(conf.LOGGING)

    #applog.print_start('encfuse started')
    a_bam_file = {'type': 'file',
             'url': 'http://s3.amazonaws.com/1000genomes/data/NA12878/exome_alignment/NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam',
             'stat': {'st_size': 17282007379},
             }
    a_bai_file = {'type': 'file',
             'url': 'http://s3.amazonaws.com/1000genomes/data/NA12878/exome_alignment/NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam.bai',
             'stat': {'st_size': 8450680},
             }
    n_bam_file = {'type': 'file',
             'url': 'http://ftp-trace.ncbi.nlm.nih.gov/1000genomes/ftp/data/NA12878/exome_alignment/NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam',
             'stat': {'st_size': 17282007379},
             }
    n_bai_file = {'type': 'file',
             'url': 'http://ftp-trace.ncbi.nlm.nih.gov/1000genomes/ftp/data/NA12878/exome_alignment/NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam.bai',
             'stat': {'st_size': 8450680},
             }
    ncbi_dir = {'type': 'directory',
            'contents': {'NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam.bai': n_bai_file,
                         'NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam': n_bam_file,
                         }}
    amazon_dir = {'type': 'directory',
            'contents': {'NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam.bai': a_bai_file,
                         'NA12878.mapped.ILLUMINA.bwa.CEU.exome.20121211.bam': a_bam_file,
                         }}
    root = {'type': 'directory',
            'contents': {'ncbi': ncbi_dir,
                         'amazon': amazon_dir,
                         }}
    fuse.FUSE(Fuse(root),
              '/home/denisenk/tmp/fuse',
              foreground=True,
              allow_other=True,
              nothreads=True,
              fsname='fuse')
    #applog.print_stop()

if __name__ == '__main__':
    main()
