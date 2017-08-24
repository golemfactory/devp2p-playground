import os.path
import io
import bson

import multihash
from multihash import Multihash
from rlp.utils import bytes_to_str, encode_hex

from golem.resources.partition import Partition, FileWrapper

try:
    from hashlib import blake2b
except:
    from pyblake2 import blake2b

try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
    #slogging.configure(config_string=':debug,p2p:info')
except:
    import devp2p.slogging as slogging

log = slogging.get_logger('playground.file')


def _append_basedir(paths, basedir):
    if not basedir:
        return paths[:]
    return [os.path.join(basedir, path) for path in paths]


class HashedFile(object):
    chunk_size = 2 ** 19
    hash_function = blake2b

    def __init__(self, partition, paths, hashes=None, haveset=None, length=None):
        self.partition = partition
        self.paths = paths
        self.sizes = partition._sizes
        partition.open()
        self.hashes = hashes
        self.tophash = None
        self.haveset = haveset
        self.length = length
        if not self.hashes:
            self._calc_hashes()
            self.haveset = set(range(len(self.hashes)))
        elif self.haveset is None:
            self._check_hashes()
        if self.hashes:
            self._calc_tophash()
            if not self.length:
                self.length = len(self.hashes) * self.partition.chunk_size

    def _hash_chunk(self, chunk_no):
        h = self.hash_function()
        block_size = min(self.chunk_size, h.block_size)

        log.debug('hashing chunk', chunk_no=chunk_no)
        if chunk_no >= self.partition.size():
            return None
        stream = self.get_chunk_stream(chunk_no)
        off = 0
        def read():
            nonlocal off
            size = min(block_size, self.chunk_size - off)
            data = stream.read(size)
            off += len(data)
            return data

        data = read()
        if not data:
            return None
        while data and (off <= self.chunk_size):
            h.update(data)
            data = read()
        return Multihash.from_hash(h)

    def _calc_hashes(self):
        hashes = []

        i = 0
        h = self._hash_chunk(i)
        while h:
            hashes.append(h)
            i += 1
            h = self._hash_chunk(i)

        self.hashes = hashes
        self.length = self.partition._size
        #self._calc_tophash()

    def _check_hashes(self):
        haveset = set()

        for i, h in enumerate(self.hashes):
            if h == self._hash_chunk(i):
                haveset.add(i)

        self.haveset = haveset

    def _calc_tophash(self):
        assert self.hashes
        self.tophash = multihash.digest(
            self.binary_metainfo(),
            multihash.Func.sha3_256).encode()

    def get_chunk_size(self, chunk_no):
        if chunk_no > len(self.hashes):
            return None
        off = self.chunk_size * chunk_no
        return min(self.chunk_size, self.length - off)

    def get_chunk_stream(self, chunk_no):
        return self.partition.get_stream(chunk_no)

    def metainfo(self):
        return {
            'chunk_size': self.chunk_size,
            'hashes': [mh.encode(None) for mh in self.hashes],
            'sizes': self.sizes,
            'paths': self.paths,
            }

    def binary_metainfo(self):
        def on_unknown(x):
            log.error('cannot serialize', x=x)
            raise Exception
        return bson.dumps(self.metainfo(), on_unknown=on_unknown)

    @classmethod
    def from_path(cls, path):
        return cls.from_paths([path])

    @classmethod
    def from_paths(cls, paths, basedir=None):
        partition = Partition(_append_basedir(paths, basedir), chunk_size=cls.chunk_size)
        wrapped = partition.file_wrapper

        class BufferedFileWrapper(FileWrapper):
            @classmethod
            def open(cls, path, mode='r+b'):
                fd = wrapped.open(path, mode)
                return io.BufferedRandom(fd)

        partition.file_wrapper = BufferedFileWrapper

        return cls(partition, paths)

    @classmethod
    def from_metainfo(cls, metainfo, outdir=None):
        hashes = [multihash.decode(mh) for mh in metainfo['hashes']]
        sizes = metainfo['sizes']
        paths = metainfo['paths']

        chunk_size = metainfo['chunk_size']

        partition = Partition.create_thin(_append_basedir(paths, outdir), sizes, chunk_size)
        return cls(partition, paths, hashes=hashes, length=sum(sizes))

    @classmethod
    def from_binary_metainfo(cls, metainfo, outdir=None):
        return cls.from_metainfo(bson.loads(metainfo), outdir)

    def __repr__(self):
        return "<%s(%r, %r)>" % (self.__class__.__name__, self.partition, self.hashes)

if __name__ == '__main__':
    import sys
    from ethereum import slogging
    slogging.configure(':debug')
    hf = HashedFile.from_path(sys.argv[1])
    #hf.do_hash()
    #print(hf)
    for h in hf.hashes:
        print(bytes_to_str(h.encode('hex')))
    print(hf.metainfo())
    #print(bytes_to_str(encode_hex(hf.tophash)))
