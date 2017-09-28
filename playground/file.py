import os.path
import io
import struct
import bson

import multihash
from multihash import Multihash
from rlp.utils import bytes_to_str, encode_hex

from typing import Dict, List, Set, Any

try:
    from hashlib import blake2b
except:
    from pyblake2 import blake2b

import devp2p.slogging as slogging

log = slogging.get_logger('playground.file')

class ChunkStream(io.BufferedIOBase):
    """ A file-like object for a single piece of a HashedFile
    """
    def __init__(self, fh, base, length):
        self.fh = fh
        self.base = base
        self.length = length
        self.off = 0

    def _read(self, read_func, size):
        self.fh.seek(self.base + self.off)
        if size < 0:
            size = self.length
        size = min(self.length - self.off, size)
        data = read_func(size)
        self.off += len(data)
        return data

    def read(self, size=-1):
        return self._read(self.fh.read, size)

    def read1(self, size=-1):
        return self._read(self.fh.read1, size)

    def write(self, data):
        self.fh.seek(self.base + self.off)
        if len(data) > self.length - self.off:
            raise IndexError
        size = self.fh.write(data)
        self.off += size
        return size

    def flush(self):
        super(ChunkStream, self).flush()
        self.fh.flush()

    def seek(self, offset, whence=io.SEEK_SET):
        assert whence in [0, 1, 2]
        if whence == 0:
            self.off = offset
        elif whence == 1:
            self.off += offset
        elif whence == 2:
            self.off = max(self.length, self.length + offset)
        return self.off

class HashedFile(object):
    """ A class for splitting a file into pieces, hashing each piece, and
        handling metainfo.
    """

    chunk_size = 2 ** 19
    hash_function = blake2b

    def __init__(self, fh=None, hashes=None, haveset=None, length=None) -> None:
        """ Creates a new HashedFile.

            :param fh: a file-like object for the backing file. Should always
                       be provided. (Calling this constructor without fh is a
                       hack used internally).
            :param hashes: a list of piece hashes. If provided, the pieces in
                           the backing file are checked against the hashes to
                           determine which have been already downloaded.
                           Otherwise, the file is assumed complete, and hashes
                           are calculated from it.
            :param haveset: TODO: delete this argument
            :param length: length of the file. If not provided, it's set to
                           the number of hashes multiplied by piece size, or the
                           size of the backing file if hashes are notprovided.
        """
        self.fh = fh
        self.hashes = hashes    # type: List[Multihash]
        self.tophash = None     # type: bytes
        self.haveset = haveset  # type: Set[int]
        self.length = length    # type: int
        if self.fh:
            if not self.hashes:
                self._calc_hashes()
                self.haveset = set(range(len(self.hashes)))
            elif self.haveset is None:
                self._check_hashes()
        if self.hashes:
            self._calc_tophash()
            if not self.length:
                self.length = len(self.hashes) * self.chunk_size

    def _hash_chunk(self, chunk_no) -> Multihash:
        #print(chunk_no)
        h = self.hash_function()
        block_size = min(self.chunk_size, h.block_size)

        self.fh.seek(chunk_no * self.chunk_size)
        off = 0
        def read():
            nonlocal off
            size = min(block_size, self.chunk_size - off)
            data = self.fh.read(size)
            off += len(data)
            return data

        data = read()
        if not data:
            return None
        while data and (off <= self.chunk_size):
            h.update(data)
            data = read()
        return Multihash.from_hash(h)

    def _calc_hashes(self) -> None:
        hashes = []

        i = 0
        h = self._hash_chunk(i)
        while h:
            hashes.append(h)
            i += 1
            h = self._hash_chunk(i)

        self.hashes = hashes
        self.length = self.fh.seek(0, io.SEEK_END)
        self.fh.seek(0)
        #self._calc_tophash()

    def _check_hashes(self) -> None:
        haveset = set()

        for i, h in enumerate(self.hashes):
            if h == self._hash_chunk(i):
                haveset.add(i)

        self.haveset = haveset

    def _calc_tophash(self) -> None:
        assert self.hashes
        self.tophash = multihash.digest(
            self.binary_metainfo(),
            multihash.Func.sha3_256).encode()

    def get_chunk_size(self, chunk_no) -> int:
        if chunk_no > len(self.hashes):
            return None
        off = self.chunk_size * chunk_no
        return min(self.chunk_size, self.length - off)

    def get_chunk_stream(self, chunk_no) -> ChunkStream:
        if chunk_no > len(self.hashes):
            return None
        off = self.chunk_size * chunk_no
        return ChunkStream(self.fh, off, self.get_chunk_size(chunk_no))

    def metainfo(self) -> Dict[str, Any]:
        """ Returns this file's metainfo dict
        """
        return {
            'hashes': [mh.encode(None) for mh in self.hashes],
            'length': self.length,
            }

    def binary_metainfo(self) -> bytes:
        """ Returns this file's metainfo in binary fromat
        """
        def on_unknown(x):
            log.error('cannot serialize', x=x)
            raise Exception
        def sorter(obj, *args):
            return sorted(obj.keys())
        return bson.dumps(self.metainfo(), generator=sorter, on_unknown=on_unknown)

    @classmethod
    def from_path(cls, path) -> HashedFile:
        """ Creates a new HashedFile backed by the file at specified path,
            assuming the file is complete and creating a new metainfo for it.

            :returns: the newly created HashedFile
        """
        return cls(fh=open(path, 'rb'))

    @classmethod
    def from_metainfo(cls, metainfo, outfh=None, outdir=None) -> HashedFile:
        """ Creates a new HashedFile from the specified metainfo, creating a
            backing file unless specified or already existing.
            If the backing file already exists, its contents are checked against
            hashes in the metainfo, to detect which pieces are already
            downloaded.

            :param metainfo: a metainfo dict.
            :param outfh: an optional file-like object for the backing file.
                          If not provided, file path will be autogenerated based
                          on the metainfo hash.
            :param outdir: an optional directory path to prepend to
                           autogenerated file paths.

            :returns: the newly created HashedFile
        """
        hashes = [multihash.decode(mh) for mh in metainfo['hashes']]
        length = metainfo['length']
        if outfh:
            return cls(fh=outfh, hashes=hashes, length=length)
        if not outfh:
            hf = cls(hashes=hashes, length=length)
            fname = '%s.part' % bytes_to_str(encode_hex(hf.tophash))
            if outdir:
                fname = os.path.join(outdir, fname)
            open(fname, 'a+b').close()
            return cls(fh=open(fname, 'r+b'), hashes=hashes, length=length)

    @classmethod
    def from_binary_metainfo(cls, metainfo, outfh=None, outdir=None) -> HashedFile:
        """ Same as :func:`~playground.file.HashedFile.from_metainfo`
            except it deserializes the metainfo from binary format first.

            :param metainfo: a binary metainfo
        """
        return cls.from_metainfo(bson.loads(metainfo), outfh, outdir)

    def __repr__(self):
        return "<%s(%r, %r)>" % (self.__class__.__name__, self.fh, self.hashes)

if __name__ == '__main__':
    try:
        import ethereum.slogging as slogging
        slogging.configure(config_string=':debug,p2p.discovery:info')
        #slogging.configure(config_string=':debug,p2p:info')
    except:
        import devp2p.slogging as slogging

    import sys
    hf = HashedFile.from_path(sys.argv[1])
    #hf.do_hash()
    #print(hf)
    for h in hf.hashes:
        print(bytes_to_str(h.encode('hex')))
    #print(bytes_to_str(encode_hex(hf.tophash)))
