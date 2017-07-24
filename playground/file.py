import hashlib
from multihash import Multihash
from rlp.utils import bytes_to_str

class HashedFile(object):
    chunk_size = 2 ** 19

    def __init__(self, fh):
        self.fh = fh
        self.hashes = None

    def doHash(self):
        self.fh.seek(0, 0)
        hashes = []
        hash_maker = hashlib.sha3_256
        h = hash_maker()

        block_size = min(HashedFile.chunk_size, h.block_size)

        off = 0
        def read():
            nonlocal off
            size = min(h.block_size, HashedFile.chunk_size - off)
            data = self.fh.read(size)
            off += len(data)
            return data

        data = read()
        while data:
            h.update(data)
            if off >= HashedFile.chunk_size:
                hashes.append(Multihash.from_hash(h))
                h = hash_maker()
                off = 0
            data = read()

        if off > 0:
            hashes.append(Multihash.from_hash(h))
        self.hashes = hashes
        self.fh.seek(0, 0)

    @classmethod
    def from_path(cls, path):
        return cls(open(path, 'rb'))

    def __repr__(self):
        return "<%s(%r, %r)>" % (self.__class__.__name__, self.fh, self.hashes)

if __name__ == '__main__':
    import sys
    hf = HashedFile.from_path(sys.argv[1])
    hf.doHash()
    #print(hf)
    for h in hf.hashes:
        print(bytes_to_str(h.encode('hex')))
