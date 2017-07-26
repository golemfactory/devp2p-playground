import math

import rlp

from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService

class FileSwarmProtocol(BaseProtocol):
    protocol_id = 2
    max_cmd_id = 6
    name = b'fileswarm'
    version = 1

    class bitmap(BaseProtocol.command):
        cmd_id = 0
        structure = [
            ('tophash', rlp.sedes.binary),
            ('bitmap', rlp.sedes.binary),
            ('is_reply', rlp.sedes.big_endian_int),
        ]

    class interested(BaseProtocol.command):
        cmd_id = 1
        structure = [
            ('tophash', rlp.sedes.binary),
            ('interested', rlp.sedes.big_endian_int),
        ]

    class choke(BaseProtocol.command):
        cmd_id = 2
        structure = [
            ('tophash', rlp.sedes.binary),
            ('choked', rlp.sedes.big_endian_int),
        ]

    class have(BaseProtocol.command):
        cmd_id = 3
        structure = [
            ('tophash', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
        ]

    class request(BaseProtocol.command):
        cmd_id = 4
        structure = [
            ('tophash', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
        ]

    class piece(BaseProtocol.command):
        cmd_id = 5
        structure = [
            ('data', rlp.sedes.binary),
        ]

def set_to_bitmap(s, length=None):
    if not length:
        length = log2(max(s))
    bmap = bytes(math.ceil(length / 8))
    for x in s:
        byte = x / 8
        bit = x % 8
        bmap[byte] |= 0x80 >> bit
    return bmap

def bitmap_to_set(bmap):
    s = set()
    for i in range(len(bmap)):
        for j in range(8):
            if bmap[i] & (0x80 >> j):
                s.add(j * 8 + i)
    return s

class FileSession(object):
    def __init__(self, hashed_file, piece_count=None):
        self.hf = hashed_file
        if piece_count is None and self.hf.hashes:
            piece_count = len(self.hf.hashes)
        if piece_count is None:
            raise ValueError("No piece count")
        self.piece_count = piece_count
        #self.pieces = set()
        self.peers = []
        self.peer_pieces = {}

    @property
    def tophash(self):
        return self.hf.tophash

    @property
    def pieces(self):
        return self.hf.haveset

    @property
    def bitmap(self):
        bmap = []
        x = 0
        for i in range(self.piece_count):
            x = (x << 1) + (i in self.pieces)
            if i % 8 == 7:
                bmap.append(x)
                x = 0
        if x % 8 != 7:
            bmap.append(x)
        return bytes(bmap)

    def add_peer(self, peer, pieces):
        self.peers.append(peer)
        self.peer_pieces[peer] = pieces

class FileSwarmService(WiredService):
    name = 'fileswarm'
    default_config = {}

    wire_protocol = FileSwarmProtocol

    def __init__(self, app):
        super(FileSwarmService, self).__init__(app)
        self.file_sessions = {}
        self.peers = []

    def log(self, text, **kargs):
        self.app.services.playgroundservice.log(text, **kargs)

    def on_wire_protocol_start(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log("hello")
        self.peers.append(proto)
        self._setup_handlers(proto)

        for sess in self.file_sessions.values():
            self.log("send_bitmap", sess=sess)
            proto.send_bitmap(sess.tophash, sess.bitmap, False)

    def _setup_handlers(self, proto):
        proto.receive_bitmap_callbacks.append(self.receive_bitmap)

    # handlers
    def receive_bitmap(self, proto, tophash, bitmap, is_reply):
        if not tophash in self.file_sessions:
            return
        sess = self.file_sessions[tophash]

        if sess and not is_reply:
            proto.send_bitmap(sess.tophash, sess.bitmap, True)

        theirs = bitmap_to_set(bitmap)
        ours = sess.pieces

        sess.add_peer(proto, theirs)

        only_ours = ours - theirs
        only_theirs = theirs - ours

        self.log('received bitmap', tophash=tophash, bitmap=bitmap, ours=ours, theirs=theirs)

        if only_theirs:
            proto.send_interested(tophash, 1)

    # API

    def add_session(self, session):
        self.file_sessions[session.tophash] = session
        for peer in self.peers:
            peer.send_bitmap(session.tophash, session.bitmap, False)

    def del_session(self, tophash):
        del self.file_sessions[tophash]
