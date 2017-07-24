import math

import rlp

from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService

class FileSwarmProtocol(BaseProtocol):
    protocol_id = 2
    max_cmd_id = 5
    name = b'fileswarm'
    version = 1

    class bitmap(BaseProtocol.command):
        cmd_id = 0
        structure [
            ('file', rlp.sedes.binary),
            ('bitmap', rlp.sedes.binary),
        ]

    class interested(BaseProtocol.command):
        cmd_id = 1
        structure = [
            ('file', rlp.sedes.binary),
            ('interested', rlp.sedes.bin_endian_int),
        ]

    class choke(BaseProtocol.command):
        cmd_id = 2
        structure [
            ('file', rlp.sedes.binary),
            ('choked', rlp.sedes.bin_endian_int),
        ]

    class have(BaseProtocol.command):
        cmd_id = 3
        structure [
            ('file', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
        ]

    class request(BaseProtocol.command):
        cmd_id = 4
        structure [
            ('file', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
        ]

    class piece(BaseProtocol.command):
        cmd_id = 5
        structure [
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

class FileSession(object):
    def __init__(self, tophash, piece_count):
        self.tophash = tophash
        self.piece_count = piece_count
        self.pieces = {}
        self.peers = []
        self.peer_pieces = {}

    @property
    def bitmap(self):
        bmap = []
        x = 0
        for i in range(self.piece_count):
            x = x << 1 + (i in pieces)
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
    name = b'fileswarm'
    default_config = {}

    wire_protocol = FireSwarmProtocol

    def __init__(self, app):
        super(PlaygroundService, self).__init__(app)
        self.file_sessions = {}

    def on_wire_protocol_start(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.setup_handlers(proto)

        for sess in self.file_session:
            proto.send_bitmap(sess.tophash, sess.bitmap)

    def _setup_handlers(self, proto):
        proto.receive_bitmap_callbacks.append(self.receive_bitmap)

    # handlers
    def receive_bitmap(self, proto, tophash, bitmap):
        if not tophash in self.file_sessions:
            return
        sess = self.file_sessions[tophash]

        theirs = bitmap_to_set(bitmap)
        ours = set(sess.pieces.keys())

        sess.add_peer(proto, theirs)

        only_ours = ours - theirs
        only_theirs = theirs - ours

        if only_theirs:
            peer.send_interested(tophash, 1)

    # API

    def add_session(self, session):
        self.file_sessions[session.tophash] = session

    def del_session(self, tophash):
        del self.file_sessions[tophash]
