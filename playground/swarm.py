import math
import random

from multihash import Multihash

import rlp
from rlp.utils import encode_hex

from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService

from .file import HashedFile

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

    class cancel(BaseProtocol.command):
        cmd_id = 5
        structure = [
            ('tophash', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
        ]

    class piece(BaseProtocol.command):
        cmd_id = 6
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
                s.add(i * 8 + j)
    return s

class FileSessionPeer(object):
    def __init__(self, peer):
        self.peer = peer
        self.pieces = set()
        self.choked = True
        self.interested = False
        self.choking_us = True
        self.interesting_us = False
        self.requests = set()

class FileSession(object):
    def __init__(self, hashed_file, piece_count=None):
        self.hf = hashed_file
        if piece_count is None and self.hf.hashes:
            piece_count = len(self.hf.hashes)
        if piece_count is None:
            raise ValueError("No piece count")
        self.piece_count = piece_count
        #self.pieces = set()
        self.peers = {}

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
        if i % 8 != 7:
            x = x << (7 - i % 8 )
            bmap.append(x)
        return bytes(bmap)

    def add_peer(self, peer, pieces):
        fsp = FileSessionPeer(peer)
        fsp.pieces = pieces
        self.peers[peer] = fsp


def receive_with_session(fun):
    def wrapper(self, proto, tophash, **kwargs):
        assert isinstance(tophash, bytes)
        if not tophash in self.file_sessions:
            return
        sess = self.file_sessions[tophash]
        if not proto in sess.peers:
            return
        fun(self, proto, sess, **kwargs)
    return wrapper

class FileSwarmService(WiredService):
    name = 'fileswarm'
    default_config = {}

    max_requests_per_peer = 3

    wire_protocol = FileSwarmProtocol

    def __init__(self, app):
        super(FileSwarmService, self).__init__(app)
        self.file_sessions = {}
        self.peers = []
        self.requests = {}

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
        proto.receive_interested_callbacks.append(self.receive_interested)
        proto.receive_choke_callbacks.append(self.receive_choke)
        proto.receive_request_callbacks.append(self.receive_request)
        proto.receive_piece_callbacks.append(self.receive_piece)
        proto.receive_have_callbacks.append(self.receive_have)

    # handlers

    def receive_bitmap(self, proto, tophash, bitmap, is_reply):
        assert isinstance(tophash, bytes)
        assert isinstance(bitmap, bytes)
        if not tophash in self.file_sessions:
            return
        sess = self.file_sessions[tophash]

        if sess and not is_reply:
            proto.send_bitmap(sess.tophash, sess.bitmap, True)

        theirs = bitmap_to_set(bitmap)
        sess.add_peer(proto, theirs)

        self.log('received bitmap', tophash=encode_hex(tophash), bitmap=bitmap, ours=sess.pieces, theirs=theirs)

        self.recalc_interest(sess, proto)

    @receive_with_session
    def receive_interested(self, proto, sess, interested):
        self.log('peer interested', proto=proto, sess=sess, tophash=encode_hex(sess.tophash), interested=interested)
        sess.peers[proto].interested = interested
        self.unchoke(sess, proto)

    @receive_with_session
    def receive_choke(self, proto, sess, choked):

        self.log('peer (un)choking', proto=proto, tophash=encode_hex(sess.tophash), choked=choked)
        sess.peers[proto].choking_us = choked
        if not choked:
            self.recalc_interest(sess, proto)

    @receive_with_session
    def receive_request(self, proto, sess, piece_no):
        self.log('peer requested piece', proto=proto, tophash=encode_hex(sess.tophash), piece_no=piece_no, choked=sess.peers[proto].choked, my_pieces=sess.pieces)
        if sess.peers[proto].choked:
            return
        if not piece_no in sess.pieces:
            return

        chunk = sess.hf.get_chunk_stream(piece_no)
        if chunk:
            data = chunk.read(-1)
            proto.send_piece(data)

    @receive_with_session
    def receive_have(self, proto, sess, piece_no):
        self.log('peer got a piece', proto=proto, tophash=encode_hex(sess.tophash), piece_no=piece_no)
        sess.peers[proto].pieces.add(piece_no)
        self.recalc_interest(sess, proto)

    def receive_piece(self, proto, data):
        h = HashedFile.hash_function()
        h.update(data)
        mh = Multihash.from_hash(h)

        self.log('received piece', proto=proto, mh=mh, requests=self.requests, mh_in=(mh in self.requests))
        if not self.requests.get(mh):
            return
        for (sess, piece_no) in self.requests[mh]:
            self.log('matched session', sess=sess, piece_no=piece_no)
            chunk = sess.hf.get_chunk_stream(piece_no)
            chunk.write(data)
            self.log('written')
            chunk.flush()
            sess.hf.haveset.add(piece_no) # FIXME: should do this closer to HashedFile
            for peer in sess.peers.keys():
                peer.send_have(sess.tophash, piece_no)
                sess.peers[peer].requests.discard(piece_no)
                # TODO: cancel the request?
                self.recalc_interest(sess, peer)

        self.requests[mh] = []


    # internal API

    def unchoke(self, sess, proto):
        sess.peers[proto].choked = False
        proto.send_choke(sess.tophash, False)

    def request(self, sess, proto, piece_no):
        piece_hash = sess.hf.hashes[piece_no]
        if not piece_hash in self.requests:
            self.requests[piece_hash] = []
        self.requests[piece_hash].append((sess, piece_no))

        sess.peers[proto].requests.add(piece_no)

        proto.send_request(sess.tophash, piece_no)

    def recalc_interest(self, sess, proto):
        peer = sess.peers[proto]
        theirs = peer.pieces
        only_theirs = theirs - sess.pieces

        requests_left = self.max_requests_per_peer - len(peer.requests)
        requests_left = min(requests_left, len(only_theirs))

        old_interest = peer.interesting_us
        peer.interesting_us = bool(only_theirs)
        if peer.interesting_us != old_interest:
            proto.send_interested(sess.tophash, peer.interesting_us)

        if only_theirs and requests_left and not peer.choking_us:
            to_request = random.sample(only_theirs, requests_left)
            self.log('will request', ours=sess.pieces, theirs=theirs, only_theirs=only_theirs, to_request=to_request)
            for piece_no in to_request:
                self.request(sess, proto, piece_no)

    # API

    def add_session(self, session):
        self.file_sessions[session.tophash] = session
        for peer in self.peers:
            peer.send_bitmap(session.tophash, session.bitmap, False)

    def del_session(self, tophash):
        del self.file_sessions[tophash]
