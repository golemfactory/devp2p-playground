import math
import random
import time
import weakref

from multihash import Multihash
import multihash

import gevent

import rlp
from rlp.utils import encode_hex, is_integer

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
            ('offset', rlp.sedes.big_endian_int),
            ('length', rlp.sedes.big_endian_int),
        ]

    class cancel(BaseProtocol.command):
        cmd_id = 5
        structure = [
            ('tophash', rlp.sedes.binary),
            ('piece_no', rlp.sedes.big_endian_int),
            ('offset', rlp.sedes.big_endian_int),
            ('length', rlp.sedes.big_endian_int),
        ]

    class piece(BaseProtocol.command):
        cmd_id = 6
        structure = [
            ('piecehash', rlp.sedes.binary),
            ('offset', rlp.sedes.big_endian_int),
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

def _calc_rate(queue, period):
    now = time.time()
    deadline = now - period

    # Get rid of anything older than period
    while queue and queue[0][0] < deadline:
        queue.pop(0)

    # Calc average of the rest
    total = sum(x[1] for x in queue)
    return total / period

class FileSessionPeer(object):
    rate_avg_period = 20

    def __init__(self, peer):
        self.peer = peer
        self.pieces = set()
        self.choked = True
        self.interested = False
        self.choking_us = True
        self.interesting_us = False
        self.requests = {}
        self.sent = []
        self.recvd = []

    def calc_rates(self):
        self.rate_up = _calc_rate(self.sent, self.rate_avg_period)
        self.rate_down = _calc_rate(self.recvd, self.rate_avg_period)

class FileSession(object):
    def __init__(self, hashed_file, piece_count=None):
        self.hf = hashed_file
        if piece_count is None and self.hf.hashes:
            piece_count = len(self.hf.hashes)
        if piece_count is None:
            raise ValueError("No piece count")
        self.piece_count = piece_count
        #self.pieces = set()
        self.peers = weakref.WeakKeyDictionary()

    @property
    def tophash(self):
        return self.hf.tophash

    @property
    def pieces(self):
        return self.hf.haveset

    def piece_length(self, piece_no):
        return self.hf.get_chunk_size(piece_no)

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

    @property
    def complete(self):
        return len(self.pieces) == self.piece_count

    def add_peer(self, peer, pieces):
        if peer in self.peers:
            return False
        fsp = FileSessionPeer(peer)
        fsp.pieces = pieces
        self.peers[peer] = fsp
        return True

class PendingPiece(object):
    def __init__(self, piece_hash, length, fh):
        self.piece_hash = piece_hash
        self.length = length
        self.fh = fh
        self.sessions = set()
        self.peers = weakref.WeakSet() # peers who have it
        self.subpieces = {} # offset -> (len, done)

    def pick_subpiece(self, include_pending=False):
        #FIXME what about done subpieces?
        next_off = 0
        for off in sorted(self.subpieces):
            if off > next_off:
                break
            if include_pending and not self.subpieces[off][1]:
                break;
            next_off += self.subpieces[off][0]

        return next_off if next_off < self.length else None

    def check_complete(self):
        return self.pick_subpiece(include_pending=True) is None

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

class ChokingStrategy(object):
    def __init__(self, service):
        self.service = service

    def start(self):
        pass
    def stop(self):
        pass
    def peer_interested(self, sess, proto):
        pass

class NaiveChokingStrategy(ChokingStrategy):
    def peer_interested(self, sess, proto):
        if sess.peers[proto].interested:
            self.service.unchoke(sess, proto)

class PerSessionTitForTatChokingStrategy(ChokingStrategy):
    regular_unchoke_cnt = 3
    optimistic_unchoke_cnt = 1
    period = 10
    optimistic_period_count = 3

    def __init__(self, service):
        super(PerSessionTitForTatChokingStrategy, self).__init__(service)
        self.is_stopped = False
        self.optimistic_lifetime = 0

    def _rechoke_session(self, sess):
        def key_up(peer):
            return peer.rate_up
        def key_down(peer):
            return peer.rate_down

        key = key_up if sess.complete else key_down

        for peer in sess.peers.values():
            peer.calc_rates()
        unchokes = sorted(sess.peers.values(), key=key, reverse=True)[:self.regular_unchoke_cnt]
        unchokes = set(map(lambda peer: peer.peer, unchokes))
        chokes = set(sess.peers.keys()) - unchokes

        if self.optimistic_lifetime:
            self.optimistic_lifetime -= 1
        else:
            self.optimistic_unchokes = set()
            choke_list = list(chokes)
            while choke_list and len(self.optimistic_unchokes) < self.optimistic_unchoke_cnt:
                optimistic_peer = random.choice(choke_list)
                self.optimistic_unchokes.add(optimistic_peer)
                choke_list.remove(optimistic_peer)
                chokes.discard(optimistic_peer)
            self.optimistic_lifetime = self.optimistic_period_count

        unchokes |= self.optimistic_unchokes
        self.service.log('will unchoke', unchokes=unchokes)

        for proto in sess.peers:
            if proto in unchokes:
                self.service.unchoke(sess, proto)
            else:
                self.service.choke(sess, proto)

    def _rechoke_loop(self):
        while not self.is_stopped:
            for sess in self.service.file_sessions.values():
                self._rechoke_session(sess)
            gevent.sleep(self.period)

    def start(self):
        self.greenlet = gevent.spawn(self._rechoke_loop)

    def stop(self):
        if self.is_stopped:
            return
        self.is_stopped = True
        try:
           self.greenlet.kill()
        except gevent.GreenletExit:
            pass

class FileSwarmService(WiredService):
    name = 'fileswarm'
    default_config = {
        'fileswarm': {'choking_strategy': NaiveChokingStrategy}
    }

    max_requests_per_peer = 3
    request_size = 2 ** 14

    wire_protocol = FileSwarmProtocol

    def __init__(self, app):
        super(FileSwarmService, self).__init__(app)
        self.file_sessions = {}
        self.peers = []
        self.pending_pieces = {}
        choking_strategy = self.config['fileswarm']['choking_strategy']
        self.choking_strategy = choking_strategy(self)

    def log(self, text, **kargs):
        self.app.services.playgroundservice.log(text, **kargs)

    def start(self):
        super(FileSwarmService, self).start()
        self.choking_strategy.start()

    def stop(self):
        self.choking_strategy.stop()
        super(FileSwarmService, self).stop()

    def on_wire_protocol_start(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log("hello")
        self.peers.append(proto)
        self._setup_handlers(proto)

        for sess in self.file_sessions.values():
            self.log("send_bitmap", sess=sess)
            proto.send_bitmap(sess.tophash, sess.bitmap, False)

    def on_wire_protocol_stop(self, proto):
        self.log('bye', peer=proto)
        self.peers.remove(proto)
        for sess in self.file_sessions:
            sess.peers.pop(proto, None)

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

        self.log('received bitmap', tophash=encode_hex(tophash), bitmap=bitmap,
                                    ours=sess.pieces, theirs=theirs)

        self.recalc_interest(sess, proto)

    @receive_with_session
    def receive_interested(self, proto, sess, interested):
        self.log('peer interested', proto=proto, sess=sess, tophash=encode_hex(sess.tophash),
                                    interested=interested)
        sess.peers[proto].interested = interested
        self.choking_strategy.peer_interested(sess, proto)

    @receive_with_session
    def receive_choke(self, proto, sess, choked):

        self.log('peer (un)choking', proto=proto, tophash=encode_hex(sess.tophash),
                                     choked=choked)
        sess.peers[proto].choking_us = choked
        if not choked:
            # re-send requests they ignored when they were choking us
            # note: if we have received the piece in the meantime, it should've
            #       been removed from all peers' requests by receive_piece
            for piece_no, (pp, reqs) in sess.peers[proto].requests.items():
                for offset, length in reqs.items():
                    proto.send_request(sess.tophash, piece_no, offset, length)
            self.recalc_interest(sess, proto)

    @receive_with_session
    def receive_request(self, proto, sess, piece_no, offset, length):
        self.log('peer requested piece', proto=proto, tophash=encode_hex(sess.tophash),
                                         piece_no=piece_no, choked=sess.peers[proto].choked,
                                         my_pieces=sess.pieces, offset=offset, length=length)
        if sess.peers[proto].choked:
            return
        if not piece_no in sess.pieces:
            return

        chunk = sess.hf.get_chunk_stream(piece_no)
        if chunk:
            chunk.seek(offset)
            data = chunk.read(length)
            sess.peers[proto].sent.append((time.time(), length))
            proto.send_piece(sess.hf.hashes[piece_no].encode(), offset, data)

    @receive_with_session
    def receive_have(self, proto, sess, piece_no):
        self.log('peer got a piece', proto=proto, tophash=encode_hex(sess.tophash),
                                     piece_no=piece_no)
        sess.peers[proto].pieces.add(piece_no)
        self.recalc_interest(sess, proto)

    def receive_piece(self, proto, piecehash, offset, data):
        assert isinstance(piecehash, bytes)
        assert is_integer(offset)
        assert isinstance(data, bytes)

        mh = multihash.decode(piecehash)
        length = len(data)

        now = time.time()
        self.log('received piece', proto=proto, mh=mh, requests=self.pending_pieces,
                                   mh_in=(mh in self.pending_pieces), offset=offset,
                                   length=length)

        pp = self.pending_pieces.get(mh)
        if not pp:
            return
        if not offset in pp.subpieces:
            return
        if pp.subpieces[offset][0] != length or pp.subpieces[offset][1]:
            return
        pp.subpieces[offset] = (length, True)
        for (sess, piece_no) in pp.sessions:
            self.log('matched session', sess=sess, piece_no=piece_no)
            sess.peers[proto].recvd.append((now, length))
            chunk = sess.hf.get_chunk_stream(piece_no)
            chunk.seek(offset)
            chunk.write(data)
            self.log('written')
            chunk.flush()

            for peer in sess.peers.keys():
                if piece_no in sess.peers[peer].requests:
                    sess.peers[peer].requests[piece_no][1].pop(offset, None)
                    # TODO: cancel the request?

        if pp.check_complete():
            self.complete_piece(pp)

        for (sess, piece_no) in pp.sessions:
            for peer in list(sess.peers.keys()):
                self.recalc_interest(sess, peer)


    # internal API

    def complete_piece(self, piece):
        self.pending_pieces.pop(piece.piece_hash, None)

        if not piece.piece_hash.verify(piece.fh.read(-1)):
            self.log('bad piece', piece=piece)
            return

        self.log('complete piece', piece_hash=piece.piece_hash)
        for sess, piece_no in piece.sessions:
            sess.hf.haveset.add(piece_no) # FIXME: should do this closer to HashedFile
            for peer in sess.peers.keys():
                peer.send_have(sess.tophash, piece_no)
                sess.peers[peer].requests.pop(piece_no, None)
            if sess.complete:
                self.log('session completed', sess=sess)

    def unchoke(self, sess, proto):
        if not sess.peers[proto].choked:
            return
        sess.peers[proto].choked = False
        proto.send_choke(sess.tophash, False)

    def choke(self, sess, proto):
        if sess.peers[proto].choked:
            return
        sess.peers[proto].choked = True
        proto.send_choke(sess.tophash, True)

    def request(self, sess, proto, piece_no, offset=0, length=None):
        if not length:
            length = sess.hf.chunk_size

        length = min(length, sess.piece_length(piece_no) - offset)

        piece_hash = sess.hf.hashes[piece_no]
        if not piece_hash in self.pending_pieces:
            self.pending_pieces[piece_hash] = PendingPiece(piece_hash, sess.piece_length(piece_no), sess.hf.get_chunk_stream(piece_no))
        pp = self.pending_pieces[piece_hash]
        pp.sessions.add((sess, piece_no))
        pp.peers.add(proto)
        pp.subpieces[offset] = (length, False)

        if not piece_no in sess.peers[proto].requests:
            sess.peers[proto].requests[piece_no] = ((pp, {}))
        sess.peers[proto].requests[piece_no][1][offset] = length

        proto.send_request(sess.tophash, piece_no, offset, length)

    def recalc_interest(self, sess, proto):
        peer = sess.peers[proto]
        theirs = peer.pieces
        only_theirs = theirs - sess.pieces

        old_interest = peer.interesting_us
        peer.interesting_us = bool(only_theirs)
        if peer.interesting_us != old_interest:
            proto.send_interested(sess.tophash, peer.interesting_us)


        requests_left = max(0, self.max_requests_per_peer - sum(len(reqs[1]) for reqs in peer.requests.values()))
        if requests_left <= 0 or peer.choking_us:
            return

        # finish an existing piece
        pending = {piece_no: sess.hf.hashes[piece_no] for piece_no in only_theirs if sess.hf.hashes[piece_no] in self.pending_pieces}
        while pending and requests_left:
            piece_no = random.choice(list(pending.keys()))
            piece_hash = pending.pop(piece_no)
            offset = self.pending_pieces[piece_hash].pick_subpiece()

            while offset is not None and requests_left:
                self.log('will request', piece_no=piece_no, offset=offset)
                self.request(sess, proto, piece_no, offset, self.request_size)
                requests_left -= 1
                offset = self.pending_pieces[piece_hash].pick_subpiece()

        # request a new piece

        # FIXME: do we really want to start mutiple pieces?

        only_theirs -= peer.requests.keys()
        requests_left = min(requests_left, len(only_theirs))

        if only_theirs:
            to_request = random.sample(only_theirs, requests_left)
            self.log('will request', ours=sess.pieces, theirs=theirs, only_theirs=only_theirs,
                                     to_request=to_request)
            for piece_no in to_request:
                self.request(sess, proto, piece_no, 0, self.request_size)

    # API

    def add_session(self, session):
        if session.tophash in self.file_sessions:
            return False
        self.file_sessions[session.tophash] = session
        for peer in self.peers:
            peer.send_bitmap(session.tophash, session.bitmap, False)
        return True

    def del_session(self, tophash):
        del self.file_sessions[tophash]
