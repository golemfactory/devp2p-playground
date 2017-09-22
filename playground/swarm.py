import math
import random
import time
import weakref
import itertools

from multihash import Multihash
import multihash

import gevent

import rlp
from rlp.utils import encode_hex, is_integer

from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService

from .file import HashedFile



CALC_RATE_AFTER_VERIFY = True



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
        length = math.log2(max(s))
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
        self.requests = {} # {piece_no -> (pending_piece, {offset -> length})}
        self.sent = []
        self.recvd = []
        self.rate_up = 0
        self.rate_down = 0


    def calc_rates(self):
        self.rate_up = _calc_rate(self.sent, self.rate_avg_period)
        self.rate_down = _calc_rate(self.recvd, self.rate_avg_period)


    def add_request(self, piece_no, pending_piece, offset, length):
        if not piece_no in self.requests:
            self.requests[piece_no] = ((pending_piece, {}))
        self.requests[piece_no][1][offset] = length


    def del_request(self, piece_no, offset):
        if piece_no in self.requests:
            self.requests[piece_no][1].pop(offset, None)


    def del_piece_requests(self, piece_no):
        self.requests.pop(piece_no, None)


    def del_all_requests(self):
        for pp, reqs in self.requests.values():
            for offset, _ in reqs.items():
                pp.del_request(self.peer, offset)


    def get_rerequests(self):
        return [(piece_no, offset, length) for piece_no, (_, reqs) in self.requests.items()
                                                         for offset, length in reqs.items()]


    @property
    def req_count(self):
        return sum(len(reqs[1]) for reqs in self.requests.values())


    def __repr__(self):
        return '<%s(peer=%r up=%s down=%s)>' % (self.__class__.__name__, self.peer, self.rate_up, self.rate_down)



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
        self.complete_callbacks = []


    @property
    def tophash(self):
        return self.hf.tophash


    @property
    def pieces(self):
        return self.hf.haveset


    def piece_length(self, piece_no):
        return self.hf.get_chunk_size(piece_no)


    def piece_hash(self, piece_no):
        return self.hf.hashes[piece_no]

    def piece_stream(self, piece_no):
        return self.hf.get_chunk_stream(piece_no)


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


    def receive_subpiece(self, recv_time, proto, piece_no, offset, data):
        length = len(data)
        if not CALC_RATE_AFTER_VERIFY:
            self.peers[proto].recvd.append((recv_time, length))
        chunk = self.piece_stream(piece_no)
        chunk.seek(offset)
        chunk.write(data)
        chunk.flush()

        for peer in self.peers.values():
            peer.del_request(piece_no, offset)


    def add_request(self, proto, piece_no, pending_piece, offset, length):
        self.peers[proto].add_request(piece_no, pending_piece, offset, length)


    def send_subpiece(self, send_time, proto, piece_no, offset, length):
        chunk = self.piece_stream(piece_no)
        if not chunk:
            return None
        chunk.seek(offset)
        data = chunk.read(length)
        self.peers[proto].sent.append((send_time, length))
        return data


    def complete_piece(self, proto, piece_no, duplicate_count=1):
        self.hf.haveset.add(piece_no)
        if CALC_RATE_AFTER_VERIFY:
            self.peers[proto].recvd.append((time.time(), self.piece_length(piece_no) / duplicate_count))
        for peer in self.peers.values():
            peer.del_piece_requests(piece_no)


    def add_peer(self, peer, pieces):
        if peer in self.peers:
            return False
        fsp = FileSessionPeer(peer)
        fsp.pieces = pieces
        self.peers[peer] = fsp
        return True


    def del_peer(self, peer):
        fspeer = self.peers.pop(peer, None)
        if fspeer:
            fspeer.del_all_requests()


    def add_complete_callback(self, callback):
        self.complete_callbacks.append(callback)


class PendingPiece(object):
    def __init__(self, log, piece_hash, length, fh):
        self.log = log
        self.piece_hash = piece_hash
        self.length = length
        self.fh = fh
        self.sessions = set()
        #self.peers = weakref.WeakSet() # peers who have it
        self.subpieces = {} # offset -> (len, done, peer_protos)


    def add_request(self, session, piece_no, peer_proto, offset, length):
        self.sessions.add((session, piece_no))
        #self.peers.add(peer_proto)
        if not offset in self.subpieces:
            self.subpieces[offset] = (length, False, set())
        self.subpieces[offset][2].add(peer_proto)


    def del_request(self, peer_proto, offset):
        self.subpieces[offset][2].discard(peer_proto)

        # If we're not done and requested from no peers, delete the entry
        if not self.subpieces[offset][1] and not self.subpieces[offset][2]:
            del self.subpieces[offset]


    def pick_subpiece(self, include_pending=False):
        next_off = 0
        for off in sorted(self.subpieces):
            if off > next_off:
                break
            if include_pending and not self.subpieces[off][1]:
                break;
            next_off += self.subpieces[off][0]

        return next_off if next_off < self.length else None


    def receive_subpiece(self, offset, length):
        if not offset in self.subpieces:
            self.log('unsolicited subpiece', offset=offset, subpieces=self.subpieces)
            return None
        if self.subpieces[offset][0] != length or self.subpieces[offset][1]:
            self.log('weird subpiece', offset=offset, length=length, sp=self.subpieces[offset])
            return None
        self.subpieces[offset] = (length, True, set())
        return self.sessions.copy()


    def check_complete(self):
        return self.pick_subpiece(include_pending=True) is None


    def verify_hash(self):
        return self.piece_hash.verify(self.fh.read(-1))


    def __repr__(self):
        return '<%s(len=%d, hash=%s, subpieces=%s)>' % (self.__class__.__name__, self.length, self.piece_hash, self.subpieces)


    @classmethod
    def from_session(cls, log, session, piece_no):
        return cls(log, session.piece_hash(piece_no), session.piece_length(piece_no),
                   session.piece_stream(piece_no))

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
    total_unchoke_cnt = 4
    period = 10
    optimistic_period_count = 3

    class SessionState(object):
        def __init__(self):
            self.optimistic_lifetime = 0


    def __init__(self, service):
        super(PerSessionTitForTatChokingStrategy, self).__init__(service)
        self.is_stopped = False
        self.sessions = {}


    def _rechoke_session(self, sess):
        if not sess.tophash in self.sessions:
            self.sessions[sess.tophash] = self.SessionState()
        state = self.sessions[sess.tophash]

        def key_up(peer):
            return peer.rate_up
        def key_down(peer):
            return peer.rate_down

        key = key_up if sess.complete else key_down

        for peer in sess.peers.values():
            peer.calc_rates()

        interested_peers = {p for p in sess.peers.values() if p.interested}
        sorted_peers = sorted(interested_peers, key=key, reverse=True)
        self.service.log('sorted peers', peers=sorted_peers)
        unchokes = set(sorted_peers[:self.regular_unchoke_cnt])
        chokes = interested_peers - unchokes
        choke_list = list(chokes)

        if not state.optimistic_lifetime:
            state.optimistic_unchokes = set()
            while choke_list and len(unchokes) + len(state.optimistic_unchokes) < self.total_unchoke_cnt:
                optimistic_peer = random.choice(choke_list)
                state.optimistic_unchokes.add(optimistic_peer)
                choke_list.remove(optimistic_peer)
            state.optimistic_lifetime = self.optimistic_period_count

        state.optimistic_lifetime -= 1

        unchokes |= state.optimistic_unchokes
        chokes -= state.optimistic_unchokes
        unchokes = {p.peer for p in unchokes}
        chokes = {p.peer for p in chokes}
        self.service.log('will unchoke', unchokes=unchokes, chokes=chokes, uninterested={p.peer for p in (set(sess.peers.values()) - interested_peers)})

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



class PieceSelectionStrategy(object):
    def __init__(self, service):
        self.service = service

    def pick(self, sess, proto, available, count):
        pass



class RandomPieceSelectionStrategy(PieceSelectionStrategy):
    def pick(self, sess, proto, available, count):
        count = min(count, len(available))
        return random.sample(available, count)



class RarestFirstPieceSelectionStrategy(PieceSelectionStrategy):
    def pick(self, sess, proto, available, count):
        count = min(count, len(available))
        available = list(available)
        random.shuffle(available) # in case they have equal frequency
        freqs = sorted(((piece_no, sum(piece_no in peer.pieces for peer in sess.peers.values())) for piece_no in available), key=lambda x: x[1])
        self.service.log('piece freqs', freqs=freqs)
        return list(itertools.islice((x[0] for x in freqs), count))



class EndGamePieceSelectionStrategy(PieceSelectionStrategy):
    def pick(self, sess, proto, available, count):
        pending = {piece_no for piece_no, piece_hash in enumerate(sess.hf.hashes) if piece_hash in self.service.pending_pieces}
        peer = sess.peers[proto] # type: FileSessionPeer
        pending &= peer.pieces
        pending -= peer.requests.keys()
        count = min(count, len(pending))
        return random.sample(pending, count)


class BEP3PieceSelectionStrategy(PieceSelectionStrategy):
    def __init__(self, service):
        super(BEP3PieceSelectionStrategy, self).__init__(service)
        self.early = RandomPieceSelectionStrategy(service)
        self.mid = RarestFirstPieceSelectionStrategy(service)
        self.late = EndGamePieceSelectionStrategy(service)


    def pick(self, sess: FileSession, proto, available, count):
        if not sess.pieces:
            # We don't have any complete pieces yet, gotta get one ASAP
            return self.early.pick(sess, proto, available, count)

        if not available:
            # We have requested everything we don't have, so speed up the last bit
            # by means of the end-game mode
            return self.late.pick(sess, proto, available, count)

        return self.mid.pick(sess, proto, available, count)



class FileSwarmService(WiredService):
    name = 'fileswarm'
    default_config = {
        'fileswarm': {
            'choking_strategy': NaiveChokingStrategy,
            'piece_strategy': RandomPieceSelectionStrategy,
            'max_request_per_peer': 3,
            'request_size': None,
        }
    }

    wire_protocol = FileSwarmProtocol


    def __init__(self, app):
        super(FileSwarmService, self).__init__(app)
        self.file_sessions = {}
        self.peers = []
        self.pending_pieces = {}
        choking_strategy = self.config['fileswarm']['choking_strategy']
        piece_strategy = self.config['fileswarm']['piece_strategy']
        self.choking_strategy = choking_strategy(self)
        self.piece_strategy = piece_strategy(self)

        self.max_requests_per_peer = self.config['fileswarm']['max_request_per_peer']
        self.request_size = self.config['fileswarm']['request_size']
        if self.request_size is None:
            self.request_size = HashedFile.chunk_size if CALC_RATE_AFTER_VERIFY else 2 ** 14


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
        for sess in self.file_sessions.values():
            sess.del_peer(proto)


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
            for piece_no, offset, length in sess.peers[proto].get_rerequests():
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

        data = sess.send_subpiece(time.time(), proto, piece_no, offset, length)
        if data:
            proto.send_piece(sess.piece_hash(piece_no).encode(), offset, data)


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
        sessions = pp.receive_subpiece(offset, length)
        if not sessions:
            self.log('invalid subpiece')
            return
        for (sess, piece_no) in sessions:
            self.log('matched session', sess=sess, piece_no=piece_no)
            sess.receive_subpiece(now, proto, piece_no, offset, data)

        if pp.check_complete():
            self.complete_piece(proto, pp)

        for (sess, piece_no) in sessions:
            for peer in list(sess.peers.keys()):
                self.recalc_interest(sess, peer)


    # internal API

    def complete_piece(self, proto, piece):
        self.pending_pieces.pop(piece.piece_hash, None)
        self.log('verifying piece', piece_hash=piece.piece_hash)

        if not piece.verify_hash():
            self.log('bad piece', piece=piece)
            return

        self.log('complete piece', piece_hash=piece.piece_hash)

        sessions_done = set()
        for sess, piece_no in piece.sessions:
            sess.complete_piece(proto, piece_no, len(piece.sessions))
            if sess.complete:
                sessions_done.add(sess)
            for peer in sess.peers.keys():
                peer.send_have(sess.tophash, piece_no)

        for sess in sessions_done:
            self.complete_session(sess)

    def complete_session(self, sess):
        self.log('session completed', sess=sess)
        for cb in sess.complete_callbacks:
            cb(sess)


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

        piece_hash = sess.piece_hash(piece_no)
        if not piece_hash in self.pending_pieces:
            self.pending_pieces[piece_hash] = PendingPiece.from_session(self.log, sess, piece_no)
        pp = self.pending_pieces[piece_hash]
        pp.add_request(sess, piece_no, proto, offset, length)

        sess.add_request(proto, piece_no, pp, offset, length)

        proto.send_request(sess.tophash, piece_no, offset, length)


    def recalc_interest(self, sess, proto):
        peer = sess.peers[proto]
        theirs = peer.pieces
        only_theirs = theirs - sess.pieces

        old_interest = peer.interesting_us
        peer.interesting_us = bool(only_theirs)
        if peer.interesting_us != old_interest:
            proto.send_interested(sess.tophash, peer.interesting_us)

        requests_left = max(0, self.max_requests_per_peer - peer.req_count)
        if requests_left <= 0 or peer.choking_us:
            return

        # finish an existing piece
        pending = {piece_no: sess.piece_hash(piece_no) for piece_no in only_theirs if sess.piece_hash(piece_no) in self.pending_pieces}
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
        # FIXME: do we really want to start multiple pieces?

        only_theirs -= {piece_no for piece_no in only_theirs if sess.piece_hash(piece_no) in self.pending_pieces}
        #only_theirs -= peer.requests.keys()

        to_request = self.piece_strategy.pick(sess, proto, only_theirs, requests_left)
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
