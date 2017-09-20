import sys
import time
import random
import datetime
import asyncio
import io
import traceback

import gevent
from gevent.event import AsyncResult
from gevent.fileobject import FileObjectThread

import multihash

import rlp
from rlp.utils import encode_hex, decode_hex, is_integer, str_to_bytes

from devp2p.app import BaseApp
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.discovery import NodeDiscovery
from devp2p.peermanager import PeerManager
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService
from devp2p.utils import colors, COLOR_END, big_endian_to_int
from devp2p import app_helper

from .swarm import FileSwarmService, FileSession, PerSessionTitForTatChokingStrategy, BEP3PieceSelectionStrategy
from .file import HashedFile
from .consvc import Console

try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
    #slogging.configure(config_string=':debug,p2p:info')
except:
    import devp2p.slogging as slogging

log = slogging.get_logger('app')

class ChatMessage(rlp.Serializable):
    fields = [
        ('ts', rlp.sedes.big_endian_int),
        ('sender', rlp.sedes.binary),
        ('content', rlp.sedes.binary),
    ]

    @classmethod
    def create(cls, ts=None, sender=b'', content=""):
        if ts is None:
            ts = int(time.time() * 1000) # ms
        content = content.encode()
        return cls(ts=ts, sender=sender, content=content)

    def __init__(self, ts=None, sender=b'', content=""):
        assert is_integer(ts)
        assert isinstance(sender, bytes)
        assert isinstance(content, bytes)
        super(ChatMessage, self).__init__(ts, sender, content)

    @property
    def content_str(self):
        return self.content.decode()

    def __repr__(self):
        try:
            return '<%s(ts=%d sender=%s content=%s)>' % (self.__class__.__name__,
                    self.ts, encode_hex(self.sender)[:5], self.content_str)
        except:
            return '<%s>' % (self.__class__.__name__)

class PlaygroundProtocol(BaseProtocol):
    protocol_id = 1
    max_cmd_id = 3
    name = b'playground'
    version = 2

    class chat(BaseProtocol.command):
        cmd_id = 0
        structure = [
            ('chatmsg', ChatMessage)
        ]

    class file_chunk(BaseProtocol.command):
        cmd_id = 1
        structure = [
            ('name', rlp.sedes.binary),
            ('data', rlp.sedes.binary),
        ]

    class file_ack(BaseProtocol.command):
        cmd_id = 2
        structure = [
            ('name', rlp.sedes.binary),
            ('window', rlp.sedes.big_endian_int),
        ]

    class file_metainfo(BaseProtocol.command):
        cmd_id = 3
        structure = [
            ('data', rlp.sedes.binary),
        ]

class DuplicateFilter(object):
    def __init__(self):
        self.set = set()

    def check(self, item):
        if item in self.set:
            return False
        self.set.add(item)
        return True

class PlaygroundService(WiredService):
    name = 'playgroundservice'
    default_config = {}

    wire_protocol = PlaygroundProtocol

    def __init__(self, app):
        self.config = app.config
        self.address = privtopub_raw(decode_hex(self.config['node']['privkey_hex']))
        self.ts_filter = DuplicateFilter()
        self.chat_handlers = []
        self.pending_connections = {}
        self.files_in = {}
        self.files_out = {}

        super(PlaygroundService, self).__init__(app)

    def start(self):
        super(PlaygroundService, self).start()

    def log(self, text, **kargs):
        node_num = self.config['node_num']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "NODE%d" % node_num,
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        log.debug(msg)

    def broadcast(self, cmd, *args, origin=None):
        self.log('broadcasting', cmd=cmd, args=args)
        self.app.services.peermanager.broadcast(PlaygroundProtocol, cmd,
                args=args, exclude_peers = [origin.peer] if (origin is not None) else [])

    def get_peer(self, pubkey):
        assert pubkey != self.address

        future = AsyncResult()

        peermanager = self.app.services.peermanager

        for p in peermanager.peers:
            if p and p.remote_pubkey == pubkey and p.protocols:
                future.set(p)
                return future

        if (pubkey in self.pending_connections):
            return self.pending_connections[pubkey]

        self.pending_connections[pubkey] = future

        nodeid = big_endian_to_int(pubkey)
        kademlia_proto = self.app.services.discovery.protocol.kademlia
        kademlia_proto.find_node(nodeid)

        def try_connect():
            gevent.sleep(1)
            neighs = kademlia_proto.routing.neighbours(nodeid, 2)
            neighs = [n for n in neighs if n.pubkey == pubkey]
            if not neighs:
                self.log('no neighs', nodeid=nodeid)
                del self.pending_connections[pubkey]
                future.set(None)
            neigh = neighs[0]
            if not peermanager.connect((neigh.address.ip, neigh.address.tcp_port), neigh.pubkey):
                self.log('cannot connect', neigh=neigh)
                self.pending_connections[pubkey]
                future.set(None)
            #return [p for p in peermanager.peers if p.remote_pubkey == pubkey][0]

        gevent.spawn(try_connect)
        
        return future

    def send_chat(self, text):
        msg = ChatMessage.create(sender=self.address, content=text)
        self.broadcast('chat', msg)

    def send_direct_msg(self, target_pubkey, text):
        msg = ChatMessage.create(sender=self.address, content=text)
        peer = self.get_peer(target_pubkey).get()
        if peer:
            self.log("sending DM", peer=peer, msg=msg)
            peer.protocols[PlaygroundProtocol].send_chat(msg)
            return True
        self.log("NOT sending DM", peer=peer, peerb=bool(peer), msg=msg)
        return False

    def send_file(self, target_pubkey, name):
        name = str_to_bytes(name)
        f_raw = open(name, 'rb')
        f = FileObjectThread(f_raw, 'rb')
        #chunk_size = 2**24
        init_win_size = 2**20
        max_chunk_size = 2**20

        peer = self.get_peer(target_pubkey).get()
        if not peer:
            return False

        offset = 0
        self.files_out[target_pubkey, name] = init_win_size
        while peer:
            win_end = self.files_out[target_pubkey, name]
            win_size = win_end - offset
            chunk_size = min(win_size, max_chunk_size)
            #self.log('chunk', chunk_size=chunk_size, offset=offset, win_end=win_end, win_size=win_size, target_pubkey=target_pubkey, name=name)
            if chunk_size > 0:
                data = f.read(chunk_size)
                offset += chunk_size

                peer.protocols[PlaygroundProtocol].send_file_chunk(name, data)
                if not data:
                    del self.files_out[target_pubkey, name]
                    return True
            else:
                gevent.sleep(0.1) #FIXME wake up only when window increases
        del self.files_out[target_pubkey, name]
        return False


    def cmd_msg(self, args, reply):
        asplit = args.split(' ', 1)
        asplit.append('')
        to, text = asplit[:2]
        targets = [p.remote_pubkey for p in self.app.services.peermanager.peers
                   if encode_hex(p.remote_pubkey).startswith(to)]
        if len(targets) == 1:
            self.send_direct_msg(targets[0], text)
        elif targets:
            reply(str([encode_hex(t) for t in targets]))
        elif len(to) == 128:
            ret = self.send_direct_msg(decode_hex(to), text)
            self.log('cmd_msg sent?', ret=ret)
            if ret:
                reply("sent")
            else:
                reply("fail")
        else:
            reply("no such target %s" % to)

    def cmd_file(self, args, reply):
        asplit = args.split(' ', 1)
        asplit.append('')
        to, name = asplit[:2]
        targets = [p.remote_pubkey for p in self.app.services.peermanager.peers
                   if encode_hex(p.remote_pubkey).startswith(to)]
        if len(targets) > 1:
            reply(str([encode_hex(t) for t in targets]))
        if targets:
            target = targets[0]
        else:
            target = decode_hex(to)
        ret = self.send_file(target, name)
        reply('sent file: %s' % ret)

    def cmd_seed(self, args, reply):
        try:
            filename = str_to_bytes(args)
            f = open(filename, 'rb')
            f = FileObjectThread(f, 'rb')
            f = io.BufferedReader(f)
            hf = HashedFile(fh=f)
            reply(encode_hex(hf.tophash))
            fs = FileSession(hf)
            self.app.services.fileswarm.add_session(fs)
            self.broadcast('file_metainfo', hf.binary_metainfo())
        except FileNotFoundError:
            reply(traceback.format_exc())

    def on_wire_protocol_start(self, proto):
        self.log('--------------------------------')
        self.log('wire_proto_start', peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        proto.receive_chat_callbacks.append(self.on_receive_chat)
        proto.receive_file_chunk_callbacks.append(self.on_receive_file)
        proto.receive_file_ack_callbacks.append(self.on_receive_file_ack)
        proto.receive_file_metainfo_callbacks.append(self.on_receive_file_metainfo)

        if proto.peer.remote_pubkey in self.pending_connections:
            future = self.pending_connections[proto.peer.remote_pubkey]
            if not future.ready():
                del self.pending_connections[proto.peer.remote_pubkey]
                future.set(proto.peer)

        gevent.sleep(random.random())
        self.send_chat(":3")

    def on_receive_chat(self, proto, chatmsg):
        assert isinstance(chatmsg, ChatMessage)
        assert isinstance(proto, self.wire_protocol)
        peer = encode_hex(proto.peer.remote_pubkey)[:5]
        sender = encode_hex(chatmsg.sender)[:5]
        self.log('--------------------------------')
        self.log('received chat', msg=chatmsg, peer=peer)

        if not self.ts_filter.check(chatmsg.ts):
            return

        self.app.services.console.print("{0:%H:%M:%S} <{1}> {2}".format(
                    datetime.datetime.fromtimestamp(chatmsg.ts / 1000),
                    sender,
                    chatmsg.content_str,
                ))

        self.broadcast('chat', chatmsg, origin=proto)

    def on_receive_file(self, proto, name, data):
        assert isinstance(name, bytes)
        assert isinstance(data, bytes)
        window_size = 2**24

        sender = proto.peer.remote_pubkey
        if not name in self.files_in:
            f_raw = open(name, 'wb')
            f = FileObjectThread(f_raw, 'wb')
            self.files_in[name] = (f, sender, window_size, time.time())

        f, orig_sender, _, ts = self.files_in[name]
        if orig_sender != sender:
            self.log('recv-file wrong sender', f=f, name=name, sender=sender, orig_sender=orig_sender)
            return

        if len(data) == 0:
            self.log('download finished', elapsed=(time.time() - ts))
            del self.files_in[name]
            f.close()
        else:
            f.write(data)

            f, s, win_end, ts = self.files_in[name]
            win_end += len(data)
            self.files_in[name] = f, s, win_end, ts
            proto.send_file_ack(name, win_end)

    def on_receive_file_ack(self, proto, name, window):
        assert isinstance(name, bytes)
        assert is_integer(window)

        sender = proto.peer.remote_pubkey
        if not ((sender, name) in self.files_out):
            self.log('wild file ack', sender=sender, name=name, window=window)
            return
        self.files_out[sender, name] = window

    def on_receive_file_metainfo(self, proto, data):
        assert isinstance(data, bytes)
        hf = HashedFile.from_binary_metainfo(data)
        #tophash = multihash.digest(data, multihash.Func.sha3_256).encode(None)
        self.log("receiving metainfo", tophash=hf.tophash, ts=time.time())
        fs = FileSession(hf)
        def cb(sess):
            self.app.services.console.print('{0:%H:%M:%S} session complete {1}'.format(datetime.datetime.now(), encode_hex(hf.tophash)))
        fs.add_complete_callback(cb)
        if self.app.services.fileswarm.add_session(fs):
            self.app.services.console.print('{0:%H:%M:%S} new metainfo {1}'.format(datetime.datetime.now(), encode_hex(hf.tophash)))
            self.broadcast('file_metainfo', data)

class PlaygroundApp(BaseApp):
    client_name = 'playground'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None

    services = [NodeDiscovery, PeerManager, PlaygroundService, FileSwarmService, Console]
    #services = [NodeDiscovery, PeerManager, PlaygroundService]

    def __init__(self, config=default_config):
        config['fileswarm']['choking_strategy'] = PerSessionTitForTatChokingStrategy
        config['fileswarm']['piece_strategy'] = BEP3PieceSelectionStrategy
        super(PlaygroundApp, self).__init__(config)
        #for service in PlaygroundApp.services:
        #    assert issubclass(service, BaseService)
        #    if service.name not in self.services:
        #        service.register_with_app(self)
        #    assert service.name in self.services

    def cmd_chat(self, args, reply):
        self.services.playgroundservice.send_chat(args)

    def cmd_msg(self, args, reply):
        self.services.playgroundservice.cmd_msg(args, reply)

    def cmd_file(self, args, reply):
        self.services.playgroundservice.cmd_file(args, reply)

    def cmd_seed(self, args, reply):
        self.services.playgroundservice.cmd_seed(args, reply)

    def cmd_rates(self, args, reply):
        for sess in self.services.fileswarm.file_sessions.values():
            for proto, peer in sess.peers.items():
                peer.calc_rates()
                reply('rates up: %f down: %f to %s' % (peer.rate_up, peer.rate_down, proto))

if __name__ == '__main__':
    #app_helper.run(PlaygroundApp, PlaygroundService, num_nodes=2, max_peers=1, min_peers=1)
    app_helper.run(PlaygroundApp, PlaygroundService)
