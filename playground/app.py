import sys
import time
import random
import datetime
import asyncio

import gevent
from gevent.event import AsyncResult

import rlp
from rlp.utils import encode_hex, decode_hex, is_integer

from devp2p.app import BaseApp
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.discovery import NodeDiscovery
from devp2p.peermanager import PeerManager
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService
from devp2p.utils import colors, COLOR_END, big_endian_to_int
from devp2p import app_helper

from .tcpconsole import startConsole

try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
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
    max_cmd_id = 1
    name = b'playground'
    version = 1

    class chat(BaseProtocol.command):
        cmd_id = 0
        structure = [
            ('chatmsg', ChatMessage)
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

        super(PlaygroundService, self).__init__(app)

    def start(self):
        self._start_console()
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
            if p.remote_pubkey == pubkey:
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
                log('no neighs', nodeid=nodeid)
                future.set(None)
            neigh = neighs[0]
            if not peermanager.connect((neigh.address.ip, neigh.address.tcp_port), neigh.pubkey):
                log('cannot connect', neigh=neigh)
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
        return False

    def _start_console(self):
        def on_connect(address, reply):
            self.chat_handlers.append(reply)
        def on_cmd(msg, address, reply):
            if msg.startswith('/'):
                msplit = msg.split(' ', 1)
                msplit.append('')
                cmd, args = msplit[:2]
                cmd = 'cmd_' + cmd[1:]
                if hasattr(self, cmd) and getattr(self, cmd):
                    getattr(self, cmd)(args, reply)
                else:
                    reply("No such command %s" % cmd)
            else:
                self.send_chat(msg)

        cons_port = self.config['p2p']['listen_port'] - 100
        self.log('starting console', port=cons_port)
        startConsole(cons_port, on_connect, on_cmd)
        self.log('started console', port=cons_port)

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
        else:
            ret = self.send_direct_msg(decode_hex(to), text)
            self.log('cmd_msg sent?', ret=ret)
            if ret:
                reply("sent")
            else:
                reply("fail")

    def on_wire_protocol_start(self, proto):
        self.log('--------------------------------')
        self.log('wire_proto_start', peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        proto.receive_chat_callbacks.append(self.on_receive_chat)

        if proto.peer.remote_pubkey in self.pending_connections:
            future = self.pending_connections[proto.peer.remote_pubkey]
            if not future.ready():
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

        for h in self.chat_handlers:
            h("{0:%H:%M:%S} <{1}> {2}".format(
                    datetime.datetime.fromtimestamp(chatmsg.ts / 1000),
                    sender,
                    chatmsg.content_str,
                ))

        self.broadcast('chat', chatmsg, origin=proto)

class PlaygroundApp(BaseApp):
    client_name = 'playground'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None

    #services = [NodeDiscovery, PeerManager, PlaygroundService]

    def __init__(self, config=default_config):
        super(PlaygroundApp, self).__init__(config)
        #for service in PlaygroundApp.services:
        #    assert issubclass(service, BaseService)
        #    assert service.name not in self.services
        #    service.register_with_app(self)
        #    assert service.name in self.services

if __name__ == '__main__':
    #app_helper.run(PlaygroundApp, PlaygroundService, num_nodes=2, max_peers=1, min_peers=1)
    app_helper.run(PlaygroundApp, PlaygroundService)
