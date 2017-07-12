import sys
import time
import random

import gevent

import rlp
from rlp.utils import encode_hex, decode_hex, is_integer

from devp2p.app import BaseApp
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.discovery import NodeDiscovery
from devp2p.peermanager import PeerManager
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService, BaseService
from devp2p.utils import colors, COLOR_END
from devp2p import app_helper

try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
except:
    import devp2p.slogging as slogging
    slogging.configure(':debug,p2p.discovery:info')

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
        super(PlaygroundService, self).__init__(app)

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
        

    def on_wire_protocol_start(self, proto):
        self.log('--------------------------------')
        self.log('wire_proto_start', peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        proto.receive_chat_callbacks.append(self.on_receive_chat)
        
        msg = ChatMessage.create(sender=self.address, content=":3")
        gevent.sleep(random.random())
        self.broadcast('chat', msg)
    
    def on_receive_chat(self, proto, chatmsg):
        assert isinstance(chatmsg, ChatMessage)
        assert isinstance(proto, self.wire_protocol)
        self.log('--------------------------------')
        self.log('received chat', msg=chatmsg, peer=encode_hex(proto.peer.remote_pubkey)[:5])
        if (self.ts_filter.check(chatmsg.ts)):
            self.broadcast('chat', chatmsg, origin=proto)


class PlaygroundApp(BaseApp):
    client_name = 'playgrond'
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
