
from devp2p.service import BaseService
from devp2p import slogging

from .tcpconsole import startConsole

log = slogging.get_logger(__name__)

class Console(BaseService):
    name = 'console'

    default_config = {
        'p2p': {},
        'console': {
            'listen_host': '127.0.0.1',
            'default_cmd': 'chat',
        },
        'log_wrapper': lambda log, app: log,
    }

    def __init__(self, app):
        super(Console, self).__init__(app)
        self.print_handlers = []
        self.listen_port = self.config['console'].get('listen_port',
            self.config['p2p'].get('listen_port', 30303) - 100)
        self.log = self.config['log_wrapper'](log, self.app)

    def start(self):
        self._start_console()
        super(Console, self).start()

    def print(self, msg):
        for h in self.print_handlers:
            h(msg)

    def _run_cmd(self, cmd, args, reply):
        cmd = 'cmd_' + cmd
        if hasattr(self.app, cmd) and getattr(self.app, cmd):
            getattr(self.app, cmd)(args, reply)
        else:
            reply("No such command %s" % cmd)

    def _start_console(self):
        def on_connect(address, reply):
            self.print_handlers.append(reply)

        def on_cmd(msg, address, reply):
            if msg.startswith('/'):
                msplit = msg.split(' ', 1)
                msplit.append('')
                cmd, args = msplit[:2]
                cmd = cmd[1:]
                self._run_cmd(cmd, args, reply)
            else:
                self._run_cmd(self.config['console']['default_cmd'], msg, reply)

        self.log.info('starting console', port=self.listen_port)
        startConsole(self.listen_port, on_connect, on_cmd,
                     host=self.config['console']['listen_host'])
        self.log.info('started console', port=self.listen_port)
