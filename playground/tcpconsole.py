from gevent.server import StreamServer

def startConsole(port, connectHandler, commandHandler):
    def handle(socket, address):
        def reply(msg):
            socket.sendall(msg.encode())
        connectHandler(address, reply)

        socket.sendall(b'Hello\n')
        rfile = socket.makefile(mode='r')

        while True:
            socket.sendall(b'>')
            line = rfile.readline()
            if not line:
                break
            commandHandler(line, address, reply)

        rfile.close()

    server = StreamServer(('127.0.0.1', port), handle)
    server.start()
