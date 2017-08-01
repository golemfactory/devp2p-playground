from gevent.server import StreamServer

def startConsole(port, connectHandler, commandHandler, host='127.0.0.1'):
    def handle(socket, address):
        def reply(msg):
            try:
                socket.sendall((msg + "\n").encode())
            except:
                pass
        connectHandler(address, reply)

        socket.sendall(b'Hello\n')
        rfile = socket.makefile(mode='r')

        while True:
            socket.sendall(b'>')
            line = rfile.readline()
            if not line:
                break
            commandHandler(line.strip(), address, reply)

        rfile.close()

    server = StreamServer((host, port), handle)
    server.start()
