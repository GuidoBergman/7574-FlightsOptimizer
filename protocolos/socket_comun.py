import socket

STATUS_OK = 0
STATUS_ERR = -1

class SocketComun:
    def __init__(self,sock=None, was_splited=False):
        # sock param is used only to build the socket from one wich is already initialized
        if not sock:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._was_splited = False
        else:
            self._socket = sock
            self._was_splited = was_splited

    def bind_and_listen(self, host, port, listen_backlog):
        self._socket.bind((host, port))
        self._socket.listen(listen_backlog)

    def set_timeout(self, timeout):
        self._socket.settimeout(timeout)

    def accept(self):
        """
        Accept new connections

        Function blocks until a connection is made.
        Then connection created is printed and returned
        """
        c, addr = self._socket.accept()
        return SocketComun(c), addr

    def connect(self, host, port):
        self._socket.connect((host, port))

    def receive(self, size):
        received_bytes = 0
        chunks = []
        while received_bytes < size:
            if not self._was_splited:
                chunk = self._socket.recv(size - received_bytes)
            else:
                chunk = self._socket.read(size - received_bytes)
            if chunk == b'':
                return STATUS_ERR, None
            
            chunks.append(chunk)
            received_bytes += len(chunk)

        buffer = b''.join(chunks)
        
        
        return STATUS_OK, buffer

    def send(self, buffer, size):
        sent_bytes = 0
        while sent_bytes < size:
            if not self._was_splited:
                sent = self._socket.send(buffer)
            else: 
                sent = self._socket.write(buffer)
            if sent == 0:
                return STATUS_ERR, None 

            sent_bytes += sent

        return STATUS_OK    

    def split(self):
        """
        Splits between one SocketComun for reading and one for writing
        """ 
        socket_send = self._socket.makefile('wb')
        socket_send = SocketComun(socket_send, was_splited=True)
        socket_rcv = self._socket.makefile('rb')
        socket_rcv = SocketComun(socket_rcv, was_splited=True)
        return socket_send, socket_rcv
        

    def close(self):
        """
        Close connecton of the server socket
        """
        self._socket.close()
