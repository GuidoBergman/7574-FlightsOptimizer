import socket

STATUS_OK = 0
STATUS_ERR = -1

class SocketComunUDP:
    def __init__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
     
           

    def bind(self, host, port):
        self._socket.bind((host, port))


    def set_timeout(self, timeout):
        self._socket.settimeout(timeout)

    def receive(self, size):
        received_bytes = 0
        chunks = []
        while received_bytes < size:
            chunk, _ = self._socket.recvfrom(size - received_bytes)
            if chunk == b'':
                return STATUS_ERR, None
            
            chunks.append(chunk)
            received_bytes += len(chunk)

        buffer = b''.join(chunks)
        
        
        return STATUS_OK, buffer

    def send(self, buffer, size, address):
        sent_bytes = 0
        while sent_bytes < size:
            sent = self._socket.sendto(buffer, address)
            if sent == 0:
                return STATUS_ERR, None 

            sent_bytes += sent

        return STATUS_OK    

        

    def close(self):
        """
        Close connecton of the server socket
        """
        self._socket.close()
