from ast import Match
import re
from uuid import UUID


class serializaAyuda:
    
    @staticmethod
    def float_to_bytes(f):
        return f.to_bytes(8, byteorder='big', signed=True)  # Utilizamos 8 bytes para representar un número flotante

    @staticmethod
    def bytes_to_float(b):
        return int.from_bytes(b, byteorder='big', signed=True) / 100.0  # Ajusta la escala según tus necesidades


    @staticmethod
    def int_to_bytes(integer, length):
        return integer.to_bytes(length, byteorder='big', signed=True)

    @staticmethod
    def bytes_to_int(bytes_data):
        return int.from_bytes(bytes_data, byteorder='big', signed=True)

    
    @staticmethod
    def guid_to_bytes(guid):
        return guid.bytes

    
    @staticmethod
    def bytes_to_guid(bytes_data):
        return UUID.UUID(bytes=bytes_data)

    
    @staticmethod
    def string_to_bytes(text, length):
        return text.encode('utf-8').ljust(length, b'\0')
    

    @staticmethod
    def bytes_to_string(bytes_data):
        return bytes_data.rstrip(b'\0').decode('utf-8')
    
    
    def patron_to_partes(patron):
        partes = re.findall(r'(\d*[a-zA-Z])', patron)
        return partes

    @staticmethod
    def serializar(patron, *args):
        result = b''
        index = 0
        partes = serializaAyuda.patron_to_partes(patron)
        match = re.match(r'^\d+', format_char)
        for format_char in partes:
            if format_char == 'H' or format_char == 'h':
                result += serializaAyuda.string_to_bytes(args[index], 1)
                index += 1                
            elif re.match(r'^\d+', format_char):
                result += serializaAyuda.string_to_bytes(args[index], int(match.group()))
                index += 1
            elif format_char == 'f':
                result += serializaAyuda.float_to_bytes(args[index])
                index += 1
            elif format_char == 'i':
                result += serializaAyuda.int_to_bytes(args[index])
                index += 1
        return result
