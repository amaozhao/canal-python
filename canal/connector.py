#!/usr/bin/env python3

import socket
import struct


class Connector:
    sock = None
    packet_len = 4

    async def connect(self, host, port, timeout=10):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(timeout)
            await self.loop.sock_connect(self.sock, (host, port))
            self.sock.settimeout(None)
        except socket.error as e:
            print(f'Connect to server error: {e}')
            self.sock.close()

    async def disconnect(self):
        self.sock.close()

    async def read(self, length):
        recv = b''
        while True:
            buf = await self.loop.sock_recv(self.sock, length)
            if not buf:
                raise Exception('TSocket: Could not read bytes from server')
            read_len = len(buf)
            if read_len < length:
                recv += buf
                length -= read_len
            else:
                return recv + buf

    async def write(self, buf):
        self.sock.sendall(buf)
        await self.loop.sock_sendall(self.sock, buf)

    async def read_next_packet(self):
        data = await self.read(self.packet_len)
        data_len = struct.unpack('>i', data)
        return await self.read(data_len[0])

    async def write_with_header(self, data):
        await self.write(struct.pack('>i', len(data)))
        await self.write(data)