"""
Peer wire protocol implementation using asyncio.
Handles handshakes, reading length-prefixed messages, and block requests.
"""

import asyncio
import struct
from typing import Optional, List, Tuple
from .torrent import Torrent
from .pieces import PieceManager

MAX_PENDING_REQUESTS = 5

class PeerConnection:
    """
    Manages a TCP connection to a single peer via asyncio.
    """
    def __init__(self, ip: str, port: int, torrent: Torrent, peer_id: bytes, piece_manager: PieceManager):
        self.ip = ip
        self.port = port
        self.torrent = torrent
        self.peer_id = peer_id
        self.piece_manager = piece_manager
        
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        
        self.choked = True
        self.bitfield = bytearray()
        self.requested_blocks: List[Tuple[int, int, int]] = []
        
    async def start(self):
        """
        Connects to the peer, handshakes, and enters the message loop.
        """
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=5.0
            )
            
            await self._handshake()
            
            # Send 'interested' immediately after handshaking
            self.writer.write(struct.pack(">IB", 1, 2))
            await self.writer.drain()
            
            await self._message_loop()
            
        except Exception:
            # Graceful failure handling on peer drop
            pass
        finally:
            # Revert any pending blocks assigned to this peer
            for idx, offset, _ in self.requested_blocks:
                await self.piece_manager.reset_block(idx, offset)
            self.requested_blocks.clear()
            
            if self.writer:
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass

    async def _handshake(self):
        # Construct and send the 68-byte BitTorrent handshake
        pstr = b"BitTorrent protocol"
        handshake = struct.pack(">B19s8s20s20s", 
                                len(pstr), pstr, bytes(8), 
                                self.torrent.info_hash, self.peer_id)
        self.writer.write(handshake)
        await self.writer.drain()
        
        # Read the handshake response
        response = await asyncio.wait_for(self.reader.readexactly(68), timeout=5.0)
        
        # Verify the info hash matches
        if response[28:48] != self.torrent.info_hash:
            raise ValueError("Info hash mismatch during handshake")

    async def _message_loop(self):
        while True:
            # Read length prefix (4 bytes)
            length_bytes = await asyncio.wait_for(self.reader.readexactly(4), timeout=120.0)
            length = struct.unpack(">I", length_bytes)[0]
            
            if length == 0:
                continue  # Keep-alive message
                
            if length > 10 * 1024 * 1024:
                raise ValueError(f"Message length too large: {length}")
                
            msg_id = (await self.reader.readexactly(1))[0]
            payload = await self.reader.readexactly(length - 1)
            
            if msg_id == 0:  # Choke
                self.choked = True
            elif msg_id == 1:  # Unchoke
                self.choked = False
                await self._fill_pipeline()
            elif msg_id == 4:  # Have
                piece_idx = struct.unpack(">I", payload)[0]
                self._update_bitfield(piece_idx)
                await self._fill_pipeline()
            elif msg_id == 5:  # Bitfield
                self.bitfield = bytearray(payload)
                await self._fill_pipeline()
            elif msg_id == 7:  # Piece block received
                index, begin = struct.unpack(">II", payload[:8])
                block_data = payload[8:]
                
                # Remove from requested_blocks tracking
                self.requested_blocks = [
                    req for req in self.requested_blocks 
                    if not (req[0] == index and req[1] == begin)
                ]
                
                await self.piece_manager.mark_block_received(index, begin, block_data)
                # Fire next request immediately to maintain pipeline
                await self._fill_pipeline()

    def _update_bitfield(self, piece_idx: int):
        byte_idx = piece_idx // 8
        bit_idx = piece_idx % 8
        while len(self.bitfield) <= byte_idx:
            self.bitfield.append(0)
        self.bitfield[byte_idx] |= (1 << (7 - bit_idx))

    async def _fill_pipeline(self):
        """
        Request blocks until the pipeline is full.
        """
        if self.choked:
            return
            
        # Refill until we have MAX_PENDING_REQUESTS in-flight
        while len(self.requested_blocks) < MAX_PENDING_REQUESTS:
            if not self.bitfield:
                break
                
            request = await self.piece_manager.get_block_to_request(self.bitfield)
            if not request:
                break
                
            idx, begin, length = request
            self.requested_blocks.append(request)
            
            # Send Request message (id 6)
            msg = struct.pack(">IBIII", 13, 6, idx, begin, length)
            self.writer.write(msg)
            await self.writer.drain()
