"""
Piece manager for handling downloaded blocks, verifying piece hashes,
and writing complete verified pieces to disk.
"""

import os
import math
import hashlib
import asyncio
from typing import Optional, Tuple, List
from .torrent import Torrent

BLOCK_SIZE = 16384

class Block:
    def __init__(self, offset: int, length: int):
        self.offset = offset
        self.length = length
        self.status = "missing"  # "missing", "pending", "received"


class Piece:
    def __init__(self, index: int, length: int, piece_hash: bytes):
        self.index = index
        self.length = length
        self.hash = piece_hash
        self.blocks: List[Block] = []
        self.is_complete = False
        self.data = bytearray(length)
        
        # Initialize blocks
        num_blocks = math.ceil(length / BLOCK_SIZE)
        for i in range(num_blocks):
            block_len = BLOCK_SIZE if i < num_blocks - 1 else length - (i * BLOCK_SIZE)
            self.blocks.append(Block(i * BLOCK_SIZE, block_len))
            
    def get_missing_block(self) -> Optional[Block]:
        for block in self.blocks:
            if block.status == "missing":
                block.status = "pending"
                return block
        return None


class PieceManager:
    """
    Coordinates the downloading and assembly of pieces and blocks.
    Thread-safe for asyncio tasks.
    """
    def __init__(self, torrent: Torrent, download_dir: str):
        self.torrent = torrent
        self.download_dir = download_dir
        self.pieces: List[Piece] = []
        self.lock = asyncio.Lock()
        
        # Initialize pieces
        for i, piece_hash in enumerate(self.torrent.piece_hashes):
            is_last = (i == len(self.torrent.piece_hashes) - 1)
            p_len = self.torrent.piece_length
            if is_last:
                remainder = self.torrent.total_length % self.torrent.piece_length
                if remainder != 0:
                    p_len = remainder
            self.pieces.append(Piece(i, p_len, piece_hash))

        self.complete_pieces = 0
        self.total_pieces = len(self.pieces)
        
        # Initialize output files and mappings
        self._init_files()

    def _init_files(self):
        self.file_mappings = []
        current_offset = 0
        for f in self.torrent.files:
            path = os.path.join(self.download_dir, f["path"])
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Pre-allocate if necessary
            if not os.path.exists(path) or os.path.getsize(path) != f["length"]:
                with open(path, "wb") as fp:
                    fp.truncate(f["length"])
                    
            self.file_mappings.append({
                "path": path,
                "start": current_offset,
                "end": current_offset + f["length"]
            })
            current_offset += f["length"]

    async def get_block_to_request(self, peer_bitfield: bytearray) -> Optional[Tuple[int, int, int]]:
        """
        Finds the next block to request from a peer sequentially.
        Returns (piece_index, block_offset, block_length) or None.
        """
        async with self.lock:
            for piece in self.pieces:
                if piece.is_complete:
                    continue
                
                # Check if the peer's bitfield indicates they have this piece
                byte_index = piece.index // 8
                bit_index = piece.index % 8
                if byte_index < len(peer_bitfield) and (peer_bitfield[byte_index] & (1 << (7 - bit_index))):
                    block = piece.get_missing_block()
                    if block:
                        return (piece.index, block.offset, block.length)
            return None

    async def mark_block_received(self, piece_index: int, offset: int, data: bytes) -> bool:
        """
        Marks a block as received. Verifies SHA1 hash when a piece is full.
        Writes the piece to disk if verified.
        Returns True if the block resulted in a verified, complete piece.
        """
        async with self.lock:
            piece = self.pieces[piece_index]
            if piece.is_complete:
                return False
                
            for block in piece.blocks:
                if block.offset == offset:
                    block.status = "received"
                    piece.data[offset : offset + len(data)] = data
                    break
            
            if all(b.status == "received" for b in piece.blocks):
                # Verify SHA1
                if hashlib.sha1(piece.data).digest() == piece.hash:
                    piece.is_complete = True
                    self.complete_pieces += 1
                    self._write_piece(piece)
                    return True
                else:
                    # Hash mismatch, reset blocks for re-download
                    for b in piece.blocks:
                        b.status = "missing"
                    return False
            return False

    async def reset_block(self, piece_index: int, offset: int):
        """
        Resets a block to 'missing' (e.g. if a peer disconnects).
        """
        async with self.lock:
            piece = self.pieces[piece_index]
            if piece.is_complete:
                return
            for block in piece.blocks:
                if block.offset == offset and block.status == "pending":
                    block.status = "missing"
                    break

    def _write_piece(self, piece: Piece):
        """
        Writes a verified piece to the pre-allocated files.
        Handles boundary crosses for multi-file torrents.
        """
        global_offset = piece.index * self.torrent.piece_length
        bytes_written = 0
        data_to_write = piece.data
        
        while bytes_written < len(data_to_write):
            current_offset = global_offset + bytes_written
            
            for f in self.file_mappings:
                if f["start"] <= current_offset < f["end"]:
                    write_size = min(len(data_to_write) - bytes_written, f["end"] - current_offset)
                    file_offset = current_offset - f["start"]
                    
                    with open(f["path"], "r+b") as fp:
                        fp.seek(file_offset)
                        fp.write(data_to_write[bytes_written : bytes_written + write_size])
                        
                    bytes_written += write_size
                    break

    def is_done(self) -> bool:
        return self.complete_pieces == self.total_pieces
