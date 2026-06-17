"""
Torrent metainfo parser.
Extracts metadata from .torrent files including info_hash, tracker URL,
piece hashes, and file structure.
"""

import hashlib
from typing import List, Optional
from . import bencode


class Torrent:
    def __init__(self, raw_data: bytes):
        self.data = bencode.decode(raw_data)
        self.announce: str = self.data[b"announce"].decode("utf-8")
        
        # The 'info' dict is hashed to create the info_hash
        info = self.data[b"info"]
        self.info_hash: bytes = hashlib.sha1(bencode.encode(info)).digest()
        
        self.piece_length: int = info[b"piece length"]
        
        # Pieces is a string of concatenated 20-byte SHA1 hashes
        pieces_raw = info[b"pieces"]
        self.piece_hashes: List[bytes] = [
            pieces_raw[i : i + 20] for i in range(0, len(pieces_raw), 20)
        ]
        
        self.files: List[dict] = []
        if b"files" in info:
            # Multi-file mode
            self.name: str = info[b"name"].decode("utf-8")
            for f in info[b"files"]:
                path = "/".join([p.decode("utf-8") for p in f[b"path"]])
                self.files.append({
                    "path": f"{self.name}/{path}",
                    "length": f[b"length"]
                })
        else:
            # Single-file mode
            self.name: str = info[b"name"].decode("utf-8")
            self.files.append({
                "path": self.name,
                "length": info[b"length"]
            })
            
        self.total_length: int = sum(f["length"] for f in self.files)

    def __repr__(self) -> str:
        return (f"Torrent(name={self.name}, "
                f"total_length={self.total_length}, "
                f"pieces={len(self.piece_hashes)})")


if __name__ == "__main__":
    # Mock bencoded torrent data for testing
    import os
    
    # Create a fake info dict
    fake_info = {
        b"name": b"test_file.txt",
        b"piece length": 16384,
        b"pieces": hashlib.sha1(b"fake data").digest(), # One piece
        b"length": 1024
    }
    
    fake_torrent_dict = {
        b"announce": b"http://tracker.example.com/announce",
        b"info": fake_info
    }
    
    fake_data = bencode.encode(fake_torrent_dict)
    
    # Test parsing
    torrent = Torrent(fake_data)
    print(f"Parsed Torrent: {torrent}")
    print(f"Announce: {torrent.announce}")
    print(f"Info Hash: {torrent.info_hash.hex()}")
    print(f"Total Length: {torrent.total_length}")
    print(f"Files: {torrent.files}")
    
    assert torrent.announce == "http://tracker.example.com/announce"
    assert len(torrent.piece_hashes) == 1
    assert torrent.total_length == 1024
    print("Torrent parsing verified!")
