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
        
        self.announces: List[str] = []
        # Main announce
        announce_bytes = self.data.get(b"announce")
        if announce_bytes:
            self.announces.append(announce_bytes.decode("utf-8"))
            
        # Announce-list (multi-tracker)
        if b"announce-list" in self.data:
            for tier in self.data[b"announce-list"]:
                for tracker in tier:
                    t_str = tracker.decode("utf-8")
                    if t_str not in self.announces:
                        self.announces.append(t_str)
        
        # Backward compatibility for code expecting self.announce
        self.announce: Optional[str] = self.announces[0] if self.announces else None
                    
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
