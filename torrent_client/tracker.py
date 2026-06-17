"""
HTTP Tracker client for BitTorrent.
Communicates with the tracker to get a list of peers.
"""

import struct
import socket
import requests
from typing import List, Tuple
from . import bencode
from .torrent import Torrent


class TrackerClient:
    """
    Handles communication with the HTTP tracker.
    """
    def __init__(self, torrent: Torrent, peer_id: bytes):
        self.torrent = torrent
        self.peer_id = peer_id

    def get_peers(self) -> List[Tuple[str, int]]:
        """
        Requests peers from the tracker.
        Returns a list of (ip, port) tuples.
        """
        params = {
            "info_hash": self.torrent.info_hash,
            "peer_id": self.peer_id,
            "port": 6881,
            "uploaded": 0,
            "downloaded": 0,
            "left": self.torrent.total_length,
            "compact": 1,
            "event": "started"
        }
        
        # requests will automatically urlencode the info_hash and peer_id bytes
        response = requests.get(self.torrent.announce, params=params, timeout=10)
        response.raise_for_status()
        
        tracker_dict = bencode.decode(response.content)
        
        if b"failure reason" in tracker_dict:
            raise ValueError(f"Tracker error: {tracker_dict[b'failure reason'].decode()}")
            
        peers_raw = tracker_dict.get(b"peers", b"")
        peers = []
        
        if isinstance(peers_raw, bytes):
            # Compact format: 6 bytes per peer (4 bytes IP, 2 bytes Port)
            for i in range(0, len(peers_raw), 6):
                ip_bytes = peers_raw[i : i + 4]
                port_bytes = peers_raw[i + 4 : i + 6]
                
                # Convert to standard format
                ip = socket.inet_ntoa(ip_bytes)
                port = struct.unpack(">H", port_bytes)[0]
                peers.append((ip, port))
        elif isinstance(peers_raw, list):
            # Non-compact format (List of dictionaries)
            for p in peers_raw:
                peers.append((p[b"ip"].decode("utf-8"), p[b"port"]))
                
        return peers
