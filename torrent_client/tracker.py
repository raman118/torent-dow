"""
Tracker client supporting both HTTP and UDP protocols.
Corrected for precise HTTP encoding and valid 98-byte UDP announce packets.
"""

import struct
import socket
import requests
import asyncio
import random
import urllib.parse
from typing import List, Tuple, Optional, Set
from . import bencode
from .torrent import Torrent


class TrackerClient:
    def __init__(self, torrent: Torrent, peer_id: bytes):
        self.torrent = torrent
        self.peer_id = peer_id

    async def get_peers(self) -> List[Tuple[str, int]]:
        """
        Tries all trackers in the torrent's announce list in parallel.
        """
        tasks = []
        for url in self.torrent.announces:
            if url.startswith("http"):
                tasks.append(self._get_peers_http(url))
            elif url.startswith("udp"):
                tasks.append(self._get_peers_udp(url))
        
        if not tasks:
            return []

        print(f"Polling {len(tasks)} trackers in parallel...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_peers: Set[Tuple[str, int]] = set()
        for res in results:
            if isinstance(res, list):
                all_peers.update(res)
            elif isinstance(res, Exception):
                pass
                
        return list(all_peers)

    async def _get_peers_http(self, url: str) -> List[Tuple[str, int]]:
        try:
            # Build the query string manually to ensure correct percent-encoding of raw bytes
            encoded_info_hash = urllib.parse.quote(self.torrent.info_hash, safe='')
            encoded_peer_id = urllib.parse.quote(self.peer_id, safe='')
            
            query = (
                f"info_hash={encoded_info_hash}"
                f"&peer_id={encoded_peer_id}"
                f"&port=6881&uploaded=0&downloaded=0"
                f"&left={self.torrent.total_length}&compact=1&event=started"
            )
            
            full_url = f"{url}?{query}" if "?" not in url else f"{url}&{query}"
            
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.get(full_url, timeout=5)
            )
            response.raise_for_status()
            
            tracker_dict = bencode.decode(response.content)
            if b"failure reason" in tracker_dict:
                return []
                
            peers_raw = tracker_dict.get(b"peers", b"")
            peers = []
            
            if isinstance(peers_raw, bytes):
                for i in range(0, len(peers_raw), 6):
                    ip = socket.inet_ntoa(peers_raw[i : i + 4])
                    port = struct.unpack(">H", peers_raw[i + 4 : i + 6])[0]
                    peers.append((ip, port))
            elif isinstance(peers_raw, list):
                for p in peers_raw:
                    peers.append((p[b"ip"].decode("utf-8"), p[b"port"]))
            
            if peers:
                print(f"  [HTTP] {url} returned {len(peers)} peers.")
            return peers
        except Exception:
            return []

    async def _get_peers_udp(self, url: str) -> List[Tuple[str, int]]:
        """
        Runs the UDP tracker transaction in a thread pool for maximum compatibility.
        """
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._udp_transaction, url)
        except Exception:
            return []

    def _udp_transaction(self, url: str) -> List[Tuple[str, int]]:
        parsed = urllib.parse.urlparse(url)
        hostname = parsed.hostname
        port = parsed.port
        if not hostname or not port:
            return []

        try:
            ip = socket.gethostbyname(hostname)
        except socket.gaierror:
            return []

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5.0)
        
        try:
            # 1. Connect
            transaction_id = random.randint(0, 2**32 - 1)
            packet = struct.pack(">QII", 0x41727101980, 0, transaction_id)
            sock.sendto(packet, (ip, port))
            
            res, _ = sock.recvfrom(1024)
            if len(res) < 16: return []
            action, res_transaction_id, connection_id = struct.unpack(">IIQ", res[:16])
            if action != 0 or res_transaction_id != transaction_id: return []

            # 2. Announce (Packet must be exactly 98 bytes)
            transaction_id = random.randint(0, 2**32 - 1)
            # connection_id(Q), action=1(I), tid(I), info_hash(20s), peer_id(20s), 
            # down(Q), left(Q), up(Q), event=2(I), ip=0(I), key=0(I), num_want=-1(i), port=6881(H)
            packet = struct.pack(">QII20s20sQQQIIIiH", 
                                connection_id, 1, transaction_id, self.torrent.info_hash, self.peer_id,
                                0, self.torrent.total_length, 0, 2, 0, 0, -1, 6881)
            sock.sendto(packet, (ip, port))
            
            res, _ = sock.recvfrom(2048)
            if len(res) < 20: return []
            action, res_transaction_id, interval, leechers, seeders = struct.unpack(">IIIII", res[:20])
            if action != 1 or res_transaction_id != transaction_id: return []

            peers_raw = res[20:]
            peers = []
            for i in range(0, len(peers_raw) - (len(peers_raw) % 6), 6):
                p_ip = socket.inet_ntoa(peers_raw[i : i + 4])
                p_port = struct.unpack(">H", peers_raw[i + 4 : i + 6])[0]
                peers.append((p_ip, p_port))
            
            if peers:
                print(f"  [UDP] {url} returned {len(peers)} peers.")
            return peers
            
        except socket.timeout:
            return []
        except Exception:
            return []
        finally:
            sock.close()
