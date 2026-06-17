"""
Asyncio orchestrator for the download.
Manages connections to peers, progress tracking via tqdm, and finalization.
"""

import asyncio
from tqdm import tqdm
from .torrent import Torrent
from .tracker import TrackerClient
from .pieces import PieceManager
from .peer import PeerConnection

MAX_CONCURRENT_PEERS = 50

async def download_torrent(torrent_path: str, download_dir: str, peer_id: bytes):
    """
    Main orchestration function for downloading a single torrent.
    """
    with open(torrent_path, "rb") as f:
        torrent = Torrent(f.read())
        
    print(f"Torrent: {torrent.name}")
    print(f"Total Size: {torrent.total_length / 1024 / 1024:.2f} MB")
    
    tracker = TrackerClient(torrent, peer_id)
    print(f"Requesting peers from {torrent.announce} ...")
    peers = tracker.get_peers()
    print(f"Found {len(peers)} peers.")
    
    piece_manager = PieceManager(torrent, download_dir)
    
    # Initialize up to MAX_CONCURRENT_PEERS
    connections = [
        PeerConnection(ip, port, torrent, peer_id, piece_manager)
        for ip, port in peers[:MAX_CONCURRENT_PEERS]
    ]
    
    tasks = [asyncio.create_task(conn.start()) for conn in connections]
    
    # Track progress with tqdm
    with tqdm(total=torrent.total_length, unit='B', unit_scale=True, desc=torrent.name) as pbar:
        previous_completed_bytes = 0
        
        while not piece_manager.is_done():
            # Abort if we have no active tasks and the torrent isn't done
            if all(t.done() for t in tasks) and not piece_manager.is_done():
                raise RuntimeError("All peer connections dropped before download completed.")
                
            completed_bytes = piece_manager.complete_pieces * torrent.piece_length
            if completed_bytes > torrent.total_length:
                completed_bytes = torrent.total_length
                
            bytes_delta = completed_bytes - previous_completed_bytes
            if bytes_delta > 0:
                pbar.update(bytes_delta)
                previous_completed_bytes = completed_bytes
                
            await asyncio.sleep(0.5)
            
        # Ensure progress bar hits 100% precisely
        final_delta = torrent.total_length - previous_completed_bytes
        if final_delta > 0:
            pbar.update(final_delta)
            
    # Cleanup tasks
    for task in tasks:
        if not task.done():
            task.cancel()
