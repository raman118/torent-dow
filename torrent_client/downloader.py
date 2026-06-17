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
INITIAL_RETRY_TIMEOUT = 30
MAX_RETRY_TIMEOUT = 300

async def download_torrent(torrent_path: str, download_dir: str, peer_id: bytes):
    """
    Main orchestration function for downloading a single torrent.
    """
    loop = asyncio.get_running_loop()
    
    # Read torrent file without blocking event loop
    with open(torrent_path, "rb") as f:
        torrent_data = await loop.run_in_executor(None, f.read)
    torrent = Torrent(torrent_data)
        
    print(f"Torrent: {torrent.name}")
    print(f"Total Size: {torrent.total_length / 1024 / 1024:.2f} MB")
    
    tracker = TrackerClient(torrent, peer_id)
    
    peers = []
    retry_timeout = INITIAL_RETRY_TIMEOUT
    
    while not peers:
        print(f"Requesting peers from trackers...")
        peers = await tracker.get_peers()
        if not peers:
            print(f"Trackers returned 0 peers. Retrying in {retry_timeout} seconds...")
            await asyncio.sleep(retry_timeout)
            retry_timeout = min(retry_timeout * 2, MAX_RETRY_TIMEOUT)
            
    print(f"Found {len(peers)} peers. Starting piece manager...")
    
    piece_manager = PieceManager(torrent, download_dir)
    # Initialize files asynchronously to avoid blocking
    await loop.run_in_executor(None, piece_manager.init_files)
    
    try:
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
                # Check if all tasks finished and we still have work
                if all(t.done() for t in tasks) and not piece_manager.is_done():
                    # Try to get more peers if we ran out of connections
                    print("\nAll peer connections dropped. Searching for more peers...")
                    new_peers = await tracker.get_peers()
                    if not new_peers:
                        print(f"No more peers found. Retrying in {retry_timeout} seconds...")
                        await asyncio.sleep(retry_timeout)
                        retry_timeout = min(retry_timeout * 2, MAX_RETRY_TIMEOUT)
                        continue
                    
                    # Reset retry timeout since we found peers
                    retry_timeout = INITIAL_RETRY_TIMEOUT
                    
                    connections = [
                        PeerConnection(ip, port, torrent, peer_id, piece_manager)
                        for ip, port in new_peers[:MAX_CONCURRENT_PEERS]
                    ]
                    tasks = [asyncio.create_task(conn.start()) for conn in connections]
                    continue
                    
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
        
        # Wait for all tasks to cancel and suppress exceptions so they don't leak
        await asyncio.gather(*tasks, return_exceptions=True)
        
    finally:
        # Final cleanup: close persistent file handles
        piece_manager.close()
