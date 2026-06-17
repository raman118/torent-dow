"""
Main CLI entry point and watch loop for the torrent client.
"""

import os
import time
import shutil
import asyncio
import traceback
from torrent_client.downloader import download_torrent


def ensure_dirs():
    """
    Creates the required directory structure.
    """
    dirs = [
        "torrent_client/download-these", 
        "torrent_client/in-progress", 
        "torrent_client/failed", 
        "torrent_client/downloads"
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def main():
    """
    The main watch loop. Polls the 'download-these' folder and orchestrates.
    """
    ensure_dirs()
    print("===================================================")
    print(" Python Terminal BitTorrent Client Started")
    print(" Drop .torrent files into 'torrent_client/download-these/'")
    print(" Press Ctrl+C to exit.")
    print("===================================================")
    
    # Generate a random 20-byte peer ID for this session
    peer_id = b"-PY3100-" + os.urandom(12)
    
    try:
        while True:
            download_dir = "torrent_client/download-these"
            files = [f for f in os.listdir(download_dir) if f.endswith(".torrent")]
            
            if not files:
                time.sleep(5)
                continue
                
            # Pick ONE torrent sequentially
            torrent_file = files[0]
            source_path = os.path.join(download_dir, torrent_file)
            in_progress_path = os.path.join("torrent_client/in-progress", torrent_file)
            
            print(f"\nFound: {torrent_file}")
            # Move to in-progress
            shutil.move(source_path, in_progress_path)
            
            try:
                print("Starting download...")
                # Run the asyncio downloader for this specific torrent
                asyncio.run(download_torrent(in_progress_path, "torrent_client/downloads", peer_id))
                
                print("Done ✓")
                os.remove(in_progress_path)
                
            except Exception as e:
                print(f"Failed ✗: {e}")
                failed_path = os.path.join("torrent_client/failed", torrent_file)
                shutil.move(in_progress_path, failed_path)
                
                # Write a log explaining the failure
                with open(os.path.join("torrent_client/failed", f"{torrent_file}.error.txt"), "w") as f:
                    f.write(traceback.format_exc())
                    
    except KeyboardInterrupt:
        print("\nExiting gracefully...")


if __name__ == "__main__":
    main()
