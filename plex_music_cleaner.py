#!/usr/bin/env python3
"""
plex_music_cleaner.py - Synchronize Plex music metadata with Apple Music library

This script compares metadata between Plex music libraries and Apple Music,
updates Plex to match Apple Music, and can sync playlists between the two systems.
"""

import os
import sys
import argparse
import sqlite3
import logging
import time
from pathlib import Path
from datetime import datetime
import re
from typing import Dict, List, Tuple, Optional, Set, Any
from collections import defaultdict

try:
    import dotenv
    from plexapi.server import PlexServer
    from plexapi.exceptions import NotFound, Unauthorized
    import paramiko
except ImportError:
    print("Required packages not found. Installing dependencies...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", 
                          "python-dotenv", "plexapi", "paramiko"])
    import dotenv
    from plexapi.server import PlexServer
    from plexapi.exceptions import NotFound, Unauthorized
    import paramiko

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('plex_music_cleaner.log')
    ]
)
logger = logging.getLogger(__name__)

class PlexClient:
    """Interface to Plex server for retrieving and updating music metadata."""
    
    def __init__(self, url: str, token: str, section_id: int):
        """
        Initialize connection to Plex server.
        
        Args:
            url: Plex server URL
            token: Plex authentication token
            section_id: Music library section ID
        """
        self.url = url
        self.token = token
        self.section_id = section_id
        self.server = None
        self.music_section = None
        self.connect()
        
    def connect(self) -> None:
        """Establish connection to Plex server."""
        try:
            logger.info(f"Connecting to Plex server at {self.url}")
            self.server = PlexServer(self.url, self.token)
            self.music_section = self.server.library.sectionByID(self.section_id)
            logger.info(f"Connected to Plex music library: {self.music_section.title}")
        except Unauthorized:
            logger.error("Failed to connect to Plex: Invalid token")
            sys.exit(1)
        except NotFound:
            logger.error(f"Music section with ID {self.section_id} not found")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to connect to Plex: {str(e)}")
            sys.exit(1)
    
    def get_all_tracks(self) -> List:
        """Retrieve all music tracks from the Plex library."""
        try:
            logger.info("Retrieving all tracks from Plex...")
            tracks = self.music_section.searchTracks()
            logger.info(f"Retrieved {len(tracks)} tracks from Plex")
            return tracks
        except Exception as e:
            logger.error(f"Failed to retrieve tracks: {str(e)}")
            return []
    
    def get_tracks_by_artist(self, artist_name: str) -> List:
        """
        Retrieve tracks filtered by artist name.
        
        Args:
            artist_name: Name of the artist to filter by
            
        Returns:
            List of track objects matching the artist
        """
        try:
            logger.info(f"Retrieving tracks for artist: {artist_name}")
            tracks = self.music_section.searchTracks(artist=artist_name)
            logger.info(f"Retrieved {len(tracks)} tracks for artist '{artist_name}'")
            return tracks
        except Exception as e:
            logger.error(f"Failed to retrieve tracks for artist '{artist_name}': {str(e)}")
            return []
    
    def find_track_by_filename(self, filename: str) -> Optional[Any]:
        """
        Find a track in Plex by its filename.
        
        Args:
            filename: The filename to search for
            
        Returns:
            Track object if found, None otherwise
        """
        try:
            # Extract the base filename without path and extension
            base_filename = os.path.basename(filename)
            name_without_ext = os.path.splitext(base_filename)[0]
            
            # Search by filename
            results = self.music_section.search(title=name_without_ext, libtype="track")
            
            # If multiple results, try to find exact match by checking media parts
            for track in results:
                for media in track.media:
                    for part in media.parts:
                        if os.path.basename(part.file) == base_filename:
                            return track
                        
            # If no exact match but we have results, return the first one
            if results:
                return results[0]
                
            return None
        except Exception as e:
            logger.error(f"Error finding track by filename '{filename}': {str(e)}")
            return None
    
    def update_track_metadata(self, track, title: str = None, artist: str = None, 
                             album: str = None) -> bool:
        """
        Update metadata for a track.
        
        Args:
            track: Plex track object
            title: New track title
            artist: New artist name
            album: New album title
            
        Returns:
            True if update was successful, False otherwise
        """
        update_fields = {}
        if title is not None and track.title != title:
            update_fields['title'] = title
        if artist is not None and track.originalTitle != artist:
            update_fields['originalTitle'] = artist
        if album is not None and track.parentTitle != album:
            update_fields['parentTitle'] = album
            
        if not update_fields:
            return False
            
        try:
            logger.info(f"Updating track {track.title} ({track.ratingKey}) with: {update_fields}")
            track.edit(**update_fields)
            track.reload()
            return True
        except Exception as e:
            logger.error(f"Failed to update track {track.title}: {str(e)}")
            return False
    
    def create_playlist(self, name: str, tracks: List) -> bool:
        """
        Create a playlist in Plex with the given tracks.
        
        Args:
            name: Name for the new playlist
            tracks: List of track objects to include
            
        Returns:
            True if playlist was created successfully, False otherwise
        """
        try:
            # Check if playlist already exists
            existing_playlists = self.server.playlists()
            for playlist in existing_playlists:
                if playlist.title.lower() == name.lower():
                    logger.info(f"Playlist '{name}' already exists, updating...")
                    playlist.delete()
                    break
                    
            logger.info(f"Creating playlist '{name}' with {len(tracks)} tracks")
            self.server.createPlaylist(name, tracks)
            logger.info(f"Playlist '{name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create playlist '{name}': {str(e)}")
            return False


class AppleMusicClient:
    """Interface to Apple Music library database for retrieving metadata."""
    
    def __init__(self, library_path: str):
        """
        Initialize connection to Apple Music library.
        
        Args:
            library_path: Path to the Apple Music library file or directory
        """
        # Normalise the incoming path – remove wrapping quotes and tidy separators
        library_path = library_path.strip().strip('"').strip("'")
        library_path = os.path.normpath(library_path)
        logger.debug(f"Normalised Apple Music library path: {library_path}")
        self.library_path = library_path
        self.db_path = None
        self.conn = None
        self.ssh_client = None
        self.is_remote = False
        self._find_and_connect_db()
        
    def _find_and_connect_db(self) -> None:
        """Find the SQLite database file and establish connection."""
        try:
            #
            # Search order:
            #   1. Local path (works for regular folders and most UNC shares)
            #   2. Network-share specific crawl (in case the UNC share lives on a
            #      non-Windows server and local APIs fail to enumerate)
            #   3. SSH path (user@host:/path/to/Library.musiclibrary)
            #
            # Step 1 – local search first
            self._find_db_locally()

            # Step 2 – fall back to explicit network-share walk
            if not self.db_path:
                self._find_db_on_network_share()

            # Step 3 – last resort: SSH
            is_ssh_path = (
                not self.db_path
                and '@' in self.library_path
                and ':' in self.library_path
                and not (len(self.library_path) >= 3 and self.library_path[1:3] == ':\\')
            )
            if is_ssh_path:
                self._find_db_via_ssh()
                
            if not self.db_path:
                logger.error("Could not locate Apple Music library database")
                sys.exit(1)
                
            logger.info(f"Connecting to Apple Music database at: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info("Connected to Apple Music database")
        except Exception as e:
            logger.error(f"Failed to connect to Apple Music database: {str(e)}")
            sys.exit(1)
    
    def _find_db_locally(self) -> None:
        """Find the database file in a local directory."""
        path = Path(self.library_path)
        
        # If direct path to the database file
        if path.is_file() and path.suffix.lower() in ('.db', '.musicdb', '.musiclibrary'):
            self.db_path = str(path)
            logger.info(f"Found Apple Music database locally: {self.db_path}")
            return
            
        # If it's the .musiclibrary bundle/directory
        if path.is_dir():
            # Look for SQLite database inside
            for db_file in path.rglob('*'):
                if (
                    db_file.is_file()
                    and db_file.suffix.lower() in ('.db', '.musicdb')
                    and 'Library' in db_file.name
                ):
                    self.db_path = str(db_file)
                    logger.info(f"Located Apple Music database locally: {self.db_path}")
                    return
    
    def _find_db_on_network_share(self) -> None:
        """Find the database file on a network share."""
        try:
            # For network shares, we need to handle paths differently
            path = Path(self.library_path)
            
            # If direct path to the database file
            if os.path.isfile(self.library_path) and self.library_path.endswith(('.db', '.musicdb')):
                self.db_path = self.library_path
                logger.info(f"Found Apple Music database on network share: {self.db_path}")
                return
                
            # If it's the .musiclibrary bundle/directory
            if os.path.isdir(self.library_path):
                # Look for SQLite database inside
                for root, dirs, files in os.walk(self.library_path):
                    for file in files:
                        if file.endswith(('.db', '.musicdb')) and 'Library' in file:
                            self.db_path = os.path.join(root, file)
                            logger.info(f"Located Apple Music database on network share: {self.db_path}")
                            return
        except Exception as e:
            logger.error(f"Error accessing network share: {str(e)}")
    
    def _find_db_via_ssh(self) -> None:
        """Find the database file via SSH connection."""
        try:
            # Parse the SSH connection string
            if '@' in self.library_path:
                user, rest = self.library_path.split('@', 1)
                host, remote_path = rest.split(':', 1)
            else:
                # Default to current user if not specified
                import getpass
                user = getpass.getuser()
                host, remote_path = self.library_path.split(':', 1)
                
            # Connect via SSH
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=host, username=user)
            self.is_remote = True
            
            # Check if direct path to database
            sftp = self.ssh_client.open_sftp()
            try:
                sftp.stat(remote_path)
                if remote_path.endswith('.db'):
                    # Create a temporary local copy
                    local_path = f"temp_{int(time.time())}_music_library.db"
                    sftp.get(remote_path, local_path)
                    self.db_path = local_path
                    return
            except FileNotFoundError:
                pass
                
            # Search for database in directory
            if remote_path.endswith('.musiclibrary'):
                # Execute find command to locate the database
                cmd = f"find {remote_path} -name '*.db' | grep -i Library"
                stdin, stdout, stderr = self.ssh_client.exec_command(cmd)
                db_files = stdout.read().decode().strip().split('\n')
                
                if db_files and db_files[0]:
                    remote_db_path = db_files[0]
                    # Create a temporary local copy
                    local_path = f"temp_{int(time.time())}_music_library.db"
                    sftp.get(remote_db_path, local_path)
                    self.db_path = local_path
            
            sftp.close()
        except Exception as e:
            logger.error(f"Error accessing remote file via SSH: {str(e)}")
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
    
    def get_all_tracks(self) -> Dict[str, Dict]:
        """
        Retrieve all tracks from Apple Music library.
        
        Returns:
            Dictionary mapping filenames to metadata dictionaries
        """
        try:
            logger.info("Retrieving all tracks from Apple Music...")
            cursor = self.conn.cursor()
            
            # Query to get track metadata including file paths
            query = """
            SELECT 
                item.title, 
                artist.name as artist_name,
                album.title as album_title,
                item.location as file_path
            FROM 
                item
            LEFT JOIN 
                artist ON item.artist_pid = artist.persistent_id
            LEFT JOIN 
                album ON item.album_pid = album.persistent_id
            WHERE 
                item.location IS NOT NULL
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Process results
            tracks_by_filename = {}
            for row in rows:
                # Apple Music stores file paths in a special format that needs decoding
                file_path = self._decode_apple_file_path(row['file_path'])
                if file_path:
                    tracks_by_filename[file_path] = {
                        'title': row['title'],
                        'artist': row['artist_name'],
                        'album': row['album_title']
                    }
            
            logger.info(f"Retrieved {len(tracks_by_filename)} tracks from Apple Music")
            return tracks_by_filename
        except Exception as e:
            logger.error(f"Failed to retrieve tracks from Apple Music: {str(e)}")
            return {}
    
    def get_tracks_by_artist(self, artist_name: str) -> Dict[str, Dict]:
        """
        Retrieve tracks for a specific artist from Apple Music library.
        
        Args:
            artist_name: Name of the artist to filter by
            
        Returns:
            Dictionary mapping filenames to metadata dictionaries
        """
        try:
            logger.info(f"Retrieving tracks for artist '{artist_name}' from Apple Music...")
            cursor = self.conn.cursor()
            
            # Query to get track metadata for a specific artist
            query = """
            SELECT 
                item.title, 
                artist.name as artist_name,
                album.title as album_title,
                item.location as file_path
            FROM 
                item
            LEFT JOIN 
                artist ON item.artist_pid = artist.persistent_id
            LEFT JOIN 
                album ON item.album_pid = album.persistent_id
            WHERE 
                item.location IS NOT NULL
                AND artist.name LIKE ?
            """
            
            cursor.execute(query, (f"%{artist_name}%",))
            rows = cursor.fetchall()
            
            # Process results
            tracks_by_filename = {}
            for row in rows:
                file_path = self._decode_apple_file_path(row['file_path'])
                if file_path:
                    tracks_by_filename[file_path] = {
                        'title': row['title'],
                        'artist': row['artist_name'],
                        'album': row['album_title']
                    }
            
            logger.info(f"Retrieved {len(tracks_by_filename)} tracks for artist '{artist_name}' from Apple Music")
            return tracks_by_filename
        except Exception as e:
            logger.error(f"Failed to retrieve tracks for artist '{artist_name}' from Apple Music: {str(e)}")
            return {}
    
    def get_playlist_tracks(self, playlist_name: str) -> List[str]:
        """
        Retrieve tracks in a playlist from Apple Music library.
        
        Args:
            playlist_name: Name of the playlist
            
        Returns:
            List of file paths for tracks in the playlist
        """
        try:
            logger.info(f"Retrieving tracks for playlist '{playlist_name}' from Apple Music...")
            cursor = self.conn.cursor()
            
            # First get the playlist ID
            query = """
            SELECT 
                persistent_id
            FROM 
                playlist
            WHERE 
                name LIKE ?
            """
            
            cursor.execute(query, (f"%{playlist_name}%",))
            playlist_row = cursor.fetchone()
            
            if not playlist_row:
                logger.error(f"Playlist '{playlist_name}' not found in Apple Music")
                return []
                
            playlist_id = playlist_row['persistent_id']
            
            # Now get the tracks in the playlist
            query = """
            SELECT 
                item.location as file_path
            FROM 
                playlist_item
            JOIN 
                item ON playlist_item.track_id = item.persistent_id
            WHERE 
                playlist_item.playlist_id = ?
            ORDER BY 
                playlist_item.position
            """
            
            cursor.execute(query, (playlist_id,))
            rows = cursor.fetchall()
            
            # Process results
            track_paths = []
            for row in rows:
                file_path = self._decode_apple_file_path(row['file_path'])
                if file_path:
                    track_paths.append(file_path)
            
            logger.info(f"Retrieved {len(track_paths)} tracks for playlist '{playlist_name}' from Apple Music")
            return track_paths
        except Exception as e:
            logger.error(f"Failed to retrieve tracks for playlist '{playlist_name}' from Apple Music: {str(e)}")
            return []
    
    def _decode_apple_file_path(self, encoded_path: str) -> Optional[str]:
        """
        Decode Apple Music's encoded file paths.
        
        Args:
            encoded_path: Encoded file path from Apple Music database
            
        Returns:
            Decoded file path or None if decoding fails
        """
        if not encoded_path:
            return None
            
        try:
            # Apple Music stores paths in a URL-encoded format with a 'file://' prefix
            if encoded_path.startswith('file://'):
                # Remove the file:// prefix
                path = encoded_path[7:]
                
                # URL decode the path
                import urllib.parse
                path = urllib.parse.unquote(path)
                
                # Convert to Windows path format if needed
                if os.name == 'nt' and path.startswith('/'):
                    # Handle macOS paths on Windows
                    if path.startswith('/Volumes/'):
                        # Map /Volumes/DriveName to Windows drive letter
                        parts = path.split('/', 3)
                        if len(parts) >= 4:
                            drive_name = parts[2]
                            rest_of_path = parts[3]
                            # This is a simplification - in reality you'd need to map
                            # the macOS volume name to the correct Windows drive letter
                            path = f"{drive_name}:/{rest_of_path}"
                    else:
                        # For local macOS paths, just remove the leading slash
                        path = path[1:]
                
                return path
            return None
        except Exception as e:
            logger.error(f"Failed to decode Apple Music file path: {str(e)}")
            return None
    
    def close(self) -> None:
        """Close database connection and clean up resources."""
        if self.conn:
            self.conn.close()
            
        if self.is_remote and self.ssh_client:
            self.ssh_client.close()
            
        # Remove temporary database file if it exists
        if self.is_remote and self.db_path and os.path.exists(self.db_path) and 'temp_' in self.db_path:
            try:
                os.remove(self.db_path)
                logger.debug(f"Removed temporary database file: {self.db_path}")
            except Exception as e:
                logger.error(f"Failed to remove temporary database file: {str(e)}")


class CleanLogger:
    """Logger for tracking metadata changes and processed tracks."""
    
    def __init__(self, db_path: str = "plex_clean_log.db"):
        """
        Initialize the cleaning log database.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self._initialize_db()
        
    def _initialize_db(self) -> None:
        """Create the database and tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # Create table for tracking cleaned tracks
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS cleaned (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plex_rating_key TEXT NOT NULL,
                field TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                timestamp TEXT NOT NULL
            )
            ''')
            
            self.conn.commit()
            logger.debug(f"Initialized clean log database at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize clean log database: {str(e)}")
            sys.exit(1)
    
    def record_change(self, rating_key: str, field: str, old_value: str, 
                     new_value: str) -> None:
        """
        Record a metadata change in the log.
        
        Args:
            rating_key: Plex rating key for the track
            field: Metadata field that was changed
            old_value: Previous value
            new_value: New value
        """
        try:
            cursor = self.conn.cursor()
            timestamp = datetime.now().isoformat()
            
            cursor.execute('''
            INSERT INTO cleaned (plex_rating_key, field, old_value, new_value, timestamp)
            VALUES (?, ?, ?, ?, ?)
            ''', (rating_key, field, old_value, new_value, timestamp))
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to record change: {str(e)}")
    
    def is_track_cleaned(self, rating_key: str, field: str) -> bool:
        """
        Check if a track has already been cleaned for a specific field.
        
        Args:
            rating_key: Plex rating key for the track
            field: Metadata field to check
            
        Returns:
            True if the track field has been cleaned, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
            SELECT COUNT(*) FROM cleaned
            WHERE plex_rating_key = ? AND field = ?
            ''', (rating_key, field))
            
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"Failed to check if track is cleaned: {str(e)}")
            return False
    
    def get_cleaned_tracks(self) -> Set[str]:
        """
        Get the set of all track rating keys that have been cleaned.
        
        Returns:
            Set of Plex rating keys
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('SELECT DISTINCT plex_rating_key FROM cleaned')
            
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to get cleaned tracks: {str(e)}")
            return set()
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the cleaning process.
        
        Returns:
            Dictionary with statistics
        """
        try:
            cursor = self.conn.cursor()
            
            # Get total number of changes
            cursor.execute('SELECT COUNT(*) FROM cleaned')
            total_changes = cursor.fetchone()[0]
            
            # Get number of unique tracks changed
            cursor.execute('SELECT COUNT(DISTINCT plex_rating_key) FROM cleaned')
            tracks_changed = cursor.fetchone()[0]
            
            # Get counts by field
            cursor.execute('''
            SELECT field, COUNT(*) FROM cleaned
            GROUP BY field
            ''')
            
            field_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            stats = {
                'total_changes': total_changes,
                'tracks_changed': tracks_changed,
                **field_counts
            }
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get stats: {str(e)}")
            return {'error': 'Failed to retrieve statistics'}
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()


def clean_all_tracks(plex_client: PlexClient, apple_music_client: AppleMusicClient, 
                    clean_logger: CleanLogger) -> Dict[str, int]:
    """
    Clean metadata for all tracks in the Plex library.
    
    Args:
        plex_client: PlexClient instance
        apple_music_client: AppleMusicClient instance
        clean_logger: CleanLogger instance
        
    Returns:
        Dictionary with statistics about the cleaning process
    """
    logger.info("Starting full library clean...")
    
    # Get all tracks from Plex
    plex_tracks = plex_client.get_all_tracks()
    
    # Get all tracks from Apple Music
    apple_tracks = apple_music_client.get_all_tracks()
    
    # Track statistics
    stats = {
        'total_tracks': len(plex_tracks),
        'matched_tracks': 0,
        'updated_tracks': 0,
        'title_updates': 0,
        'artist_updates': 0,
        'album_updates': 0,
        'skipped_tracks': 0
    }
    
    # Get already cleaned tracks to skip
    cleaned_tracks = clean_logger.get_cleaned_tracks()
    
    # Process each track
    for track in plex_tracks:
        # Skip already cleaned tracks
        if track.ratingKey in cleaned_tracks:
            stats['skipped_tracks'] += 1
            continue
            
        # Get file path from track
        file_path = None
        try:
            for media in track.media:
                for part in media.parts:
                    file_path = part.file
                    break
                if file_path:
                    break
        except Exception:
            continue
            
        if not file_path:
            continue
            
        # Try to find matching track in Apple Music
        apple_track = None
        
        # Try direct path match first
        if file_path in apple_tracks:
            apple_track = apple_tracks[file_path]
            stats['matched_tracks'] += 1
        else:
            # Try matching by filename
            filename = os.path.basename(file_path)
            for apple_path, metadata in apple_tracks.items():
                if os.path.basename(apple_path) == filename:
                    apple_track = metadata
                    stats['matched_tracks'] += 1
                    break
        
        if not apple_track:
            continue
            
        # Compare and update metadata
        updated = False
        
        # Check title
        if track.title != apple_track['title']:
            logger.info(f"Updating title for track {track.ratingKey}: '{track.title}' -> '{apple_track['title']}'")
            clean_logger.record_change(track.ratingKey, 'title', track.title, apple_track['title'])
            stats['title_updates'] += 1
            updated = True
            
        # Check artist
        track_artist = getattr(track, 'originalTitle', None) or track.grandparentTitle
        if track_artist != apple_track['artist']:
            logger.info(f"Updating artist for track {track.ratingKey}: '{track_artist}' -> '{apple_track['artist']}'")
            clean_logger.record_change(track.ratingKey, 'artist', track_artist, apple_track['artist'])
            stats['artist_updates'] += 1
            updated = True
            
        # Check album
        if track.parentTitle != apple_track['album']:
            logger.info(f"Updating album for track {track.ratingKey}: '{track.parentTitle}' -> '{apple_track['album']}'")
            clean_logger.record_change(track.ratingKey, 'album', track.parentTitle, apple_track['album'])
            stats['album_updates'] += 1
            updated = True
            
        # Update track metadata if needed
        if updated:
            plex_client.update_track_metadata(
                track,
                title=apple_track['title'],
                artist=apple_track['artist'],
                album=apple_track['album']
            )
            stats['updated_tracks'] += 1
    
    logger.info(f"Library clean complete. Updated {stats['updated_tracks']} of {stats['total_tracks']} tracks.")
    return stats


def clean_artist_tracks(plex_client: PlexClient, apple_music_client: AppleMusicClient, 
                       clean_logger: CleanLogger, artist_name: str) -> Dict[str, int]:
    """
    Clean metadata for tracks by a specific artist.
    
    Args:
        plex_client: PlexClient instance
        apple_music_client: AppleMusicClient instance
        clean_logger: CleanLogger instance
        artist_name: Name of the artist to clean
        
    Returns:
        Dictionary with statistics about the cleaning process
    """
    logger.info(f"Starting clean for artist: {artist_name}")
    
    # Get tracks for the artist from Plex
    plex_tracks = plex_client.get_tracks_by_artist(artist_name)
    
    # Get tracks for the artist from Apple Music
    apple_tracks = apple_music_client.get_tracks_by_artist(artist_name)
    
    # Track statistics
    stats = {
        'total_tracks': len(plex_tracks),
        'matched_tracks': 0,
        'updated_tracks': 0,
        'title_updates': 0,
        'artist_updates': 0,
        'album_updates': 0,
        'skipped_tracks': 0
    }
    
    # Get already cleaned tracks to skip
    cleaned_tracks = clean_logger.get_cleaned_tracks()
    
    # Process each track
    for track in plex_tracks:
        # Skip already cleaned tracks
        if track.ratingKey in cleaned_tracks:
            stats['skipped_tracks'] += 1
            continue
            
        # Get file path from track
        file_path = None
        try:
            for media in track.media:
                for part in media.parts:
                    file_path = part.file
                    break
                if file_path:
                    break
        except Exception:
            continue
            
        if not file_path:
            continue
            
        # Try to find matching track in Apple Music
        apple_track = None
        
        # Try direct path match first
        if file_path in apple_tracks:
            apple_track = apple_tracks[file_path]
            stats['matched_tracks'] += 1
        else:
            # Try matching by filename
            filename = os.path.basename(file_path)
            for apple_path, metadata in apple_tracks.items():
                if os.path.basename(apple_path) == filename:
                    apple_track = metadata
                    stats['matched_tracks'] += 1
                    break
        
        if not apple_track:
            continue
            
        # Compare and update metadata
        updated = False
        
        # Check title
        if track.title != apple_track['title']:
            logger.info(f"Updating title for track {track.ratingKey}: '{track.title}' -> '{apple_track['title']}'")
            clean_logger.record_change(track.ratingKey, 'title', track.title, apple_track['title'])
            stats['title_updates'] += 1
            updated = True
            
        # Check artist
        track_artist = getattr(track, 'originalTitle', None) or track.grandparentTitle
        if track_artist != apple_track['artist']:
            logger.info(f"Updating artist for track {track.ratingKey}: '{track_artist}' -> '{apple_track['artist']}'")
            clean_logger.record_change(track.ratingKey, 'artist', track_artist, apple_track['artist'])
            stats['artist_updates'] += 1
            updated = True
            
        # Check album
        if track.parentTitle != apple_track['album']:
            logger.info(f"Updating album for track {track.ratingKey}: '{track.parentTitle}' -> '{apple_track['album']}'")
            clean_logger.record_change(track.ratingKey, 'album', track.parentTitle, apple_track['album'])
            stats['album_updates'] += 1
            updated = True
            
        # Update track metadata if needed
        if updated:
            plex_client.update_track_metadata(
                track,
                title=apple_track['title'],
                artist=apple_track['artist'],
                album=apple_track['album']
            )
            stats['updated_tracks'] += 1
    
    logger.info(f"Artist clean complete. Updated {stats['updated_tracks']} of {stats['total_tracks']} tracks.")
    return stats


def sync_playlist(plex_client: PlexClient, apple_music_client: AppleMusicClient, 
                 playlist_name: str) -> Dict[str, int]:
    """
    Sync a playlist from Apple Music to Plex.
    
    Args:
        plex_client: PlexClient instance
        apple_music_client: AppleMusicClient instance
        playlist_name: Name of the playlist to sync
        
    Returns:
        Dictionary with statistics about the sync process
    """
    logger.info(f"Starting sync for playlist: {playlist_name}")
    
    # Get tracks in the playlist from Apple Music
    apple_track_paths = apple_music_client.get_playlist_tracks(playlist_name)
    
    # Track statistics
    stats = {
        'total_tracks': len(apple_track_paths),
        'matched_tracks': 0,
        'missing_tracks': 0
    }
    
    # Find matching tracks in Plex
    plex_tracks = []
    missing_tracks = []
    
    for file_path in apple_track_paths:
        # Try to find matching track in Plex
        plex_track = plex_client.find_track_by_filename(file_path)
        
        if plex_track:
            plex_tracks.append(plex_track)
            stats['matched_tracks'] += 1
        else:
            missing_tracks.append(file_path)
            stats['missing_tracks'] += 1
    
    # Create playlist in Plex
    if plex_tracks:
        success = plex_client.create_playlist(playlist_name, plex_tracks)
        if success:
            logger.info(f"Created playlist '{playlist_name}' with {len(plex_tracks)} tracks")
        else:
            logger.error(f"Failed to create playlist '{playlist_name}'")
    else:
        logger.error(f"No matching tracks found for playlist '{playlist_name}'")
    
    # Log missing tracks
    if missing_tracks:
        logger.warning(f"Could not find {len(missing_tracks)} tracks in Plex:")
        for path in missing_tracks[:10]:  # Log first 10 missing tracks
            logger.warning(f"  - {os.path.basename(path)}")
        if len(missing_tracks) > 10:
            logger.warning(f"  ... and {len(missing_tracks) - 10} more")
    
    return stats


def interactive_clean_all(plex_client: PlexClient, apple_music_client: AppleMusicClient, 
                         clean_logger: CleanLogger, threshold: int = 10) -> Dict[str, int]:
    """
    Interactive clean of all artists in the Plex library.
    
    Args:
        plex_client: PlexClient instance
        apple_music_client: AppleMusicClient instance
        clean_logger: CleanLogger instance
        threshold: Maximum number of tracks to list individually
        
    Returns:
        Dictionary with statistics about the cleaning process
    """
    logger.info("Starting interactive library clean...")
    
    # Get all tracks from Plex
    plex_tracks = plex_client.get_all_tracks()
    
    # Group tracks by artist
    artists_tracks = defaultdict(list)
    for track in plex_tracks:
        artist = getattr(track, 'originalTitle', None) or track.grandparentTitle
        artists_tracks[artist].append(track)
    
    # Sort artists alphabetically
    sorted_artists = sorted(artists_tracks.keys())
    
    # Track statistics
    stats = {
        'total_artists': len(sorted_artists),
        'processed_artists': 0,
        'skipped_artists': 0,
        'total_tracks': len(plex_tracks),
        'updated_tracks': 0,
        'title_updates': 0,
        'artist_updates': 0,
        'album_updates': 0
    }
    
    print("\n===== Plex Music Library Cleaner =====")
    print(f"Found {len(sorted_artists)} artists with {len(plex_tracks)} total tracks")
    print("Starting interactive clean process. For each artist, you can:")
    print("  [y] - Clean this artist's tracks")
    print("  [n] - Skip this artist for now")
    print("  [e] - Exit the cleaning process")
    print("========================================\n")
    
    # Process each artist
    for artist_name in sorted_artists:
        tracks = artists_tracks[artist_name]
        track_count = len(tracks)
        
        print(f"\nArtist: {artist_name}")
        print(f"Tracks: {track_count}")
        
        # Group tracks by album for artists with many tracks
        if track_count > threshold:
            # Get unique albums
            albums = {}
            for track in tracks:
                album = track.parentTitle
                if album not in albums:
                    albums[album] = 0
                albums[album] += 1
            
            # Display albums
            print("\nAlbums:")
            for album, count in albums.items():
                print(f"  - {album} ({count} tracks)")
        else:
            # Display individual tracks for artists with few tracks
            print("\nTracks:")
            for track in tracks:
                print(f"  - {track.title} (Album: {track.parentTitle})")
        
        # Prompt user for action
        while True:
            choice = input("\nClean this artist? [y]es/[n]o/[e]xit: ").lower()
            
            if choice in ('y', 'yes'):
                # Clean this artist's tracks
                artist_stats = clean_artist_tracks(plex_client, apple_music_client, clean_logger, artist_name)
                
                # Update overall statistics
                stats['processed_artists'] += 1
                stats['updated_tracks'] += artist_stats['updated_tracks']
                stats['title_updates'] += artist_stats['title_updates']
                stats['artist_updates'] += artist_stats['artist_updates']
                stats['album_updates'] += artist_stats['album_updates']
                
                # Display results for this artist
                print(f"\nCleaned {artist_stats['updated_tracks']} of {artist_stats['total_tracks']} tracks for {artist_name}")
                print(f"  Title updates: {artist_stats['title_updates']}")
                print(f"  Artist updates: {artist_stats['artist_updates']}")
                print(f"  Album updates: {artist_stats['album_updates']}")
                print(f"  Skipped (already cleaned): {artist_stats['skipped_tracks']}")
                break
            elif choice in ('n', 'no'):
                # Skip this artist
                stats['skipped_artists'] += 1
                print(f"Skipped {artist_name}")
                break
            elif choice in ('e', 'exit'):
                # Exit the cleaning process
                print("Exiting clean process...")
                return stats
            else:
                print("Invalid choice. Please enter 'y', 'n', or 'e'.")
    
    # Display overall results
    print("\n===== Cleaning Complete =====")
    print(f"Processed {stats['processed_artists']} of {stats['total_artists']} artists")
    print(f"Updated {stats['updated_tracks']} of {stats['total_tracks']} tracks")
    print(f"Title updates: {stats['title_updates']}")
    print(f"Artist updates: {stats['artist_updates']}")
    print(f"Album updates: {stats['album_updates']}")
    print(f"Skipped artists: {stats['skipped_artists']}")
    
    return stats


def interactive_menu(plex_client: PlexClient, apple_music_client: AppleMusicClient, 
                    clean_logger: CleanLogger) -> None:
    """
    Display an interactive menu for the user.
    
    Args:
        plex_client: PlexClient instance
        apple_music_client: AppleMusicClient instance
        clean_logger: CleanLogger instance
    """
    while True:
        print("\n===== Plex Music Cleaner =====")
        print("1. Clean entire library")
        print("2. Clean tracks for specific artist")
        print("3. Sync playlist from Apple Music to Plex")
        print("4. View cleaning statistics")
        print("0. Exit")
        
        choice = input("\nEnter your choice (0-4): ")
        
        if choice == '0':
            break
        elif choice == '1':
            stats = interactive_clean_all(plex_client, apple_music_client, clean_logger)
            print("\nCleaning complete!")
            print(f"Processed {stats['processed_artists']} of {stats['total_artists']} artists")
            print(f"Updated {stats['updated_tracks']} of {stats['total_tracks']} tracks")
            print(f"Title updates: {stats['title_updates']}")
            print(f"Artist updates: {stats['artist_updates']}")
            print(f"Album updates: {stats['album_updates']}")
        elif choice == '2':
            artist_name = input("Enter artist name: ")
            stats = clean_artist_tracks(plex_client, apple_music_client, clean_logger, artist_name)
            print("\nCleaning complete!")
            print(f"Total tracks: {stats['total_tracks']}")
            print(f"Matched tracks: {stats['matched_tracks']}")
            print(f"Updated tracks: {stats['updated_tracks']}")
            print(f"Title updates: {stats['title_updates']}")
            print(f"Artist updates: {stats['artist_updates']}")
            print(f"Album updates: {stats['album_updates']}")
            print(f"Skipped tracks: {stats['skipped_tracks']}")
        elif choice == '3':
            playlist_name = input("Enter playlist name: ")
            stats = sync_playlist(plex_client, apple_music_client, playlist_name)
            print("\nPlaylist sync complete!")
            print(f"Total tracks in playlist: {stats['total_tracks']}")
            print(f"Matched tracks: {stats['matched_tracks']}")
            print(f"Missing tracks: {stats['missing_tracks']}")
        elif choice == '4':
            stats = clean_logger.get_stats()
            print("\nCleaning Statistics:")
            print(f"Total changes made: {stats.get('total_changes', 0)}")
            print(f"Tracks modified: {stats.get('tracks_changed', 0)}")
            print("Changes by field:")
            for field, count in stats.items():
                if field not in ('total_changes', 'tracks_changed', 'error'):
                    print(f"  - {field}: {count}")
        else:
            print("Invalid choice. Please try again.")


def main():
    """Main entry point for the script."""
    # Load environment variables
    dotenv.load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sync Plex music metadata with Apple Music')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Clean all command
    clean_all_parser = subparsers.add_parser('clean-all', help='Clean metadata for all tracks')
    
    # Clean artist command
    clean_artist_parser = subparsers.add_parser('clean-artist', help='Clean metadata for tracks by a specific artist')
    clean_artist_parser.add_argument('--name', required=True, help='Name of the artist')
    
    # Sync playlist command
    sync_playlist_parser = subparsers.add_parser('sync-playlist', help='Sync a playlist from Apple Music to Plex')
    sync_playlist_parser.add_argument('--name', required=True, help='Name of the playlist')
    
    args = parser.parse_args()
    
    # ------------------------------------------------------------------
    # Apple Music source-of-truth selection
    # Prefer an XML export (dropped in the repo root) if one is present.
    # Otherwise fall back to the SQLite-based AppleMusicClient that works
    # with the .musiclibrary bundle / network share / ssh.
    # ------------------------------------------------------------------
    xml_used = False
    xml_path: Optional[str] = None
    xml_candidates = [f for f in os.listdir('.') if f.lower().endswith('.xml')]
    if xml_candidates and AppleMusicXMLClient:
        # Use the first XML file found
        xml_path = os.path.abspath(xml_candidates[0])
        xml_used = True
        logger.info(f\"Using Apple Music XML library: {xml_path}\")

    # Check required environment variables
    required_vars = ['SOOBIN_URL', 'SOOBIN_TOKEN', 'MUSIC_SECTION']
    if not xml_used:
        # We'll still need the path to the .musiclibrary / db
        required_vars.append('LIBRARY_MUSICFILE')
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in the .env file")
        sys.exit(1)
    
    # Initialize clients
    try:
        plex_client = PlexClient(
            os.environ.get('SOOBIN_URL'),
            os.environ.get('SOOBIN_TOKEN'),
            int(os.environ.get('MUSIC_SECTION'))
        )

        # Instantiate the appropriate Apple Music client
        if xml_used:
            try:
                apple_music_client = AppleMusicXMLClient(xml_path)  # type: ignore
            except Exception as exc:
                logger.error(f"Failed to load Apple Music XML library: {exc}")
                sys.exit(1)
        else:
            apple_music_client = AppleMusicClient(
                os.environ.get('LIBRARY_MUSICFILE')
            )
        
        clean_logger = CleanLogger()
        
        # Run the appropriate command
        if args.command == 'clean-all':
            interactive_clean_all(plex_client, apple_music_client, clean_logger)
        elif args.command == 'clean-artist':
            clean_artist_tracks(plex_client, apple_music_client, clean_logger, args.name)
        elif args.command == 'sync-playlist':
            sync_playlist(plex_client, apple_music_client, args.name)
        else:
            # No command specified, show interactive menu
            interactive_menu(plex_client, apple_music_client, clean_logger)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up resources
        if 'apple_music_client' in locals():
            apple_music_client.close()
        if 'clean_logger' in locals():
            clean_logger.close()


if __name__ == "__main__":
    main()
