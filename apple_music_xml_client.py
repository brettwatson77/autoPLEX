#!/usr/bin/env python3
"""
apple_music_xml_client.py - Parser for iTunes/Apple Music XML library exports

This module provides functionality to read and extract data from iTunes or
Apple Music XML library exports, making track and playlist information
available for the Plex music cleaner.
"""

import os
import sys
import logging
import plistlib
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Any

# Configure logging
logger = logging.getLogger(__name__)

class AppleMusicXMLClient:
    """Client for accessing Apple Music data from XML library exports."""
    
    def __init__(self, xml_path: str):
        """
        Initialize the Apple Music XML client.
        
        Args:
            xml_path: Path to the iTunes/Apple Music XML library export
        """
        self.xml_path = xml_path
        self.track_map = {}  # Maps file paths to metadata
        self.id_map = {}     # Maps track IDs to file paths
        self.playlists = {}  # Maps playlist names to lists of file paths
        
        self._load_library()
        
    def _load_library(self) -> None:
        """Load the XML library file and build the track and playlist maps."""
        try:
            logger.info(f"Loading Apple Music XML library from: {self.xml_path}")
            
            if not os.path.exists(self.xml_path):
                logger.error(f"XML library file not found: {self.xml_path}")
                raise FileNotFoundError(f"XML library file not found: {self.xml_path}")
            
            # Load the plist XML file
            with open(self.xml_path, 'rb') as f:
                library = plistlib.load(f)
                
            # Process tracks
            if 'Tracks' in library:
                self._process_tracks(library['Tracks'])
                logger.info(f"Processed {len(self.track_map)} tracks from XML library")
            else:
                logger.warning("No tracks found in XML library")
                
            # Process playlists
            if 'Playlists' in library:
                self._process_playlists(library['Playlists'])
                logger.info(f"Processed {len(self.playlists)} playlists from XML library")
            else:
                logger.warning("No playlists found in XML library")
                
        except Exception as e:
            logger.error(f"Failed to load Apple Music XML library: {str(e)}")
            raise
            
    def _process_tracks(self, tracks_dict: Dict[str, Any]) -> None:
        """
        Process the tracks dictionary from the XML library.
        
        Args:
            tracks_dict: Dictionary of tracks from the XML library
        """
        for track_id, track_data in tracks_dict.items():
            # Skip tracks without a location (e.g., Apple Music streaming tracks)
            if 'Location' not in track_data:
                continue
                
            # Decode the file URL
            file_path = self._decode_file_url(track_data['Location'])
            if not file_path:
                continue
                
            # Extract metadata
            metadata = {
                'title': track_data.get('Name', ''),
                'artist': track_data.get('Artist', ''),
                'album': track_data.get('Album', '')
            }
            
            # Add to mappings
            self.track_map[file_path] = metadata
            self.id_map[track_id] = file_path
            
    def _process_playlists(self, playlists_list: List[Dict[str, Any]]) -> None:
        """
        Process the playlists list from the XML library.
        
        Args:
            playlists_list: List of playlists from the XML library
        """
        for playlist in playlists_list:
            # Skip system playlists
            if playlist.get('Master', False) or playlist.get('Distinguished Kind', None) is not None:
                continue
                
            playlist_name = playlist.get('Name', '')
            if not playlist_name:
                continue
                
            # Get playlist tracks
            tracks = []
            playlist_items = playlist.get('Playlist Items', [])
            
            for item in playlist_items:
                track_id = str(item.get('Track ID', ''))
                if track_id in self.id_map:
                    tracks.append(self.id_map[track_id])
                    
            if tracks:
                self.playlists[playlist_name] = tracks
                logger.debug(f"Playlist '{playlist_name}' contains {len(tracks)} tracks")
                
    def _decode_file_url(self, file_url: str) -> Optional[str]:
        """
        Decode an Apple Music file URL to a file path.
        
        Args:
            file_url: URL string from the XML library
            
        Returns:
            Decoded file path or None if decoding fails
        """
        try:
            if file_url.startswith('file://'):
                # Remove the file:// prefix
                path = file_url[7:]
                
                # URL decode the path
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
            logger.error(f"Failed to decode file URL: {str(e)}")
            return None
            
    def get_all_tracks(self) -> Dict[str, Dict]:
        """
        Retrieve all tracks from the Apple Music library.
        
        Returns:
            Dictionary mapping file paths to metadata dictionaries
        """
        return self.track_map
        
    def get_tracks_by_artist(self, artist_name: str) -> Dict[str, Dict]:
        """
        Retrieve tracks for a specific artist from the Apple Music library.
        
        Args:
            artist_name: Name of the artist to filter by (case-insensitive substring match)
            
        Returns:
            Dictionary mapping file paths to metadata dictionaries
        """
        artist_name_lower = artist_name.lower()
        return {
            path: metadata
            for path, metadata in self.track_map.items()
            if artist_name_lower in metadata['artist'].lower()
        }
        
    def get_playlist_tracks(self, playlist_name: str) -> List[str]:
        """
        Retrieve tracks in a playlist from the Apple Music library.
        
        Args:
            playlist_name: Name of the playlist (case-insensitive match)
            
        Returns:
            List of file paths for tracks in the playlist
        """
        # Try exact match first
        if playlist_name in self.playlists:
            return self.playlists[playlist_name]
            
        # Try case-insensitive match
        playlist_name_lower = playlist_name.lower()
        for name, tracks in self.playlists.items():
            if name.lower() == playlist_name_lower:
                return tracks
                
        # Try substring match as a last resort
        for name, tracks in self.playlists.items():
            if playlist_name_lower in name.lower():
                logger.info(f"Using partial match for playlist: '{name}'")
                return tracks
                
        logger.warning(f"Playlist '{playlist_name}' not found")
        return []
