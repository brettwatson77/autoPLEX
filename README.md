# autoPLEX – Keep Plex Music in Sync with Apple Music

autoPLEX is a small toolkit that **copies the truth of your Apple Music / iTunes
library into Plex**.  It matches tracks by the physical media files you already
copied into Plex, overwrites Plex’s agent-scraped metadata so albums, titles and
artists are identical, and can recreate Apple Music playlists inside Plex.

Key features
------------
* Compare & update **artist / album / track titles** in Plex so they match
  Apple Music exactly – no “clean-up” or punctuation stripping, just a direct
  copy.  
* Works from an **Apple Music XML export** (preferred) *or* directly against the
  `.musiclibrary` SQLite database (local, UNC path or SSH).  
* Interactive “clean all” that lets you step through artists and confirm before
  changes are written.  
* Command-line options for full-library clean, single-artist clean or playlist
  sync.  
* **SQLite change log** tracks every field updated so you can resume later or
  audit changes.


Prerequisites
-------------
* Python 3.9+ (tested on Win 10/11 with PowerShell 7)  
* A Plex server reachable on your LAN + an API token  
* Apple Music (macOS) on the same network  
  * Option A – **Export Library XML** (`Music ▸ File ▸ Library ▸ Export…`) and
    place it in the project root (recommended)  
  * Option B – Provide a path/UNC share to the `.musiclibrary` bundle or an SSH
    path to the Mac  


Installation
------------
```powershell
# clone and enter the project
git clone https://github.com/brettwatson77/autoPLEX.git
cd autoPLEX

# create & activate a virtual-env (optional but recommended)
python -m venv venv
.\venv\Scripts\Activate.ps1     # on Windows / PowerShell 7

# install dependencies
pip install -r requirements.txt
```


Configuration (.env)
--------------------
Create a `.env` file (or edit the provided sample) with at least:

```
SOOBIN_URL=http://192.168.1.5:32400
SOOBIN_TOKEN=PlexTokenHere

# If you are **not** using an XML export:
LIBRARY_MUSICFILE=\\SOOBIN\Macintosh HD\Users\me\Music\Music\Music Library.musiclibrary

# Plex library section IDs
MUSIC_SECTION=27

# Optional – default playlist IDs / sections if you need them elsewhere
VIDEO_SECTION=36
```

If an XML file ending in `.xml` exists in the project root, it will be used in
place of `LIBRARY_MUSICFILE`.


Usage
-----
Interactive menu (recommended for first run):
```powershell
python plex_music_cleaner.py
```

Direct commands:
```powershell
# Clean every artist (interactive per-artist prompts)
python plex_music_cleaner.py clean-all

# Clean one artist immediately
python plex_music_cleaner.py clean-artist --name "Daft Punk"

# Recreate an Apple Music playlist in Plex
python plex_music_cleaner.py sync-playlist --name "Road Trip Mix"
```

Command reference
-----------------
| Command | Description |
|---------|-------------|
| (none)  | Launches an interactive TUI menu |
| `clean-all` | Walk every artist alphabetically; prompt `[y]es/[n]o/[e]xit` |
| `clean-artist --name "<artist>"` | Clean metadata for a single artist |
| `sync-playlist --name "<playlist>"` | Look up playlist in Apple Music & create in Plex |


Development & Contributing
--------------------------
Pull requests are welcome – please open an issue first to discuss what you’d
like to change.  Follow the existing code style and keep patches minimal and
focused.


License
-------
This project is licensed under the MIT License – see the [LICENSE](LICENSE)
file for details.