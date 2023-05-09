# Download data from RackSpace

This Notebook is used to download media files from RackSpace (rs) using a list of paths of the files GRRID is using to read from rs.

It creates two folders
- "logs/" - for the log files where each line corresponds to a media download attempt, if '[[MP3  NOT VALID]]' this means the media download is not a valid mp3 file, chances are that the specific path to download is not in RackSpace, which can be used to identify which messages or songs are not in RackSpace.
- "media/" - for the media files downloaded following the same structure as in RackSpace ("messaging_{n}" and "music_{n}" having only files which its last digit corresponds to "n")