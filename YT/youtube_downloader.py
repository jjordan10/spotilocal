import os
import logging
from typing import Optional, Dict, Any
import requests
from ytmusicapi import YTMusic
from pytubefix import YouTube
from pytubefix.cli import on_progress
from pydub import AudioSegment
from mutagen.id3 import ID3, ID3NoHeaderError, APIC, TIT2, TPE1, TALB, USLT
from mutagen.mp4 import MP4, MP4Cover
from concurrent.futures import ThreadPoolExecutor, as_completed


class YouTubeAudioDownloader:
    """
    A class to download audio from YouTube videos, convert to MP3,
    and add metadata and thumbnail.

    Attributes:
        ytmusic (YTMusic): An instance of YTMusic API.
        logger (logging.Logger): Logger for the downloader.
    """

    def __init__(self, output_path: str = '.', log_level: int = logging.INFO):
        """
        Initializes the YouTubeAudioDownloader.

        Args:
            output_path (str, optional): Directory to save downloaded files. Defaults to '.'.
            log_level (int, optional): Logging level. Defaults to logging.INFO.
        """
        self.output_path = output_path
        self.ytmusic = YTMusic()
        self.logger = self._setup_logger(log_level)

    @staticmethod
    def _setup_logger(log_level: int) -> logging.Logger:
        """
        Sets up the logger for the downloader.

        Args:
            log_level (int): Logging level.

        Returns:
            logging.Logger: Configured logger.
        """
        logger = logging.getLogger('YouTubeAudioDownloader')
        logger.setLevel(log_level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not logger.handlers:
            logger.addHandler(handler)
        return logger

    def download_audio(
        self,
        video_url: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Downloads audio from a YouTube video URL, converts it to MP3,
        and adds metadata.

        Args:
            video_url (str): The URL of the YouTube video.
            metadata (Optional[Dict[str, Any]], optional): Metadata to embed. Defaults to None.

        Returns:
            Optional[str]: Path to the downloaded MP3 file, or None if failed.
        """
        try:
            yt = YouTube(video_url, on_progress_callback=on_progress)
            audio_stream = yt.streams.filter(only_audio=True).first()

            if not audio_stream:
                self.logger.warning('No audio streams available for this video.')
                return None

            self.logger.info(f'Downloading audio for: {yt.title}')
            downloaded_file = audio_stream.download(self.output_path)
            self.logger.debug(f'Audio downloaded to: {downloaded_file}')

            mp3_file = self._convert_to_mp3(downloaded_file)
            self.logger.info(f'Converted to MP3: {mp3_file}')
            thumbnail_url = yt.thumbnail_url.replace('/sd','/maxres')
            if metadata:
                self._add_metadata(mp3_file, metadata, thumbnail_url)
                self.logger.info('Metadata added to MP3 file.')

            return mp3_file

        except Exception as e:
            self.logger.error(f'Error downloading audio: {e}', exc_info=True)
            return None

    def _convert_to_mp3(self, file_path: str) -> str:
        """
        Converts an audio file to MP3 format.

        Args:
            file_path (str): Path to the downloaded audio file.

        Returns:
            str: Path to the converted MP3 file.
        """
        try:
            mp3_file_path = os.path.splitext(file_path)[0] + ".mp3"
            audio = AudioSegment.from_file(file_path)
            audio.export(mp3_file_path, format="mp3")
            os.remove(file_path)
            self.logger.debug(f'Original file removed: {file_path}')
            return mp3_file_path
        except Exception as e:
            self.logger.error(f'Error converting to MP3: {e}', exc_info=True)
            raise

    def _add_metadata(
        self,
        file_path: str,
        metadata: Dict[str, Any],
        thumbnail_url: Optional[str]
    ) -> None:
        """
        Adds metadata and thumbnail to an MP3 file.

        Args:
            file_path (str): Path to the MP3 file.
            metadata (Dict[str, Any]): Metadata to embed.
            thumbnail_url (Optional[str]): URL of the thumbnail image.
        """
        try:
            audio = ID3(file_path)
        except ID3NoHeaderError:
            audio = ID3()

        # Add standard metadata
        audio.add(TIT2(encoding=3, text=metadata.get('title', 'Unknown Title')))
        audio.add(TPE1(encoding=3, text=metadata.get('artist', 'Unknown Artist')))
        audio.add(TALB(encoding=3, text=metadata.get('album', 'Unknown Album')))

        # Add lyrics if available
        lyrics = metadata.get('lyrics')
        if lyrics:
            audio.add(USLT(encoding=3, lang='eng', desc='Lyrics', text=lyrics))

        # Add thumbnail if URL is provided
        if thumbnail_url:
            thumbnail_path = self._download_thumbnail(thumbnail_url, file_path)
            if thumbnail_path:
                with open(thumbnail_path, 'rb') as img:
                    audio.add(APIC(
                        encoding=3,
                        mime='image/jpeg',
                        type=3,  # Cover (front)
                        desc='Cover',
                        data=img.read()
                    ))
                os.remove(thumbnail_path)
                self.logger.debug(f'Thumbnail added and temporary file removed: {thumbnail_path}')

        # Save the tags
        try:
            audio.save(file_path)
        except Exception as e:
            self.logger.error(f'Error saving metadata: {e}', exc_info=True)
            raise

    def _download_thumbnail(self, thumbnail_url: str, file_path: str) -> Optional[str]:
        """
        Downloads the thumbnail image from a URL.

        Args:
            thumbnail_url (str): URL of the thumbnail image.
            file_path (str): Path to the audio file (for naming purposes).

        Returns:
            Optional[str]: Path to the downloaded thumbnail image, or None if failed.
        """
        try:
            response = requests.get(thumbnail_url, timeout=10)
            response.raise_for_status()
            thumbnail_path = os.path.splitext(file_path)[0] + ".jpg"
            with open(thumbnail_path, 'wb') as f:
                f.write(response.content)
            self.logger.debug(f'Thumbnail downloaded to: {thumbnail_path}')
            return thumbnail_path
        except requests.RequestException as e:
            self.logger.warning(f'Failed to download thumbnail: {e}')
            return None
        except Exception as e:
            self.logger.error(f'Unexpected error downloading thumbnail: {e}', exc_info=True)
            return None

    def download_song(self, song_data):
        song, index, limit = song_data
        metadata = {
            "title": song.get('title', 'Unknown Title'),
            "artist": song.get('artists', [{'name': 'Unknown Artist'}])[0].get('name', 'Unknown Artist'),
            "album": song.get('album', {}).get('name', 'Unknown Album'),
            "lyrics": self._fetch_lyrics(song.get('videoId'))
        }
        video_url = f"https://www.youtube.com/watch?v={song.get('videoId')}"
        
        if len(song_data) < limit:
            self.logger.info(f'Downloading [{index}/{len(song_data)}]: {metadata["title"]} by {metadata["artist"]}')
        else:
            self.logger.info(f'Downloading [{index}/{limit}]: {metadata["title"]} by {metadata["artist"]}')
        
        self.download_audio(video_url, metadata=metadata)
        return f"{metadata['title']} by {metadata['artist']}"

    def search_and_download_song(self, search_query: str, limit: int = 1) -> None:
        """
        Searches for songs on YouTube Music and downloads the audio for the top results.

        Args:
            search_query (str): Search term for YouTube Music.
            limit (int, optional): Number of top results to download. Defaults to 1.
        """
        try:
            results = self.ytmusic.search(search_query, limit=limit, filter='songs')
            if not results:
                self.logger.warning(f'No results found for query: "{search_query}"')
                return

            # Prepare a list of tasks
            tasks = [(song, index, limit) for index, song in enumerate(results[:limit], start=1)]

            # Use ThreadPoolExecutor to download songs concurrently
            with ThreadPoolExecutor() as executor:
                future_to_song = {executor.submit(self.download_song, task): task for task in tasks}
                for future in as_completed(future_to_song):
                    try:
                        result = future.result()
                        self.logger.info(f'Finished downloading: {result}')
                    except Exception as e:
                        self.logger.error(f'Error downloading a song: {e}', exc_info=True)

        except Exception as e:
            self.logger.error(f'Error in search/download process: {e}', exc_info=True)

    def _fetch_lyrics(self, video_id: Optional[str]) -> str:
        """
        Fetches lyrics for a given video ID from YouTube Music.

        Args:
            video_id (Optional[str]): YouTube video ID.

        Returns:
            str: Lyrics if available, else an empty string.
        """
        if not video_id:
            return ''

        try:
            playlist = self.ytmusic.get_watch_playlist(video_id)
            lyrics_id = playlist.get('lyrics')
            if not lyrics_id:
                return ''

            lyrics_data = self.ytmusic.get_lyrics(lyrics_id)
            return lyrics_data.get('lyrics', '')

        except Exception:
            self.logger.debug(f'No lyrics found for video ID: {video_id}')
            return ''


if __name__ == "__main__":
    # Example usage
    downloader = YouTubeAudioDownloader(output_path='bts', log_level=logging.DEBUG)
    search_query = "bts"
    downloader.search_and_download_song(search_query,limit=1000)