import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from YT.youtube_downloader import YouTubeAudioDownloader
from spotify.spotify import SpotifyPlaylistScraper
import os

class SpotifyPlaylistDownloader:
    def __init__(self, playlist_urls, output_path='ipod'):
        """
        Initialize the downloader with multiple playlist URLs.

        Args:
            playlist_urls (list): List of Spotify playlist URLs.
            output_path (str): Directory where downloaded songs will be saved.
        """
        self.playlist_urls = playlist_urls  # List of Spotify playlist URLs
        self.output_path = output_path
        # Ensure the output directory exists
        os.makedirs(self.output_path, exist_ok=True)

    @staticmethod
    def _download_single_playlist(playlist_url, output_path):
        """
        Helper method to download a single playlist.
        This method is defined as a static method to make it picklable
        for multiprocessing.

        Args:
            playlist_url (str): The Spotify playlist URL.
            output_path (str): Directory where downloaded songs will be saved.
        """
        # Initialize scraper and downloader for the specific playlist
        scraper = SpotifyPlaylistScraper(playlist_url=playlist_url)
        downloader = YouTubeAudioDownloader(output_path=output_path)

        # Fetch tracks from the playlist
        try:
            extracted_tracks = scraper.scrape_playlist()
            print(f"Fetched {len(extracted_tracks)} tracks from playlist: {playlist_url}")
        except Exception as e:
            print(f"Failed to scrape playlist {playlist_url}: {e}")
            return

        # Function to download a single song
        def download_song(track):
            song_name = track['song_name']
            artists = ', '.join(track['artists'])
            search_query = f"{song_name}, {artists}"
            print(f"[Playlist: {playlist_url}] Downloading: {search_query}")
            try:
                downloader.download_song_with_retries(search_query)
            except Exception as e:
                print(f"Error downloading {search_query}: {e}")

        # Use ThreadPoolExecutor to download songs concurrently within the playlist
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(download_song, extracted_tracks)

        print(f"Finished downloading playlist: {playlist_url}")

    def download_multiple_playlists(self):
        """
        Download all playlists concurrently, each in a separate process.

        Args:
            max_workers (int): Maximum number of processes to use.
        """
        with ProcessPoolExecutor() as executor:
            # Prepare arguments for each playlist
            futures = [
                executor.submit(self._download_single_playlist, playlist_url, self.output_path)
                for playlist_url in self.playlist_urls
            ]
            # Optionally, wait for all futures to complete and handle exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"An error occurred during playlist download: {e}")

if __name__ == "__main__":
    # List of Spotify playlist URLs
    playlist_urls = [
        'https://open.spotify.com/playlist/37i9dQZEVXbOa2lmxNORXQ?si=de2e8149c0f44284',
        'https://open.spotify.com/playlist/37i9dQZF1E8M8u5oOxrXqr?si=5f86d4c5929f451b',  # Replace with actual URLs
        'https://open.spotify.com/playlist/37i9dQZF1E8PA7o7uuF5Vh?si=506d5c057aa24634',
        'https://open.spotify.com/playlist/37i9dQZF1E8KwG1PHffM74?si=4af9d7e05ab34b6d',
        'https://open.spotify.com/playlist/37i9dQZF1E8UuxKVbjM0QW?si=ccd7952d7ed5437c',
        'https://open.spotify.com/playlist/1PRKYYH7Id33YRz9Mkc87R?si=72d72a6d8a8943e4',
        'https://open.spotify.com/playlist/02jm5CivflNBWhu44Gfy8J?si=56ef620a4e184f6f',
        'https://open.spotify.com/playlist/2k2YShNuDEJKpFPeFtTSW8?si=3ecf82ee16e346c5',
        'https://open.spotify.com/playlist/6DaFGvMyXSfZ48ty6qXhpG?si=e6b713a8468a4b87',
        'https://open.spotify.com/playlist/37i9dQZF1E8LI7y3xM7Rxa?si=7a8df657710d4a67',
        'https://open.spotify.com/playlist/37i9dQZF1E8UoS7GiNwvrN?si=219e7fc621764573',
        'https://open.spotify.com/playlist/37i9dQZF1E8LcPC0ewaL1A?si=75e476f48c984b91',
        'https://open.spotify.com/playlist/69DvrHsB8LkP1HtZrWQYJL?si=3a2a3d53cff44616',
        'https://open.spotify.com/playlist/37i9dQZF1E8ESekQUzrCud?si=2aaccae0ac1e4148',
        'https://open.spotify.com/playlist/37i9dQZF1DXbvPjXfc8G9S?si=f3f7b889b89e4f01',
        'https://open.spotify.com/playlist/7oVjzQJsQg7b0gS1qmQjcV?si=bf6b05d5b9b24da7'
    ]

    # Initialize the downloader with the list of playlists and desired output directory
    downloader = SpotifyPlaylistDownloader(playlist_urls=playlist_urls, output_path='ipod')

    # Start downloading all playlists
    downloader.download_multiple_playlists()  # Adjust max_workers as needed