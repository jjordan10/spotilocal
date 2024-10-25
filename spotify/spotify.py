import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import logging
from selenium.webdriver.chrome.options import Options
# Optional: Import your YouTubeAudioDownloader if needed
# from YT import YouTubeAudioDownloader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyPlaylistScraper:
    def __init__(self, playlist_url, window_size=(1280*5*10, 800*5), zoom_level='4%', wait_initial=3, wait_zoom=1):
        """
        Initializes the SpotifyPlaylistScraper with the given parameters.

        Args:
            playlist_url (str): The URL of the Spotify playlist to scrape.
            window_size (tuple, optional): The window size for the browser. Defaults to (1280, 800).
            zoom_level (str, optional): The zoom level for the page. Defaults to '4%'.
            wait_initial (int, optional): Initial wait time after loading the page. Defaults to 5 seconds.
            wait_zoom (int, optional): Wait time after setting the zoom level. Defaults to 3 seconds.
        """
        self.playlist_url = playlist_url
        self.window_size = window_size
        self.zoom_level = zoom_level
        self.wait_initial = wait_initial
        self.wait_zoom = wait_zoom
        self.driver = None

    def setup_driver(self):
        """Sets up the Selenium WebDriver with Chrome."""
        logger.info("Setting up Chrome WebDriver.")
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            self.driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=chrome_options
            )
            self.driver.set_window_size(*self.window_size)
            logger.info(f"Browser window size set to: {self.window_size}")
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {e}")
            raise

    def load_playlist(self):
        """Navigates to the Spotify playlist URL and waits for the page to load."""
        if not self.driver:
            raise Exception("WebDriver is not initialized. Call setup_driver() first.")
        
        logger.info(f"Navigating to playlist URL: {self.playlist_url}")
        try:
            self.driver.get(self.playlist_url)
            logger.info(f"Waiting for {self.wait_initial} seconds to allow the page to load.")
            time.sleep(self.wait_initial)
        except Exception as e:
            logger.error(f"Error loading playlist URL: {e}")
            self.close_driver()
            raise

    def adjust_zoom(self):
        """Adjusts the zoom level of the webpage and waits for the adjustment to take effect."""
        if not self.driver:
            raise Exception("WebDriver is not initialized. Call setup_driver() first.")
        
        try:
            logger.info(f"Setting page zoom to: {self.zoom_level}")
            self.driver.execute_script(f"document.body.style.zoom='{self.zoom_level}'")
            logger.info(f"Waiting for {self.wait_zoom} seconds after setting zoom.")
            time.sleep(self.wait_zoom)
        except Exception as e:
            logger.error(f"Error adjusting zoom level: {e}")
            self.close_driver()
            raise

    def extract_tracks(self):
        """
        Extracts track information from the Spotify playlist.

        Returns:
            list of dict: A list containing dictionaries with 'song_name' and 'artists'.
        """
        if not self.driver:
            raise Exception("WebDriver is not initialized. Call setup_driver() first.")
        
        tracks = []
        try:
            logger.info("Locating track elements on the page.")
            track_elements = self.driver.find_elements(By.XPATH, "//div[@data-testid='tracklist-row']")
            logger.info(f"Found {len(track_elements)} tracks in the playlist.")

            for index, track in enumerate(track_elements, start=1):
                try:
                    # Extract song name
                    song_name_element = track.find_element(
                        By.XPATH, ".//div[contains(@class, 'encore-text-body-medium')]"
                    )
                    song_name = song_name_element.text.strip()

                    # Extract artists
                    artist_elements = track.find_elements(
                        By.XPATH, ".//span[contains(@class, 'encore-text-body-small')]//a"
                    )
                    artists = ", ".join(artist.text.strip() for artist in artist_elements)

                    # Append to the tracks list
                    tracks.append({"song_name": song_name, "artists": artists})
                except Exception as e:
                    logger.warning(f"Failed to extract information for track {index}: {e}")
                    continue
        except Exception as e:
            logger.error(f"Error during track extraction: {e}")
            self.close_driver()
            raise

        return tracks

    def close_driver(self):
        """Closes the Selenium WebDriver."""
        if self.driver:
            logger.info("Closing the WebDriver.")
            self.driver.quit()
            self.driver = None
        else:
            logger.warning("WebDriver is already closed.")

    def scrape_playlist(self):
        """
        Executes the full scraping workflow.

        Returns:
            list of dict: Extracted track information.
        """
        try:
            self.setup_driver()
            self.load_playlist()
            self.adjust_zoom()
            tracks = self.extract_tracks()
            return tracks
        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")
            return []
        finally:
            self.close_driver()

# ---------------------------- Usage Example ----------------------------

if __name__ == "__main__":
    # Replace with your specific Spotify playlist URL
    playlist_url = 'https://open.spotify.com/playlist/7oVjzQJsQg7b0gS1qmQjcV?si=099ed217390f426b'

    # Initialize the scraper with the desired URL
    scraper = SpotifyPlaylistScraper(playlist_url=playlist_url)

    # Perform the scraping
    extracted_tracks = scraper.scrape_playlist()

    # Display the extracted tracks
    for track in extracted_tracks:
        print(f"Song: {track['song_name']}, Artists: {track['artists']}")