import requests
from bs4 import BeautifulSoup
import time
import os
from typing import List, Dict, Optional, Tuple
import json
from datetime import datetime
import warnings
import pandas as pd
import random
from urllib.parse import urlparse
import logging

# Selenium imports (optional - only loaded if needed)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: Selenium not installed. Install with: pip install selenium")

# Hide FutureWarnings (as shown in the article)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FBRefMatchDownloader:
    """
    Enhanced FBRef match downloader with anti-blocking measures.
    Based on best practices from web scraping to avoid getting blocked.
    """
    
    def __init__(self, chromedriver_path: Optional[str] = None, use_selenium: bool = False,
                 min_delay: float = 3.0, max_delay: float = 7.0, batch_size: int = 5):
        """
        Initialize the downloader with anti-blocking measures.
        
        Parameters:
        -----------
        chromedriver_path : str, optional
            Path to chromedriver executable
        use_selenium : bool
            Force use of Selenium for all downloads
        min_delay : float
            Minimum delay between requests (seconds)
        max_delay : float
            Maximum delay between requests (seconds)
        batch_size : int
            Number of downloads before taking a longer break
        """
        # Rotate between different user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        
        self.use_selenium = use_selenium
        self.driver = None
        self.chromedriver_path = chromedriver_path
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.batch_size = batch_size
        self.request_count = 0
        self.session = None
        
        # Create a session for connection pooling
        self._create_session()
        
        # Setup Selenium if forced
        if self.use_selenium and SELENIUM_AVAILABLE:
            self._setup_selenium()
    
    def _create_session(self):
        """Create a requests session with retry strategy."""
        self.session = requests.Session()
        
        # Add retry strategy
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _get_headers(self):
        """Get randomized headers to avoid detection."""
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'TE': 'Trailers'
        }
        
        # Sometimes add referer to look more natural
        if random.random() > 0.5:
            headers['Referer'] = 'https://fbref.com/en/comps/9/Premier-League-Stats'
        
        return headers
    
    def _get_delay(self):
        """Get randomized delay to avoid pattern detection."""
        base_delay = random.uniform(self.min_delay, self.max_delay)
        
        # Add extra delay every batch_size requests
        if self.request_count > 0 and self.request_count % self.batch_size == 0:
            extra_delay = random.uniform(10, 20)
            logger.info(f"Taking extended break: {extra_delay:.1f}s after {self.request_count} requests")
            return base_delay + extra_delay
        
        return base_delay
    
    def _setup_selenium(self):
        """Setup Selenium driver with anti-detection measures."""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium is not installed. Install with: pip install selenium")
        
        chrome_options = Options()
        
        # Anti-detection measures
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Standard options
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"--user-agent={random.choice(self.user_agents)}")
        
        if self.chromedriver_path:
            service = ChromeService(executable_path=self.chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(options=chrome_options)
        
        # Execute script to remove webdriver property
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    def _check_if_blocked(self, html_content: str) -> bool:
        """Check if we've been blocked or rate limited."""
        if not html_content:
            return True
        
        blocked_indicators = [
            'Access Denied',
            'Error 403',
            'Forbidden',
            'Too Many Requests',
            'rate limit',
            'blocked',
            'captcha',
            'cloudflare'
        ]
        
        html_lower = html_content.lower()
        return any(indicator.lower() in html_lower for indicator in blocked_indicators)
    
    def _handle_blocking(self):
        """Handle blocking by waiting longer and resetting session."""
        wait_time = random.uniform(30, 60)
        logger.warning(f"Possible blocking detected. Waiting {wait_time:.1f}s before continuing...")
        time.sleep(wait_time)
        
        # Reset session
        self._create_session()
        
        # Reset Selenium if using it
        if self.driver:
            self.driver.quit()
            self.driver = None
            if self.use_selenium:
                self._setup_selenium()
    
    def _download_with_beautifulsoup(self, url: str) -> Tuple[Optional[str], int]:
        """Download using BeautifulSoup with anti-blocking measures."""
        try:
            headers = self._get_headers()
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # Check if we're blocked
                if self._check_if_blocked(response.text):
                    logger.warning("Blocking detected in response content")
                    return None, 429
                return response.text, response.status_code
            else:
                return None, response.status_code
                
        except requests.exceptions.Timeout:
            logger.error("Request timed out")
            return None, -1
        except Exception as e:
            logger.error(f"BeautifulSoup download error: {str(e)}")
            return None, -1
    
    def _download_with_selenium(self, url: str) -> Optional[str]:
        """Download using Selenium with anti-detection measures."""
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium not available")
            return None
        
        if self.driver is None:
            self._setup_selenium()
        
        try:
            logger.info("Using Selenium for download...")
            
            # Random delay before navigation
            time.sleep(random.uniform(0.5, 1.5))
            
            self.driver.get(url)
            
            # Random mouse movements and scrolls to appear human-like
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(random.uniform(1, 2))
            self.driver.execute_script("window.scrollTo(0, 0);")
            
            # Wait for content
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            # Additional wait
            time.sleep(random.uniform(2, 3))
            
            html_content = self.driver.page_source
            
            # Check if blocked
            if self._check_if_blocked(html_content):
                logger.warning("Blocking detected in Selenium response")
                return None
            
            return html_content
            
        except Exception as e:
            logger.error(f"Selenium download error: {str(e)}")
            return None
    
    def _check_if_page_needs_selenium(self, html_content: str, match_id: str) -> bool:
        """
        Check if the page has dynamically loaded content that requires Selenium.
        Based on the article's approach of checking for specific tables.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for common dynamically loaded elements on FBRef match pages
        # Look for match data tables that might be loaded via JavaScript
        tables_to_check = ['summary', 'keeper', 'shots_all', 'passing', 'passing_types']
        
        for table_id in tables_to_check:
            table = soup.find('table', {'id': table_id})
            if table is None:
                # Table might be dynamically loaded
                return True
        
        # Check if main content div is empty or minimal
        content_div = soup.find('div', {'id': 'content'})
        if content_div and len(content_div.text.strip()) < 100:
            return True
        
        return False
    
    def download_match_html(self, match_id: str, output_dir: str = "match_html_files", 
                          extract_tables: bool = False, retry_on_block: bool = True) -> Dict[str, any]:
        """
        Download HTML content with enhanced anti-blocking measures.
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Increment request counter
        self.request_count += 1
        
        # Construct URL
        url = f"https://fbref.com/en/matches/{match_id}"
        
        # Get delay and wait
        delay = self._get_delay()
        logger.info(f"Downloading match {match_id} (request #{self.request_count}, waiting {delay:.1f}s)...")
        time.sleep(delay)
        
        # Try download
        html_content = None
        download_method = "selenium" if self.use_selenium else "beautifulsoup"
        retry_count = 0
        max_retries = 2 if retry_on_block else 0
        
        while retry_count <= max_retries:
            if not self.use_selenium:
                html_content, status_code = self._download_with_beautifulsoup(url)
                
                if status_code == 404:
                    logger.warning(f"Match {match_id} not found (404)")
                    return {
                        'status': 'not_found',
                        'match_id': match_id,
                        'url': url,
                        'error': 'Match not found (404)'
                    }
                elif status_code == 429 or status_code == 403:
                    logger.warning(f"Rate limited or blocked (HTTP {status_code})")
                    if retry_count < max_retries:
                        self._handle_blocking()
                        retry_count += 1
                        continue
                elif status_code != 200:
                    logger.warning(f"HTTP {status_code}, trying Selenium")
                    html_content = self._download_with_selenium(url)
                    if html_content:
                        download_method = "selenium_fallback"
                elif html_content and self._check_if_page_needs_selenium(html_content, match_id):
                    logger.info("Detected dynamic content, switching to Selenium...")
                    html_content = self._download_with_selenium(url)
                    download_method = "selenium_dynamic"
            else:
                html_content = self._download_with_selenium(url)
            
            # If we got content and it's not blocked, break the retry loop
            if html_content and not self._check_if_blocked(html_content):
                break
            elif html_content and self._check_if_blocked(html_content):
                if retry_count < max_retries:
                    self._handle_blocking()
                    retry_count += 1
                else:
                    break
            else:
                break
        
        if not html_content:
            return {
                'status': 'error',
                'match_id': match_id,
                'url': url,
                'error': 'Failed to download content (possibly blocked)'
            }
        
        # Save HTML file
        filename = f"match_{match_id}.html"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✓ Successfully saved: {filename} (method: {download_method})")
        
        result = {
            'status': 'success',
            'match_id': match_id,
            'url': url,
            'filename': filename,
            'filepath': filepath,
            'download_method': download_method,
            'retry_count': retry_count
        }
        
        # Extract tables if requested
        if extract_tables:
            tables_extracted = self._extract_tables(html_content, match_id, output_dir)
            result['tables_extracted'] = tables_extracted
        
        return result
    
    def download_multiple_matches(self, match_ids: List[str], output_dir: str = "match_html_files", 
                                extract_tables: bool = False, save_summary: bool = True,
                                resume_from: Optional[str] = None) -> List[Dict[str, any]]:
        """
        Download multiple matches with resume capability.
        """
        results = []
        total = len(match_ids)
        
        # Handle resume
        start_index = 0
        if resume_from and resume_from in match_ids:
            start_index = match_ids.index(resume_from)
            logger.info(f"Resuming from match {resume_from} (index {start_index})")
        
        print(f"Starting download of {total - start_index} matches...")
        print(f"Files will be saved to: {os.path.abspath(output_dir)}/")
        print(f"Delay range: {self.min_delay}-{self.max_delay}s between requests")
        print(f"Extended break every {self.batch_size} requests")
        print("-" * 50)
        
        for i, match_id in enumerate(match_ids[start_index:], start_index + 1):
            print(f"\n[{i}/{total}] ", end="")
            result = self.download_match_html(match_id, output_dir, extract_tables)
            results.append(result)
            
            # Save intermediate results periodically
            if i % 10 == 0 and save_summary:
                self._save_intermediate_summary(results, output_dir, total)
        
        # Final summary
        successful = sum(1 for r in results if r['status'] == 'success')
        not_found = sum(1 for r in results if r['status'] == 'not_found')
        errors = sum(1 for r in results if r['status'] in ['error', 'exception'])
        
        print("\n" + "=" * 50)
        print(f"Download Summary:")
        print(f"  ✓ Successful: {successful}")
        print(f"  ✗ Not found: {not_found}")
        print(f"  ✗ Errors: {errors}")
        print(f"  Total: {len(results)}")
        
        if save_summary:
            self._save_final_summary(results, output_dir, total)
        
        return results
    
    def _save_intermediate_summary(self, results: List[Dict], output_dir: str, total: int):
        """Save intermediate results in case of interruption."""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_planned': total,
            'completed': len(results),
            'last_match_id': results[-1]['match_id'] if results else None,
            'results': results
        }
        
        summary_path = os.path.join(output_dir, 'download_summary_intermediate.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def _save_final_summary(self, results: List[Dict], output_dir: str, total: int):
        """Save final summary."""
        successful = sum(1 for r in results if r['status'] == 'success')
        not_found = sum(1 for r in results if r['status'] == 'not_found')
        errors = sum(1 for r in results if r['status'] in ['error', 'exception'])
        
        methods_used = {}
        for r in results:
            if 'download_method' in r:
                method = r['download_method']
                methods_used[method] = methods_used.get(method, 0) + 1
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_matches': total,
            'completed': len(results),
            'successful': successful,
            'not_found': not_found,
            'errors': errors,
            'methods_used': methods_used,
            'results': results
        }
        
        summary_path = os.path.join(output_dir, 'download_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nSummary saved to: {summary_path}")
    
    def _extract_tables(self, html_content: str, match_id: str, output_dir: str) -> List[str]:
        """Extract tables from HTML and save as CSV files."""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables_dir = os.path.join(output_dir, 'tables', match_id)
        os.makedirs(tables_dir, exist_ok=True)
        
        tables_extracted = []
        tables = soup.find_all('table')
        
        for table in tables:
            table_id = table.get('id', 'unnamed')
            if table_id and table_id != 'unnamed':
                try:
                    df = pd.read_html(str(table))[0]
                    csv_filename = f"{match_id}_{table_id}.csv"
                    csv_path = os.path.join(tables_dir, csv_filename)
                    df.to_csv(csv_path, index=False)
                    tables_extracted.append(table_id)
                except Exception as e:
                    logger.error(f"Failed to extract table {table_id}: {str(e)}")
        
        return tables_extracted
    
    def __del__(self):
        """Clean up resources."""
        if self.driver:
            self.driver.quit()
        if self.session:
            self.session.close()


# ============= CONVENIENCE FUNCTIONS FOR JUPYTER =============

def quick_download(match_ids: List[str], use_selenium: bool = False, extract_tables: bool = False):
    """
    Simple wrapper function for easy use in Jupyter notebooks.
    
    Parameters:
    -----------
    match_ids : List[str]
        List of match IDs to download
    use_selenium : bool
        Force use of Selenium for all downloads
    extract_tables : bool
        Extract and save tables as CSV files
    
    Example:
    --------
    match_ids = ['83edc9ff', 'd0426a07']
    quick_download(match_ids, extract_tables=True)
    """
    downloader = FBRefMatchDownloader(use_selenium=use_selenium)
    return downloader.download_multiple_matches(match_ids, extract_tables=extract_tables)


def download_with_custom_settings(match_ids: List[str], chromedriver_path: Optional[str] = None, 
                                output_dir: str = "match_html_files", delay: float = 2.0, 
                                extract_tables: bool = False):
    """
    Download with custom settings including chromedriver path.
    
    Example:
    --------
    download_with_custom_settings(
        match_ids=['83edc9ff'],
        chromedriver_path=r"C:\path\to\chromedriver.exe",
        extract_tables=True,
        delay=3.0
    )
    """
    downloader = FBRefMatchDownloader(chromedriver_path=chromedriver_path)
    return downloader.download_multiple_matches(match_ids, output_dir, delay, extract_tables)


def resume_download(match_ids: List[str], resume_from: str, **kwargs):
    """
    Resume a download from a specific match ID.
    
    Example:
    --------
    # If download was interrupted at match 'abc123'
    resume_download(match_ids, resume_from='abc123')
    """
    downloader = FBRefMatchDownloader(**kwargs)
    return downloader.download_multiple_matches(match_ids, resume_from=resume_from)


# ============= EXAMPLE USAGE =============
if __name__ == "__main__":
    # Example 1: Simple download with smart method selection
    match_ids = ['83edc9ff', 'd0426a07']
    results = quick_download(match_ids)
    
    # Example 2: Force Selenium and extract tables
    # downloader = FBRefMatchDownloader(use_selenium=True)
    # results = downloader.download_multiple_matches(
    #     match_ids=['83edc9ff'],
    #     extract_tables=True
    # )
    
    # Example 3: With custom chromedriver path
    # downloader = FBRefMatchDownloader(
    #     chromedriver_path=r"C:\Users\henri\Documents\Chromedriver\chromedriver.exe"
    # )
    # results = downloader.download_multiple_matches(match_ids)