import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchData:
    name: str = ""
    sponsored: bool = False
    stars: float = 0
    rank: int = 0
    review_count: str = ""
    url: str = ""

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()


def scrape_search_results(keyword, location, page_number, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    #possibly need to urlencode location: city + state + zip code
    url = f"https://www.yelp.com/search?find_desc={formatted_keyword}&find_loc={location}&start={page_number*10}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code == 200:
                success = True
            
            else:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
                ## Extract Data

            soup = BeautifulSoup(response.text, "html.parser")           
            div_cards = soup.select("div[data-testid='serp-ia-card']")

            for div_card in div_cards:
                card_text = div_card.text
                sponsored = card_text[0].isdigit() == False
                ranking = None

                img = div_card.find("img")
                title = img.get("alt")
                
                if not sponsored:
                    rank_string = card_text.replace(title, "").split(".")
                    ranking = int(rank_string[0])                

                has_rating = div_card.select_one("div span[data-font-weight='semibold']")
                rating = 0.0

                if len(has_rating.text) > 0:
                    if has_rating.text[0].isdigit():
                        rating = float(has_rating.text)

                review_count = 0
                
                if "review" in card_text:
                    review_count = card_text.split("(")[1].split(")")[0].split(" ")[0]                

                a_element = div_card.find("a")
                link = a_element.get("href")
                yelp_url = f"https://www.yelp.com{link}"

                search_data = SearchData(
                    name=title,
                    sponsored=sponsored,
                    stars=rating,
                    rank=ranking,
                    review_count=review_count,
                    url=yelp_url
                )
                data_pipeline.add_data(search_data)
                                
            logger.info(f"Successfully parsed data from: {url}")            
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keyword, pages, location, data_pipeline=None, max_threads=5, retries=3):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(
            scrape_search_results,
            [keyword] * pages,
            [location] * pages,
            range(pages),
            [data_pipeline] * pages,
            [retries] * pages
        )


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 2
    PAGES = 5
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["restaurants"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        start_scrape(keyword, PAGES, LOCATION, data_pipeline=crawl_pipeline, max_threads=MAX_THREADS, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")
