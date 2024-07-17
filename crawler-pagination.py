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

def scrape_search_results(keyword, location, page_number, retries=3):
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

                search_data = {
                    "name": title,
                    "sponsored": sponsored,
                    "stars": rating,
                    "rank": rank,
                    "review_count": review_count,
                    "url": yelp_url
                }
                
                print(search_data)
                                
            logger.info(f"Successfully parsed data from: {url}")            
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_scrape(keyword, pages, location, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, page, retries=retries)


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

        start_scrape(keyword, PAGES, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")
