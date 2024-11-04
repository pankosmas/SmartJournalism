import re
import time
import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from scrapy_splash import SplashRequest
from scrapy.exceptions import CloseSpider
from w3lib.html import remove_tags

class Newspider(scrapy.Spider):
    name = "newspider"
    max_retries = 3

    # Load the configuration from the JSON file
    with open('configuration.json') as config_file:
        config = json.load(config_file)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.termination_counter = 1

    def start_requests(self):
        # Connect to MongoDB
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['news_database']

        # Iterate over each site in the configuration
        for site_name, site_config in self.config.items():
            yield scrapy.Request(url=site_config["start_url"], callback=self.parse_categories, meta={'site_name': site_name, 'site_config': site_config, 'retry_count': 0})

    def parse_categories(self, response):
        site_name = response.meta['site_name']
        site_config = response.meta['site_config']
        regex_pattern = re.compile(site_config["regex"])  # Compile the regex pattern

        categories = {}
        seen_names = set()  # To track seen category names
        seen_urls = set()   # To track seen category URLs

        # Extract categories using the selectors from the config
        for category_name, category_url in zip(response.css(site_config['category_name']).getall(), response.css(site_config['category']).getall()):
            # Check if the category URL matches the regex
            if regex_pattern.match(category_url):
                category_name = category_name.strip()
                category_url = category_url.strip()

                # Check for duplicates
                if category_name not in seen_names and category_url not in seen_urls:
                    categories[category_name] = category_url
                    seen_names.add(category_name)
                    seen_urls.add(category_url)

        # Scrape articles for each category
        for name, url in categories.items():
            yield scrapy.Request(url=url, callback=self.parse_articles, meta={'site_name': site_name, 'category_name': name, 'site_config': site_config, 'retry_count': 0})

    def parse_articles(self, response):
        site_name = response.meta['site_name']
        site_config = response.meta['site_config']
        category_name = response.meta['category_name']
        retry_count = response.meta['retry_count']
        
        articles = response.css(site_config['articles_container'])
        date_limit = datetime.strptime(site_config['date_limit'], "%Y-%m-%d")

        for article in articles.css(site_config['article']):
            title = article.css(site_config['title_selector']).get()
            article_url = article.css(site_config['article_url']).get()
            image_url = article.css(site_config['image_url']).get()
            date_str = article.css(site_config['date_selector']).get()
            try:
                article_date = datetime.strptime(date_str.strip(), "%d.%m.%Y, %H:%M")
                article_timestamp = int(article_date.timestamp())
            except ValueError:
                # If we get a ValueError, retry the request up to the max_retries limit
                if retry_count < self.max_retries:
                    retry_count += 1
                    self.log(f"Retrying {response.url} (Attempt {retry_count}/{self.max_retries}) due to invalid date: {article_date}")
                    yield response.follow(response.url, callback=self.parse_articles, meta={
                        'site_name': site_name, 
                        'category_name': category_name, 
                        'site_config': site_config, 
                        'retry_count': retry_count
                    })
                else:
                    self.log(f"Max retries reached for {response.url} due to repeated invalid dates like: {article_date}")
                return  # Exit after max retries to avoid infinite loops
            
            # Check if the article date is newer than the limit
            if article_date >= date_limit:
                yield scrapy.Request(url=article_url, callback=self.parse_article_content, meta={
                    'site_name': site_name,
                    'category_name': category_name,
                    'title': title.strip(),
                    'image_url': image_url.strip(),
                    'article_url': article_url.strip(),
                    'article_date': article_date,
                    'timestamp': article_timestamp,
                    'site_config': site_config
                })
            else:
                # If the article date is older, terminate the scraping for this category
                self.log(f"Terminating scraping for category '{category_name}' due to article date '{article_date}' being older than the limit.")
                print(f"{self.termination_counter}: Terminating scraping for category '{category_name}' due to article date '{article_date}' being older than the limit.")
                self.termination_counter += 1
                return  # Stop further processing of this category

        # Handle pagination if enabled
        if site_config.get('pagination', False):
            next_page = response.css(site_config['next_page']).get()
            if next_page:
                yield response.follow(next_page, callback=self.parse_articles, meta={'site_name': site_name, 'category_name': category_name, 'site_config': site_config, 'retry_count': 0})

    def parse_article_content(self, response):
        site_name = response.meta['site_name']
        category_name = response.meta['category_name']
        title = response.meta['title']
        image_url = response.meta['image_url']
        article_url = response.meta['article_url']
        article_date = response.meta['article_date']
        timestamp = response.meta['timestamp']
        site_config = response.meta['site_config']

        # Extract article content
        summary = ' '.join(response.css(site_config['summary']).getall())
        text = ' '.join(response.css(site_config['text']).getall())
        author = response.css(site_config['author']).get()
        tags = response.css(site_config['tags']).getall()

        # Prepare the article data
        article_data = {
            "category": category_name,
            "url": article_url,
            "image_url": image_url,            
            "title": title,
            "text": remove_tags(text),
            "summary": remove_tags(summary),         
            "author": author.strip() if author else None,
            "tags": [tag.strip() for tag in tags],
            "date": article_date,
            'timestamp': timestamp
        }
        
        # Add the article under the correct category's 'articles' array in MongoDB
        self.db[site_name].insert_one(article_data)
