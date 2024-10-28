import re
import time
import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from scrapy_splash import SplashRequest
from scrapy.exceptions import CloseSpider
from w3lib.html import remove_tags

class CategoriesSpider(scrapy.Spider):
    name = "categories_spider"

    def __init__(self, site_name=None, option=1, target_date=None, *args, **kwargs):
        super(CategoriesSpider, self).__init__(*args, **kwargs)
        self.option = int(option)
        self.target_timestamp = self.convert_date_to_timestamp(target_date) if target_date else None
        # Load the configuration from the JSON file
        with open("configuration.json", "r") as f:
            self.configs = json.load(f)
        
        # Track the start time
        self.start_time = time.time()
        self.last_call_timestamp = self.get_last_call_timestamp()

    def convert_date_to_timestamp(self, date_string):
        """Convert date string to Unix timestamp."""
        try:
            # Adjust the format string based on the expected date format
            date_obj = datetime.strptime(date_string, "%Y-%m-%d")  # Change format as needed
            return int(date_obj.timestamp())
        except ValueError:
            self.log(f"Invalid date format: {date_string}. Expected format: YYYY-MM-DD.")
            return None
        
    def get_last_call_timestamp(self):
        # Get the last call timestamp from the JSON file
        try:
            with open('last_call_info.json') as json_file:
                data = json.load(json_file)
                return data.get("timestamp", 0)  # Default to 0 if not found
        except FileNotFoundError:
            return 0  # If file not found, return 0
        
    def closed(self, reason):
        # Calculate the elapsed time
        elapsed_time = time.time() - self.start_time
        self.log(f"Spider closed: {reason}. Elapsed time: {elapsed_time:.2f} seconds.")
        print(f"Spider closed: {reason}. Elapsed time: {elapsed_time:.2f} seconds.")

    def start_requests(self):
        # Loop through each site in the configuration
        for site_name, config in self.configs.items():
            start_url = config["start_url"]
            yield scrapy.Request(url=start_url, callback=self.parse_categories, meta={'site_name': site_name})

    def parse_categories(self, response):
        site_name = response.meta['site_name']
        config = self.configs[site_name]
        url_concat = config['url_concat']
        # Extract category URLs and names using the CSS selectors from the config
        category_urls = response.css(config["category"]).getall()
        category_names = response.css(config["category_name"]).getall()
        # Use a set to avoid duplicates
        unique_categories = set()
        categories_dict = []
        # Filter categories and clean the data
        if url_concat:
            for name, url in zip(category_names, category_urls):
                if url and name:
                    url = config['start_url'] + url[1:]
                    if url not in unique_categories:
                        unique_categories.add(url)
                        categories_dict.append({"name": name.strip(), "url": url.strip(), "articles": []})
        else:
            for name, url in zip(category_names, category_urls):
                if url and name and url.startswith(config["start_url"]):
                    if url not in unique_categories:
                        unique_categories.add(url)
                        categories_dict.append({"name": name.strip(), "url": url.strip(), "articles": []})

        
        # Filter categories based on the regex pattern
        regex_pattern = config["regex"]
        filtered_categories = [category for category in categories_dict if re.match(regex_pattern, category["url"])]

        # Store the categories in MongoDB and Update the local JSON file with the categories
        self.store_categories_in_mongo(filtered_categories, config)
        self.update_json(filtered_categories, config)            
        self.log(f"Categories successfully stored for {site_name} in MongoDB and updated in JSON file.")
        print(f"Categories successfully stored for {site_name} in MongoDB and updated in JSON file.")
        # Now scrape articles for each category
        print("scraping news from ... " + site_name + f" for {len(filtered_categories)} categories " + str([catname['name'] for catname in filtered_categories]))
        for category in filtered_categories:
            yield scrapy.Request(url=category["url"], callback=self.parse_articles, meta={'site_name': site_name, 'category': category})


    def parse_articles(self, response):
        site_name = response.meta['site_name']
        category = response.meta['category']
        config = self.configs[site_name]

        #articles_container = response.css(config["articles_container"])
        articles_container = response.css(config["articles_container"])
        articles = articles_container.css(config["article"])
        
        def returnTimestamp(date, site_name):
            if site_name == "news247": return int((datetime.strptime(date.strip(), "%d.%m.%Y, %H:%M")).timestamp())
            elif site_name == "news": return int((datetime.strptime(date.strip(), "%d/%m/%Y %H:%M")).timestamp())
            elif site_name == "documento": return int((datetime.strptime(date.strip(), "%d/%m/%Y, %H:%M")).timestamp())
            elif site_name == "tanea": return int((datetime.strptime((date).strip(), "%d/%m/%Y %H:%M")).timestamp())   
            else: return date

        for article in articles:
            title = article.css(config["title_selector"]).get()
            article_url = article.css(config["article_url"]).get()
            image_url = article.css(config["image_url"]).get()
            date = article.css(config["date_selector"]).get()

            if site_name == 'news':
                date = date + " " + article.css(config["time_selector"]).get()
            
            # Save the article data
            if title and article_url:  # Ensure title and URL are present
                timestamp = returnTimestamp(date, site_name)
                article_data = {
                    "title": title.strip(),
                    "url": response.urljoin(article_url.strip()),
                    "image_url": image_url.strip() if image_url else None,
                    "date": date.strip() if date else None,
                    "timestamp": timestamp
                }

                # Make a request to the article's URL to scrape its content
                yield scrapy.Request(
                    url=article_data["url"],
                    callback=self.parse_content,
                    meta={'site_name': site_name, 'category': category, 'article_data': article_data}
                )
                
                # Check timestamp condition for Option 1
                #if self.option == 1 and article_data["timestamp"] <= self.last_call_timestamp:
                #    continue  # Skip this article if the timestamp doesn't meet the criteria                
        
        # Handle pagination for Option 2
        if self.option == 2:
            next_page = response.css(config['next_page']).get()  
            if next_page:
                yield response.follow(next_page, self.parse_articles, meta={'site_name': site_name, 'category': category})


    def parse_content(self, response):
        site_name = response.meta['site_name']
        category = response.meta['category']
        article_data = response.meta['article_data']
        config = self.configs[site_name]

        # Extract the content of the article
        summary = ' '.join(response.css(config['summary']).getall())
        text = ' '.join(response.css(config['text']).getall())
        author = response.css(config['author']).get()
        tags = response.css(config['tags']).getall()
        tags = [tag.strip() for tag in tags]
        
        # Update the article data with the extracted content
        article_data["summary"] = remove_tags(summary).strip() if summary else None
        article_data["text"] = remove_tags(text).strip() if text else None
        article_data["author"] = author.strip() if author else None
        article_data["tags"] = tags if tags else None
        
        # Add the article to the category's articles list
        category["articles"].append(article_data)
        # Store articles in MongoDB
        self.store_articles_in_mongo(site_name, category)
        self.update_last_call()

         # Check if the timestamp is less than the target_timestamp
        if category["articles"][-1]["timestamp"] < self.target_timestamp:
            print(article_data['url'])
            self.log(f"Found article with timestamp {article_data['timestamp']}, which is less than target {self.target_timestamp}.")
            self.log("Stopping spider after processing current articles.")
            self.crawler.engine.close_spider(self, "Found article with timestamp lower than target.")      


    def store_articles_in_mongo(self, site_name, category):
        client = MongoClient("mongodb://localhost:27017/")
        db = client["news_db"]
        collection = db["news_sites"]
        # Update the category with the articles
        collection.update_one(
            {"page_url": self.configs[site_name]["start_url"], "categories.name": category["name"]},
            {"$set": {"categories.$.articles": category["articles"]}}
        )
        
            
    def store_categories_in_mongo(self, categories_list, config):
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["news_db"]
        collection = db["news_sites"]
        # Ensure that the document for the current site exists
        existing_data = collection.find_one({"page_url": config["start_url"]})
        # If the site doesn't exist, create a new document
        if not existing_data:
            collection.insert_one({
                "page_url": config["start_url"],
                "categories": categories_list
            })
            return  # Exit since we've created a new record
        
        # Get existing categories
        existing_categories = existing_data.get("categories", [])
        # Append new filtered categories if they are not already present
        for category in categories_list:
            if category not in existing_categories:
                existing_categories.append(category)

        # Update the MongoDB document with the new categories if there's a change
        collection.update_one(
            {"page_url": config["start_url"]},
            {"$set": {"categories": existing_categories}}
        )


    def update_json(self, categories, config):
        # Read the existing JSON file
        try:
            with open("page_urls.json", "r", encoding="utf-8") as json_file:
                data = json.load(json_file)

            # Find the entry for the start URL and update the categories
            for entry in data:
                if entry["page_url"] == config["start_url"]:
                    existing_categories = entry.get("categories", [])
                    # Append new filtered categories to existing ones
                    for category in categories:
                        if category not in existing_categories:
                            existing_categories.append(category)
                    entry["categories"] = existing_categories
                    break
            else:
                # If not found, add a new entry
                data.append({"page_url": config["start_url"], "categories": categories})

            # Write the changes back to the file
            with open("page_urls.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

        except FileNotFoundError:
            self.log("The file page_urls.json was not found.")
        except json.JSONDecodeError:
            self.log("Error reading the JSON file.")

    
    def update_last_call(self):
        # Get the current timestamp
        current_timestamp = int(time.time())
        # Store the current timestamp to last_call_info.json
        with open('last_call_info.json', 'w') as json_file:
            json.dump({"timestamp": current_timestamp}, json_file)
