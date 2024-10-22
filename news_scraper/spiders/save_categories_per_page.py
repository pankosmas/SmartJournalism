import re
import scrapy
import json
from pymongo import MongoClient

class CategoriesSpider(scrapy.Spider):
    name = "categories_spider"

    def __init__(self, site_name=None, *args, **kwargs):
        super(CategoriesSpider, self).__init__(*args, **kwargs)
        # Load the configuration from the JSON file
        with open("configuration.json", "r") as f:
            self.configs = json.load(f)

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

        # Store the categories in MongoDB
        self.store_categories_in_mongo(categories_dict, config)
        # Update the local JSON file with the categories
        self.update_json(categories_dict, config)            
        self.log(f"Categories successfully stored for {site_name} in MongoDB and updated in JSON file.")
        print(f"Categories successfully stored for {site_name} in MongoDB and updated in JSON file.")


    def store_categories_in_mongo(self, categories_list, config):
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["news_db"]
        collection = db["news_sites"]

        # Filter categories based on the regex pattern
        regex_pattern = config["regex"]
        filtered_categories = [category for category in categories_list if re.match(regex_pattern, category["url"])]

        # Ensure that the document for the current site exists
        existing_data = collection.find_one({"page_url": config["start_url"]})

        # If the site doesn't exist, create a new document
        if not existing_data:
            collection.insert_one({
                "page_url": config["start_url"],
                "categories": filtered_categories
            })
            return  # Exit since we've created a new record

        # Get existing categories
        existing_categories = existing_data.get("categories", [])

        # Append new filtered categories if they are not already present
        for category in filtered_categories:
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

            # Filter categories based on regex
            regex_pattern = config["regex"]
            filtered_categories = [category for category in categories if re.match(regex_pattern, category["url"])]

            # Find the entry for the start URL and update the categories
            for entry in data:
                if entry["page_url"] == config["start_url"]:
                    existing_categories = entry.get("categories", [])
                    # Append new filtered categories to existing ones
                    for category in filtered_categories:
                        if category not in existing_categories:
                            existing_categories.append(category)
                    entry["categories"] = existing_categories
                    break
            else:
                # If not found, add a new entry
                data.append({"page_url": config["start_url"], "categories": filtered_categories})

            # Write the changes back to the file
            with open("page_urls.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

        except FileNotFoundError:
            self.log("The file page_urls.json was not found.")
        except json.JSONDecodeError:
            self.log("Error reading the JSON file.")
