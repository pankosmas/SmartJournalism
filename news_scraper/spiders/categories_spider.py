import scrapy
from pymongo import MongoClient

class CategoriesSpider(scrapy.Spider):
    name = "categories_spider"
    start_urls = ["https://www.news247.gr/"]

    def parse(self, response):
        # Εύρεση κατηγοριών από τη σελίδα
        categories = response.css("li.menu-item a::attr(href)").getall()
        category_names = response.css("li.menu-item a::text").getall()

        # Χρήση set για να αποφευχθούν τα διπλότυπα
        unique_categories = set()
        categories_dict = []

        # Φιλτράρισμα κατηγοριών και καθαρισμός
        for name, url in zip(category_names, categories):
            if url and name and url.startswith("https://www.news247.gr/"):
                # Προσθήκη στη λίστα μόνο αν δεν υπάρχει ήδη
                if url not in unique_categories:
                    unique_categories.add(url)
                    categories_dict.append({"name": name.strip(), "url": url.strip()})

        # Σύνδεση με MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["news_db"]
        collection = db["news_sites"]

        # Ενημέρωση του εγγράφου στη βάση με τις κατηγορίες
        if categories_dict:
            # Διαγραφή μη έγκυρων κατηγοριών από το υπάρχον έγγραφο
            collection.update_one(
                {"page_url": "https://www.news247.gr/"},
                {"$pull": {"categories": {"url": {"$not": {"$regex": "^https://www\\.news247\\.gr/[^/]+/?$"}}}}}
            )

            # Προσθήκη των νέων κατηγοριών
            collection.update_one(
                {"page_url": "https://www.news247.gr/"},
                {"$set": {"categories": categories_dict}},
                upsert=True
            )
        else:
            self.log("Δεν βρέθηκαν έγκυρες κατηγορίες.")

        self.log("Οι κατηγορίες αποθηκεύτηκαν επιτυχώς στη MongoDB.")
