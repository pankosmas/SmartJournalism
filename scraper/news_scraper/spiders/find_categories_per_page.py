import scrapy
import json
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

            # Προσθήκη κατηγοριών στο υπάρχον JSON αρχείο
            self.update_json(categories_dict)

        else:
            self.log("Δεν βρέθηκαν έγκυρες κατηγορίες.")

        self.log("Οι κατηγορίες αποθηκεύτηκαν επιτυχώς στη MongoDB.")

    def update_json(self, categories):
        # Ανάγνωση του υπάρχοντος JSON αρχείου
        try:
            with open("page_urls.json", "r", encoding="utf-8") as json_file:
                data = json.load(json_file)

            # Εύρεση του URL του news247
            for entry in data:
                if entry["page_url"] == "https://www.news247.gr/":
                    entry["categories"] = categories
                    break

            # Αποθήκευση των αλλαγών στο ίδιο αρχείο
            with open("page_urls.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            self.log("Οι κατηγορίες προστέθηκαν επιτυχώς στο page_urls.json.")

        except FileNotFoundError:
            self.log("Το αρχείο page_urls.json δεν βρέθηκε.")
        except json.JSONDecodeError:
            self.log("Σφάλμα κατά την ανάγνωση του JSON αρχείου.")
