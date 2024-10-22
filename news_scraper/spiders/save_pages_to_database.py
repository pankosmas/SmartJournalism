import pymongo
import json

# Σύνδεση με τη MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Ανάλογα με τις ρυθμίσεις σου
db = client["news_db"]  # Το όνομα της βάσης δεδομένων
collection = db["news_sites"]  # Το όνομα της συλλογής

def save_page_urls(urls):
    data_to_save = []  # Λίστα για να αποθηκεύσουμε τα δεδομένα για JSON
    for url in urls:
        # Εισαγωγή του URL στη βάση
        collection.update_one(
            {"page_url": url},
            {"$set": {"page_url": url, "categories": []}},  # Δημιουργία κενής λίστας κατηγοριών αρχικά
            upsert=True
        )
        # Προσθήκη του URL στα δεδομένα που θα αποθηκευτούν
        data_to_save.append({"page_url": url, "categories": []})

    # Αποθήκευση των δεδομένων σε JSON αρχείο
    with open("page_urls.json", "w", encoding="utf-8") as json_file:
        json.dump(data_to_save, json_file, ensure_ascii=False, indent=4)

# Ενδεικτική λίστα με τα URLs των σελίδων
page_urls = [
    "https://www.news247.gr/",
    "https://www.news.gr/",
    "https://www.documentonews.gr/",
    "https://www.tanea.gr/",
    "https://www.tovima.gr/",
    "https://www.iefimerida.gr/",
    "https://www.kathimerini.gr/",
    "https://www.newsit.gr/",
    "https://www.newsbeast.gr/",
    "https://www.cnn.gr/",
    "https://www.protothema.gr/",
    "https://www.ethnos.gr/",
    "https://www.ertnews.gr/"
]

# Αποθήκευση των URLs των σελίδων στη βάση και σε JSON αρχείο
save_page_urls(page_urls)

print("Τα URLs των σελίδων αποθηκεύτηκαν επιτυχώς στη MongoDB και στο JSON αρχείο.")
