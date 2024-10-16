import pymongo

# Σύνδεση με τη MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Ανάλογα με τις ρυθμίσεις σου
db = client["news_db"]  # Το όνομα της βάσης δεδομένων
collection = db["news_sites"]  # Το όνομα της συλλογής

def save_page_urls(urls):
    for url in urls:
        # Εισαγωγή του URL στη βάση
        collection.update_one(
            {"page_url": url},
            {"$set": {"page_url": url, "categories": []}},  # Δημιουργία κενής λίστας κατηγοριών αρχικά
            upsert=True
        )

# Ενδεικτική λίστα με τα URLs των σελίδων
page_urls = [
    "https://www.news247.gr/",
    "https://www.protothema.gr/",
    "https://www.kathimerini.gr/",
    "https://www.tanea.gr/",
    "https://www.ethnos.gr/",
    "https://www.iefimerida.gr/",
    "https://www.in.gr/",
    "https://www.ertnews.gr/",
    "https://www.newsit.gr/",
    "https://www.tovima.gr/"
]

# Αποθήκευση των URLs των σελίδων στη βάση
save_page_urls(page_urls)

print("Τα URLs των σελίδων αποθηκεύτηκαν επιτυχώς στη MongoDB.")
