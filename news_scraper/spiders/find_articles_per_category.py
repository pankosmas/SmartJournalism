import scrapy
from pymongo import MongoClient

class News247Spider(scrapy.Spider):
    name = 'articles_spider'

    def start_requests(self):
        # Σύνδεση με τη βάση δεδομένων MongoDB
        client = MongoClient('localhost', 27017)
        db = client['news_db']
        collection = db['news_sites']

        # Ανάκτηση των κατηγοριών για το news247.gr
        news_site = collection.find_one({"page_url": "https://www.news247.gr/"})
        categories = news_site.get('categories', [])

        # Δημιουργία αιτημάτων για κάθε κατηγορία
        for category in categories:
            url = category.get('url')
            if url:
                yield scrapy.Request(url=url, callback=self.parse_category)

    def parse_category(self, response):
        # Εδώ κάνεις scraping για τα άρθρα στη συγκεκριμένη κατηγορία
        articles = response.css('article.article')  # Επιλέγουμε όλα τα articles

        for article in articles:
            article_url = article.css('h3.post__title a::attr(href)').get()  # Ανάκτηση URL άρθρου
            if article_url:
                yield response.follow(article_url, self.parse_article)

    def parse_article(self, response):
        # Εδώ κάνεις scraping για τα στοιχεία του άρθρου
        title = response.css('h3.post__title a::text').get().strip()  # Ανάκτηση τίτλου άρθρου
        content = response.css('div.post__content').get()  # Ανάκτηση περιεχομένου άρθρου (μπορείς να το προσαρμόσεις αν χρειάζεται)

        # Εάν θέλεις να συμπεριλάβεις και την ημερομηνία
        date = response.css('div.post__category span.caption.s-font::text').get().strip()  # Ανάκτηση ημερομηνίας

        yield {
            'title': title,
            'content': content,
            'url': response.url,
            'date': date,  # Προσθήκη ημερομηνίας στο αποτέλεσμα
        }
