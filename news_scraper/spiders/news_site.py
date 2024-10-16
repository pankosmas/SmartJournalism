import scrapy


class NewsSiteSpider(scrapy.Spider):
    name = "news_site"
    allowed_domains = ["www.news247.gr"]
    start_urls = ["https://www.news247.gr/"]

    def parse(self, response):
        # Εξαγωγή των κατηγοριών και των URLs τους
        category_links = response.css('li.menu-item a::attr(href)').getall()
        
        # Πλοήγηση στα URLs των κατηγοριών
        for link in category_links:
            yield response.follow(link, callback=self.parse_category)

    def parse_category(self, response):
        # Εξαγωγή των πρώτων 10 άρθρων από τη σελίδα της κατηγορίας
        articles = response.css('article.article')[:10]

        for article in articles:
            # Εξαγωγή URL άρθρου
            url = article.css('h3.post__title a::attr(href)').get()

            # Αποθήκευση του URL
            yield {
                'url': response.urljoin(url)
            }



