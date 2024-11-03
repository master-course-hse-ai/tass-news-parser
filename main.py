from news.tass import TassScraper

def main():
    scraper = TassScraper(
        state_file='state.json', 
        output_file='tass_news.csv',
        max_retries=15
        )
    
    print("Starting TASS news scraping...")
    scraper.get_news()
    print("TASS news scraping completed.")

if __name__ == "__main__":
    main()
