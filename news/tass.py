import requests
import time
import csv
import os
import json
from datetime import datetime, timedelta
from utils import sanitize_text, parse_date
import random

class TassScraper:
    def __init__(self, state_file='scraper_state.json', output_file='tass_news.csv', max_retries=3):
        self.base_url = "https://tass.ru/tbp/api/v1/content"
        self.rubrics = [
            "v-strane", "politika", "nacionalnye-proekty", "mezhdunarodnaya-panorama",
            "ekonomika", "nedvizhimost", "msp", "armiya-i-opk"
        ]
        self.state_file = state_file
        self.output_file = output_file
        self.existing_news = self.load_existing_news()
        self.state = self.load_state()
        print("Initialized with state:", self.state)
        self.max_retries = max_retries
        self.current_rubric = self.state.get("rubric", self.rubrics[0])

    def load_existing_news(self):
        existing_news = set()
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_news.add(row['id'])
        return existing_news

    def save_news(self, news):
        file_exists = os.path.exists(self.output_file)
        
        with open(self.output_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'id', 'meta_description', 'meta_title', 'lead', 'es_updated_dt', 
                'updated_dt', 'published_dt', 'publish_updated_dt', 'url', 'rubric'
            ], quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            if not file_exists:
                writer.writeheader() 

            for item in news:
                sanitized_item = {k: sanitize_text(v if v is not None else '') for k, v in item.items()}
                writer.writerow(sanitized_item)

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                return json.load(f)
        print("No valid state found, going default")
        return {'last_es_updated_dt': None, 'rubric': self.rubrics[0], 'last_run': None}

    def save_state(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)

    def get_news(self):
        END_DATE = datetime(2010, 1, 1)

        try:
            rubric_index = self.rubrics.index(self.current_rubric) 
            for rubric in self.rubrics[rubric_index:]:
                print(f"Scraping rubric: {rubric}")
                self.state['rubric'] = rubric  

                last_es_updated_dt = self.state.get('last_es_updated_dt', datetime.now().isoformat())
                print('Requesting with', last_es_updated_dt)

                should_move_to_next_rubric = False

                while True:
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            params = {
                                'limit': 20,
                                'lang': 'ru',
                                'rubrics': f"/{rubric}",
                                'sort': '-es_updated_dt'
                            }

                            if last_es_updated_dt:
                                params['last_es_updated_dt'] = last_es_updated_dt

                            response = requests.get(self.base_url, params=params)
                            response.raise_for_status()
                            data = response.json()
                            news_items = data.get('result', [])

                            if not news_items:
                                print(f"No more news items available in rubric: {rubric}. Moving to the next rubric.")
                                should_move_to_next_rubric = True
                                break

                            current_dt = datetime.fromisoformat(last_es_updated_dt)
                            if current_dt < END_DATE:
                                print(f"Reached END_DATE for rubric: {rubric}. Moving to the next rubric.")
                                should_move_to_next_rubric = True
                                break 

                            news_data = []
                            for item in news_items:
                                news_entry = {
                                    'id': str(item['id']),
                                    'meta_description': item.get('meta_description', ''),
                                    'meta_title': item.get('meta_title', ''),
                                    'lead': item.get('lead', ''),
                                    'es_updated_dt': item.get('es_updated_dt', ''),
                                    'updated_dt': item.get('updated_dt', ''),
                                    'published_dt': item.get('published_dt', ''),
                                    'publish_updated_dt': item.get('publish_updated_dt', ''),
                                    'url': item.get('url', ''),
                                    'rubric': rubric
                                }

                                if news_entry['id'] not in self.existing_news:
                                    news_data.append(news_entry)
                                    self.existing_news.add(news_entry['id'])

                            self.save_news(news_data)

                            random_hours = random.randint(1, 6)
                            last_es_updated_dt = (current_dt - timedelta(hours=random_hours)).isoformat()

                            self.state['last_es_updated_dt'] = last_es_updated_dt
                            self.save_state()

                            retry_count = 0
                            break

                        except requests.exceptions.RequestException as e:
                            retry_count += 1
                            print(f"Error fetching news (attempt {retry_count}/{self.max_retries}): {e}")

                            if retry_count >= self.max_retries:
                                print("Max retries reached. Saving state and stopping.")
                                self.save_state()
                                return

                            time.sleep(5 * retry_count)

                    if should_move_to_next_rubric:
                        break

                print(f"Completed scraping rubric: {rubric}. Moving to the next rubric.")
                self.state['last_es_updated_dt'] = None
                self.state['rubric'] = self.rubrics[(self.rubrics.index(rubric) + 1) % len(self.rubrics)]
                self.save_state()

        except KeyboardInterrupt:
            print("\nInterrupted. Saving state and stopping.")
            self.save_state()