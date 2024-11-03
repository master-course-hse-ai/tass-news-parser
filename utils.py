import re
from datetime import datetime

def sanitize_text(text):
    if not isinstance(text, str):
        text = str(text)
    
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_date(date_str):
    try:
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None