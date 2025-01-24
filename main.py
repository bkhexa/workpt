import os
import requests
import json
import re
import base64
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import logging
from bs4 import BeautifulSoup
from newspaper import Article

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
api_key=""
base_url_entries = ""
base_url_news = ""

headers = {
    "Authorization": api_key,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None

def log_error(conn, error_message, error_type, related_item):
    try:
        conn.rollback()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO Logs (Log_Timestamp, Log_Level, Log_Message, Error_Type, Related_Item)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (datetime.now(), "ERROR", error_message, error_type, related_item)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to log error: {e}")

def fetch_and_clean_article(company_url):
    retries = 3
    for attempt in range(retries):
        try:
            article = Article(company_url)
            article.download()
            article.parse()

            title = article.title if article.title else "Article"
            article_html = article.html

            article_text = article.text
            soup = BeautifulSoup(article_html, 'html.parser')

            for tag in soup(['script', 'style', 'font', 'header', 'footer', 'aside', 'nav', 'advertisement']):
                tag.decompose()

            cleaned_text = ''
            for para in soup.find_all('p'):
                para_text = para.get_text().strip()
                if para_text:
                    cleaned_text += para_text + '\n'

            final_text = cleaned_text.strip() if cleaned_text else article_text
            formatted_text = re.sub(r'\n{3,}', '\n\n', final_text)

            return {
                "title": title,
                "text": formatted_text
            }
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for URL {company_url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

def get_token():
    retries = 3
    auth_url = ""
    client_id = ""
    client_secret = ""

    body = {
        "grant_type": "client_credentials",
        "scope": "openai"
    }

    headers = {
        "Authorization": f"Basic {base64.b64encode(bytes(client_id + ":" + client_secret, 'ISO-8859-1')).decode('ascii')}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    for i in range(retries):
        try:
            response = requests.post(f'{auth_url}/v1/token', data=body, headers=headers,timeout=300)
            response.raise_for_status()
            token = response.json().get("access_token")
            if not token:
                logging.error("Access token missing in response payload.")
            return token
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {i + 1} failed to fetch token: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)
    return None

def analyze_article(token, article_text):
    retries = 3
    endpoint_url = ""
    format_str = """
    
    """

    content = f"""
    
    """
    
    payload = json.dumps({
        "messages": [
            {"role": "system", "content": "Assistant is a large language model trained for investment analysis."},
            {"role": "user", "content": content"}
        ],
        "max_tokens": 2000
    })

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    for i in range(retries):
        try:
            response = requests.post(endpoint_url, headers=headers, data=payload)
            response.raise_for_status()

            json_res = response.json()
            if 'choices' in json_res:
                answer = json_res['choices'][0]['message']['content']
                return json.loads(answer)
            else:
                logging.error("The 'choices' key is missing from the API response.")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {i + 1} failed to analyze article: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)
    return None

def insert_into_company_table(conn, company_name, company_short_name, company_id):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM company_master_details WHERE company_id = %s;", (company_id,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(
                """
                INSERT INTO company_master_details (company_name, company_short_name, company_id)
                VALUES (%s, %s, %s)
                """,
                (company_name, company_short_name, company_id)
            )
            conn.commit()
            logging.info(f"Inserted company {company_name} into company_master_details.")
        else:
            logging.info(f"Company ID {company_id} already exists. No insertion performed.")
        cursor.close()
    except Exception as e:
        logging.error(f"Error inserting into company table: {e}")

def insert_into_article_table(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """

            """,
            data
        )
        conn.commit()
        logging.info(f"Inserted article data for company_id {data[0]}.")
        cursor.close()
    except Exception as e:
        log_error(conn, "Database Insertion Error", str(e), data[0])
        
def gen_batch_id(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) FROM batch_execution_details
            """
        )
        batch_id = cursor.fetchone()[0] + 1
        conn.commit()
        logging.info(f"Batch_ID has been Generated: {batch_id}.")
        cursor.close()
        return batch_id
    except Exception as e:
        log_error(conn, "Batch_ID Generation Error", str(e), None)

def main():
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    try:
        response = requests.get(os.getenv("BASE_URL_ENTITIES"), headers=headers)
        response.raise_for_status()
        companies_data = response.json()
        if not companies_data or "companies" not in companies_data:
            logging.error("No 'companies' key found in the response.")
            return

        for company in companies_data.get("companies", []):
            company_name = company.get("companyName")
            company_id = company.get("companyId")
            if not company_id:
                logging.error(f"Skipping company with missing company_id: {company_name}")
                continue

            company_short_name = "Unknown"
            insert_into_company_table(conn, company_name, company_short_name, company_id)

            try:
                news_response = requests.get(os.getenv("BASE_URL_NEWS").format(company_id), headers=headers)
                news_response.raise_for_status()
                news_data = news_response.json()

                for article in news_data:
                    company_url = article.get("url")
                    article_data = fetch_and_clean_article(company_url)
                    if not article_data:
                        log_error(conn, "Scraping Error", "Failed to fetch article text", company_url)
                        continue

                    token = get_token()
                    if not token:
                        log_error(conn, "Authentication Error", "Failed to fetch token", company_url)
                        continue

                    analysis = analyze_article(token, article_data["text"])
                    if not analysis:
                        log_error(conn, "Analysis Error", f"Failed to analyze article with company_url: {company_url}", company_url)
                        continue

                    data = 
                    insert_into_article_table(conn, data)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching news data for company ID {company_id}: {e}")
                log_error(conn, "API Request Error", str(e), company_id)

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
