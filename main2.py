import os
import requests
import pandas as pd
import json
import logging
import psycopg2
from datetime import datetime
from bs4 import BeautifulSoup
from newspaper import Article
import re
import base64
import time
from dotenv import load_dotenv
import asyncio
from playwright.async_api import async_playwright
import nest_asyncio

nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration flag to toggle between API and text file processing
USE_PRODUCTION_API = False # Set to True for Sandbox API, False for text file
USE_GIVEN_COMPANY = False # Set to True for given cusom companies to be placed inside list, False for Sandbox API


load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

API_KEY = "PB-Token gPD3qd122Pv2l8lXdxLVMQrGU0NnLeVW"
BASE_URL_ENTITIES = "https://api-v2.pitchbook.com/sandbox-entities?entityType=COMPANIES"
BASE_URL_NEWS  = "https://api.pitchbook.com/entities/{}/news/?trailingRange=30"
ENDPOINT_URL = "https://apim-dev-io-eus-1.azure-api.net/deployments/gpt-4o/chat/completions?api-version=2024-02-15-preview"

# Headers for API requests
HEADERS = {
    "Authorization": API_KEY,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# Database connection
def connect_to_db():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None
    
conn = connect_to_db()

# Log errors to database
def log_error(conn, error_type, error_message, error_data, related_item):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO Logs (Log_Timestamp, error_type, error_message, error_data, related_item)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (datetime.now(), error_type, error_message, error_data, related_item)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to log error: {e}")

def extract_domain_regex(url):
    match = re.search(r"https?://(?:www\.)?([^.]+(?:\.[^.]+)*)\.com", url)
    return match.group(1) if match else None

def is_javascript_disabled(content):
    javascript_disabled_keywords = [
        "JavaScript is disabled",
        "Please enable JavaScript",
        "This site requires JavaScript",
        "Your browser does not support JavaScript",
        "enable JavaScript",
        "JavaScript has been disabled",
        "Sign up now for free access to this content",
        "Continue to Checkout",
        "Continue reading your article with a subscription to continue reading",
        "Continue reading your article with"
    ]
    
    for keyword in javascript_disabled_keywords:
        if keyword.lower() in content.lower():
            return True
    return False

# Fetch and clean article using BeautifulSoup and newspaper3k
def fetch_and_clean_article(company_url):
    retries = 3
    for attempt in range(retries):
        try:
            article = Article(company_url)
            article.download()
            article.parse()

            title= article.title if article.title else "No Article"
            article_html = article.html
            article_text = article.text
            
            soup= BeautifulSoup(article_html, 'html.parser')
            for tag in soup(['script', 'style', 'font', 'footer', 'aside', 'nav', 'advertisement']):
                tag.decompose()
                
            cleaned_text = ' '
            for para in soup.find_all('p'):
                para_text = para.get_text().strip()
                if para_text:
                    cleaned_text += para_text + '\n'
                    
            final_text= cleaned_text.strip() if cleaned_text else article_text
            formatted_text = re.sub( r'\n{3,}', '\n\n', final_text)
            return {"title": title, "text": formatted_text.strip(), "html": article_html}
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for URL {company_url}: {e}")
            #log_error(connect_to_db(), "ERROR", str(e), company_url)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

async def fetch_and_clean_article_pr(company_url):
    """Fetches article content using Playwright for JavaScript-enabled sites."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(company_url, wait_until="domcontentloaded", timeout=10000)
            
            article = Article(company_url)
            article.download()
            article.parse()
            title= article.title if article.title else "No Article"
            article_html = article.html
            
            content = await page.content()
            await browser.close()

            soup = BeautifulSoup(content, "html.parser")
            title = soup.title.string if soup.title else "No Title"
            text = " ".join(p.get_text().strip() for p in soup.find_all("p"))

            return {"title": title, "text": text.strip(), "html": article_html}
    except Exception as e:
        logging.error(f"Playwright error fetching URL {company_url}: {e}")
        log_error(conn, "Scraping Error", "Playwright failed", str(e), company_url)
        return {"error": "Scraping failed"}


def analyze_article(token, article_text, company_name, company_source):
    """Sends article text to LLM for analysis."""
    format_str = """
        {
            "**Company Name**": "[Company Name]",
            "**Article Title**": "[Article Title]",
            "**Article Published Timestamp in PT**": "[MM/dd/yyyy hh:mm:ss] (check the article_text thouroughly to detect date and time, and convert timestamp to Pacific Time if it’s posted as a different time zone)",
            "**Article Modified Timestamp in PT**": "[MM/dd/yyyy hh:mm:ss] (only include if there is an article updated or modified timestamp, convert timestamp to Pacific Time if it’s posted as a different time zone)",
            "**Article News Source**": "[Article News Source]",
            "**Article Summary**": "[Article Summary]", (provide a detailed summary of the article content in 200 or more words)
            "**Sentiment Score**": "[Sentiment Score (based on -10 to 10)]",(provide a sentiment score based on the content of the article and also show the scale of the sentiment score)
            "**Sentiment Score Reasoning**": "[Sentiment Score Reasoning]", (explain the reasoning for the sentiment score, and what parts of the article's text influenced the score and include those texts in teh response)
            "**Company Valuation Significance**": "[Company Valuation Significance]",
            "**Company Valuation Significance Reasoning**": "[Company Valuation Significance Reasoning]",
            "**Explicit Company Impacts**": "[Explicit Company Impacts (that summarize the direct impact to the valuation of the company the news article is primarily about)]",
            "**Implicit Industry Impacts**": "[Implicit Industry Impacts (if any) that explain potential impacts beyond what’s in the article itself such as impacts to other companies or the industry]",
            "**Implicit Impact Peer Companies**": "[Implicit Impact Peer Companies (these are companies in the same industry or companies that may be affected by the news. List them in this field separated by a comma)]"
        }
        """
    content = f"""
        I own an investment in {company_name}. Based on the article content provided below, analyze it and provide the output in the exact dictionary format outlined below:
        {format_str} consider [Article News Source] as {company_source}. 

        News Article Content:
        {article_text}
        - Detect the date and time accurately and there will be different timezones in different articles 
            (namely Eastern time -ET, Central Time -CT, Pacific Time -PT). So, make sure to convert the timestamp to Pacific Time and provide it in the output.
        - Ensure the response is factually accurate, follows the dictionary structure strictly, and maintains field order.
        - Convert timestamps to Pacific Time.
        - If no content is available, return a dictionary where all fields contain "Insufficient Data to perform analysis".
        """
    payload = json.dumps({
        "messages": [
            {"role": "system", "content": "Assistant is a large language model trained for investment analysis."},
            {"role": "user", "content": f"Analyze this article: {content}"}
        ],
        "max_tokens": 2000,
        "temperature": 0.0,
    })

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    for i in range(5):
        try:
            response = requests.post(ENDPOINT_URL, headers=headers, data=payload)
            response.raise_for_status()
            json_res = response.json()

            # Improved response handling
            if isinstance(json_res, dict) and 'choices' in json_res and json_res["choices"]:
                first_choice = json_res['choices'][0]
                if isinstance(first_choice, dict) and 'message' in first_choice and 'content' in first_choice['message']:
                    answer = first_choice['message']['content']
                else:
                    logging.error("Unexpected API response format.")
                    return {"error": "Invalid response format"}
            else:
                logging.error("API response missing choices.")
                return {"error": "Missing choices"}

            match = re.search(r'(\{.*\})', answer, re.DOTALL)
            if match:
                answer = match.group(0).strip()
                return json.loads(answer)  # Parse JSON safely
            else:
                logging.error("No JSON structure found in LLM response.")
                return {"error": "Malformed response"}
        
        except requests.exceptions.RequestException as e:
            logging.error(f"GPT API error on attempt {i + 1} for analyze article function: {e}")
            if i < 4:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                return {"error": "LLM request failed"}

# Retrieve token for authentication
def get_token():
    retries = 3
    client_id = '0oa286b3cb29vETey0h8'
    client_secret = 'CPksgNFQaFIE1rtE2AFuh6FyBcCADA-tFGLJoGetEqOmrHlLDxkUZ9likHA8vj2p'
    auth_url = 'https://capgroup-dev.oktapreview.com/oauth2/aus286bigg9s2o7zq0h8'

    body = {
        "grant_type": "client_credentials",
        "scope": "openai"
    }
    authorization = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")
    headers = {
        "Authorization": f"Basic {authorization}",
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

# Insert companies into database
def insert_into_company_table(conn, company_name, company_short_name, company_id):
    """Inserts company details into the database safely."""
    if not conn:
        logging.error("Database connection is None.")
        return
    
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
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting into company table: {e}")
    finally:
        cursor.close()

# Insert articles into database
def insert_into_article_table(conn, data):
    """Inserts article analysis data into the database."""
    if not conn:
        logging.error("Database connection is None.")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO article_analysis_data (company_id, company_url, art_title, art_raw_data, art_summary, art_eval_metrics,
                                            art_sentiment_score, art_sentiment_score_reasoning, art_analysis, art_published_on,
                                            art_modified_on, company_valuation_significance, company_valuation_significance_reasoning,
                                            explicit_company_impacts, implicit_industry_impacts, implicit_impact_peer_companies ,
                                            sys_name, user_name, exe_on, batch_id, raw_html_content)
            VALUES (%s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            data
        )
        conn.commit()
        logging.info(f"Inserted article data for company_id {data[0]}.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting into article table: {e}")
        log_error(conn, "Database Error", "Error while Data Insertion into DB", str(e), data[0])
    finally:
        cursor.close()

        
############LLM Evaluation Metrics####################
def article_evaluation_metrics(refdata,generateddata):
    """Computes evaluation metrics (BERT Score, Accuracy) between reference & generated summaries."""
    max_retry = 5
    try:       
        
        prompt = f"""
        You are an NLP evaluation expert. Your task is to compare the **Generated Summary** and **Reference Summary** based on their semantic similarity and accuracy.
        ### **Reference Summary**:
        {refdata}
        ### **Generated Summary**:
        {generateddata}
                    
        ### **Instructions:**
        1. **Compute the following evaluation metrics based on the given summaries:**
           - **BERT Score** (range: 0 to 1) → Measure the deep contextual similarity between the reference and generated summary.
           - **Accuracy Score** (range: 0 to 100) → Determine how closely the generated summary matches the reference summary in terms of key information and structure.
        
        2. **Provide the results in a structured format:**
           - Display the computed scores in a table.
           - Explain how each score was derived based on the actual content.
        
        ---

        ### **Expected Output Format:**
        
        | Metric      | Score | Explanation |
        |------------|-------|-------------|
        | BERT Score | X.XX  | Justification based on key phrase similarity and semantic overlap. |
        | Accuracy   | X.XX  | Explanation considering word overlap, missing details, or extra information. |
        
        ---
        
        ### **Analysis Section**
        1. **Why is the BERT Score for this generated summary?**  
           - Identify key phrases that match and highlight deviations.
           - Explain whether the generated summary maintains meaning.
           - **Explain it in Layman terms.**
            - *Does the generated summary "mean the same thing" as the reference summary, even if the different words are used?*
            - *Would an average person get the same understanding from both summaries?* 
        
        2. **Why is the Accuracy Score for this generated summary?**  
           - Analyze the correctness of details in `{generateddata}` compared to `{refdata}`.
           - Mention whether the generated summary missed or added any critical details.
           - **Explain it in Layman terms.**
            - *If someone only reads the geenrated summary, would they still know the key facts?*
            - *Are any important details missing or changed that might confuse the reader?* 
           
        ---
        
        ### **Guidelines:**
        - **Use actual content** of `{refdata}` and `{generateddata}` to compute the scores.
        - **Avoid generic descriptions**—base all responses on real comparisons.
        - **Provide meaningful justifications** for both scores.
        """
              
        payload = json.dumps({
            "messages": [
                {"role": "system", "content": "Assistant is evaluating NLP quality."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 2000,
        })
        headers = {
            "Authorization": f"Bearer {get_token()}",
            "Content-Type": "application/json"
        }
        for attempt in range(1, max_retry + 1):
            try:
                response = requests.post(ENDPOINT_URL, headers=headers, data=payload)
                response.raise_for_status()
                result = response.json()['choices'][0]['message']['content']
                return result
            
            except requests.exceptions.RequestException as e:
                logging.error(f"Attempt {attempt} failed: {e}")
                time.sleep(2 ** attempt)

        logging.error("Max retries reached. Failed to fetch evaluation metrics.")
        return None                
             
    except Exception as e:
        logging.error(f"Error in fetching Evaluation Metrics: {e}")
        return None

def parse_text_to_dataframe(text):
    """Parses structured text data into a list of dictionaries for DataFrame conversion."""
    if not text or not isinstance(text, str):
        logging.error("Invalid input: The text provided is empty or not a string.")
        return []

    companies = []
    lines = text.strip().split("\n")
    current_company = {}

    try:
        for line in lines:
            line = line.strip()

            if line.startswith("Company ID:"):
                if current_company:
                    companies.append(current_company)
                current_company = {"company_id": line.split(":", 1)[1].strip()}

            elif line.startswith("Company Name:"):
                current_company["company_name"] = line.split(":", 1)[1].strip()

            elif line.startswith("Number of URLs:"):
                try:
                    current_company["number_of_urls"] = int(line.split(":", 1)[1].strip())
                except ValueError:
                    logging.warning(f"Invalid number format in line: {line}")
                    current_company["number_of_urls"] = 0
                current_company["urls"] = []

            elif line.startswith("- "):  # URL line
                url = line[2:].strip()
                if url.lower() != "none":
                    current_company["urls"].append(url)

            elif line.startswith("No URLs found."):
                current_company["urls"] = []

        if current_company:
            companies.append(current_company)

    except Exception as e:
        logging.error(f"Error parsing text to DataFrame format: {e}")
        return []

    return companies

def insert_dataframe_to_table(df, conn):
    """Inserts company data from a DataFrame into the database."""
    if conn is None:
        logging.error("Database connection is None. Cannot insert data.")
        return

    if df is None or df.empty:
        logging.warning("No data provided for insertion.")
        return

    table_name = "company_master_details"
    company_short_name = "unknown"

    try:
        cursor = conn.cursor()
        insert_query = f""" 
        INSERT INTO {table_name} (company_name, company_short_name, company_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (company_id) DO NOTHING;
        """

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (row["company_name"], company_short_name, row["company_id"]))
            except Exception as row_error:
                logging.error(f"Error inserting row {row.to_dict()}: {row_error}")

        conn.commit()
        logging.info("Data inserted successfully.")

    except Exception as e:
        logging.error(f"Database insertion error: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
