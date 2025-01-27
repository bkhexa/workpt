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
USE_PRODUCTION_API = True  # Set to True for production API, False for text file

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

API_KEY = "PB-Token gPD3qd122Pv2181XdxLVMQrGU0NnLeVW"
BASE_URL_ENTITIES = "https://api-v2.pitchbook.com/sandbox-entities?entityType=COMPANIES "
BASE_URL_NEWS  = "https://api.pitchbook.com/entities/{}/news/?trailingRange=30"
ENDPOINT_URL = "https://apim-dev-io-eus-1.azure-api.net/deployments/gpt-4o/chat/completions?api-version=2024-02-24"

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

# Log errors to database
def log_error(conn, error_message, error_type, related_item):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO Logs (Log_Timestamp, Log_Level, Log_Message, Error_Type, Error_Details, Related_Item)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (datetime.now(), "ERROR", error_message, error_type, error_message, related_item)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to log error: {e}")
def is_javascript_disabled(content):
    javascript_disabled_keywords = [
        "JavaScript is disabled",
        "Please enable JavaScript",
        "This site requires JavaScript",
        "Your browser does not support JavaScript",
        "enable JavaScript",
        "JavaScript has been disabled",
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
            if is_javascript_disabled(article_text):
                logging.warning(f"JavaScript is disabled for URL: {company_url}")
                return {"title": title, "text": "JavaScript is disabled. Retry with Playwright or enable JavaScript."}
            soup= BeautifulSoup(article_html, 'html.parser')
            for tag in soup([' script' , 'style' , 'font', 'header', 'footer', 'aside' , 'nav' , ' advertisement' ] ):
                tag.decompose()
                
            cleaned_text = ' '
            for para in soup.find_all('p'):
                para_text = para.get_text().strip()
                if para_text:
                    cleaned_text += para_text + '\n'
                    
            final_text= cleaned_text.strip() if cleaned_text else article_text
            formatted_text = re.sub( r'\n{3,}', '\n\n', final_text)
            return {"title": title, "text": formatted_text.strip()}
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for URL {company_url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None


async def fetch_and_clean_article_pr(company_url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(company_url, wait_until="networkidle")
            content = await page.content()
            await browser.close()
            soup = BeautifulSoup(content, "html.parser")
            title = soup.title.string or "No Title"
            text = " ".join(p.get_text().strip() for p in soup.find_all("p"))
            if is_javascript_disabled(text):
                logging.warning(f"JavaScript is disabled for URL: {company_url}")
                return {"title": title, "text": "JavaScript is disabled. Retry with JavaScript enabled."}
            return {"title": title, "text": text.strip()}
    except Exception as e:
        logging.error(f"Playwright error fetching URL {company_url}: {e}")
        return None
    
# Analyze article using API
def analyze_article(token, article_text):
    format_str = """
    {
        "Company Name": " [Company Name]",
        "Article Title": "[Article Title]",
        "Article Published Timestamp in PT": "[MM/dd/yyyy hh:mm:ss a] (convert timestamp to PT if it's posted as a diffe]",
        "Article Modified Timestamp in PT" : "[MM/dd/yyyy hh:mm:ss a] (only include if there is an article updated or mod]",
        "Article News Source": "[Article News Source]",
        "Article Summary": "[Article Summary]",
        "Sentiment Score": "[Sentiment,. Score (based on -10 to 10)]",
        "Sentiment Score Reasoning": "[Sentiment Score Reasoning]",
        "Company Valuation Significance": "[Company Valuation Significance]",
        "Company Valuation Significance Reasoning": "[Company Valuation Significance Reasoning]",
        "Explicit Company Impacts " : "[Explicit Company Impacts (that summarize the direct impact to the valuation of]",
        "Implicit Industry Impacts": "[Implicit Industry Impacts (if any) that explain potential impacts beyon]",
        "Implicit Impact Peer Companies": "[Implicit Impact Peer Companies (these are companies in the same industry]",
    }
    """
    
    content = f"""
        I own an investment in the company in the news article.
        Based on the news article provided below, please provide me an analysis in a dictionary format ( exacty as given) {format_str}
        Based on this news article content: {article_text}
        """
        
    payload = json.dumps({
        "messages": [
            {"role": "system", "content": "Assistant is a large language model trained for investment analysis."},
            {"role": "user", "content": f"Analyze this article: {content}"}
        ],
        "max_tokens": 2000
    })
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    retries = 3
    for i in range(retries):
        try:
            response = requests.post(ENDPOINT_URL, headers=headers, data=payload)
            response.raise_for_status()
            json_res = response.json()
            if 'choices' in json_res and 'message' in json_res['choices'][0]:
                return json.loads(json_res['choices'][0]['message']['content'])
            else:
                logging.error("The 'choices' key is missing from the API response.")
                return None 
        except requests.exceptions.RequestException as e:
            logging.error(f"GPT API error on attempt {i + 1}: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)
    return None

# Retrieve token for authentication
def get_token():
    retries = 3
    client_id = '0oa286b3cb29vETey0h8'
    client_secret = 'CPksgNFQaFIElrtE2AFuh6F~BcCADA-tFGLJoGetEqOmrHlLDxkUZ9likHA8vj2p '
    auth_url = 'https://capgroup-dev.oktapreview.com/oauth2/aus286bigg9s2o7zq0h8'

    body = {
        "grant_type": "client_credentials",
        "scope": "openai"
    }
    authorization= base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii" )
    headers = {
        "Authorization": f"Basic {authorization}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    for i in range(retries):
        try:
            response= requests.post(f'{auth_url}/vl/token', data=body, headers=headers, timeout=300)
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

# Insert articles into database
def insert_into_article_table(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO article_analysis_data (company_id, company_url, art_title, art_raw_data, art_summary, art_eval_metrics,
                                            art_sentiment_score, art_sentiment_score_reasoning, art_analysis, art_published_on,
                                            art_modified_on, company_valuation_significance, company_valuation_significance_reasoning,
                                            explicit_company_impacts, implicit_industry_impacts, implicit_impact_peer_companies ,
                                            sys_name, user_name, exe_on, batch_id)
            VALUES (%s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            data
        )
        conn.commit()
        logging.info(f"Inserted article data for company_id {data[0]}.")
        cursor.close()
    except Exception as e:
        log_error(conn, "Database Insertion Error", str(e), data[0])


def gen_batch_id(conn):
    try :
        cursor= conn.cursor()
        cursor.execute(
            """
            SELECT COUNT (*) FROM batch_execution_details
            """
        )  
        batch_id = cursor.fetchone()[0] + 1
        conn.commit()
        logging.info(f"Batch_ID has been Generated: {batch_id}." )
        cursor.close()
        return batch_id
    except Exception as e:
        log_error(conn, "Batch_ ID Generation Error" , str(e), None )
        

  

def insert_into_prod_table(conn, company_short_name, company_id):
    try:
        cursor= conn.cursor()
        cursor.execute("SELECT * FROM prod_table WHERE company_short_name· = %s AND company _ i d = %s;", (company_id))
        exists= cursor.fetchone()
        if not exists:
            cursor.execute(
                """
                INSERT INTO prod_table (company_short_name, company_id)
                VALUES (%s, %s)
                """,
                (company_short_name, company_id)
            )
            conn.commit()
            logging.info(f"Inserted company with ID {company_id} into prod_table.")
        else:
            logging.info(f"Company ID {company_id} already exists. No insertion performed." )    
        cursor.close()    
    except Exception as e:
        logging.error(f"Error inserting into prod_table: {e}")        

 
 
 
# First main function: Process company data from API        
def main_api():
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    try:
        response= requests.get(BASE_URL_ENTITIES, headers=HEADERS)
        response.raise_for_status()
        companies_data = response.json()

        for company in companies_data.get("companies", []):
            company_name = company.get("companyName")
            if company_name:
                company_name = company_name.strip()
                company_id = company.get("companyid")
                cursor= conn.cursor()
                cursor.execute(
                    """
                    UPDATE prod_table
                    SET company_id = %s
                    WHERE company_name = %s
                    """ ,
                    (company_id, company_name)
                )
                if cursor.rowcount == 0 :
                    cursor.execute(
                        """
                    INSERT INTO prod_table (company_name, company_id)
                    VALUES (%s, %s)
                    """ ,
                    (company_name, company_id)
                    )
                    conn.commit()
                    cursor.close()
                    logging.info(f"Updated or inserted co111pany_id for {company_name} in prod_table.")
                    
                    try:
                        news_response = requests.get(BASE_URL_ENTITIES.format(company_id), headers=HEADERS)
                        news_response.raise_for_status()
                        news_data = news_response.json()
                        for article in news_data:
                            company_url = article.get("url")
                            article_data = fetch_and_clean_article(company_url)
                            if not article_data:
                                logging.warning("Primary method failed, ·trying with playwright.")
                                article_data = asyncio.run(fetch_and_clean_article_pr(company_url))
                                if not article_data:
                                    log_error(conn, "Scraping Error", "Failed to fetch article text" , company_url)
                                    continue
                            token = get_token()            
                            if not token:
                                log_error(conn, "Authentication Error", "Failed to fetch token", company_url)
                                continue
                            analysis = analyze_article(token, article_data["text"])    
                            if not analysis:
                                log_error(conn, "Analysis Error", f"Failed to analyze article with company_url: {company_url}", company_url)
                                continue
                            data = (
                                company_id, company_url, article_data["title"], article_data["text"],
                                analysis.get("Article Summary"), 'TBD', analysis.get("Sentiment Score"),
                                analysis.get("Sentiment Score Reasoning"),
                                json . dumps(analysis),
                                datetime.now(), datetime.now(),
                                analysis.get("Company Valuation Significance"), analysis.get( "Company Valuation"),
                                analysis.get("Explicit Company Impacts"), analysis.get("Implicit Industry Impacts"),
                                datetime.now(), 1
                            )
                            insert_into_article_table(conn, data)
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Error fetching news data for company ID {company_id}: {e}")
                        log_error(conn, "API Request Error", str(e), company_id)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching company data from API : {e}")
        log_error(conn, "API Request Error", str(e), None)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
            
   
           
############## Main function for Production URL's data fetching and processing - from txt file # ######

def parse_text_to_dataframe(text):
    companies = []
    lines = text.strip().split("\n")
    current_company = {}

    for line in lines:
        if line.startswith("Company ID:"):
            if current_company:
                companies.append(current_company)
            current_company = {"company_id": line.split(":", 1)[1].strip()}
        elif line.startswith("Company Name:"):
            current_company["company_name"] = line.split(":", 1)[1].strip()
        elif line.startswith("Number of URLs:"):
            current_company["number_of_urls"] = int(line.split(":", 1)[1].strip())
            current_company["urls"] = []
        elif line.startswith("  - "):
            url = line[3:].strip()
            if url.lower() != "none":
                current_company["urls"].append(url)
        elif line.startswith("No URLs found."):
            current_company["urls"] = []

    if current_company:
        companies.append(current_company)

    return companies

def insert_dataframe_to_table(df,conn):
    table_name = "company_master_:details"
    company_short_name = "unknown"
    try:
    # Prepare the insert query
        insert_query = f""" 
        INSERT INTO {table_name} (compony_name, company_short_name, company_id)
        VALUES (%s, %s,%s)
        ON CONFLICT (company_id) DO NOTHING;
        """           
        # Connect to the database
        cursor= conn.cursor()
        # Iterate over DataFrame rows
        for _, row in df.iterrows():
            cursor.execute(insert_query, (row["company_name"], company_short_name, row["company_id" ]))
            # Commit the transaction
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error : {e}")
        conn.rollback() 
    finally:
        cursor.close()
   
   
   # Second main function: Process URLs from text file
def main_text():
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    try:
        file_path = "company_article_urlsS.txt"
        with open(file_path, "r") as file:
            text_data = file.read()
        parsed_data = parse_text_to_dataframe(text_data)
        df = pd.DataFrame(parsed_data)
        try:
            insert_dataframe_to_table(df, conn)
        except Exception as e:
            logging.error(f"Error inserting data into table : {e}")
            
        file_path = " company_a r tic le_urlsS.txt "
        with open(file_path, " r" ) as file:
            text_data = file.read()
        parsed_data = parse_text_to_dataframe(text_data)
        df = pd.DataFrame(parsed_data)
        
        df["urls"] = df["urls"].apply(lambda x: x if isinstance(x, list) else[])
        print(df)
        exploded_df = df.explode( "urls" ).reset_index(drop=True)
        exploded_df = exploded_df[exploded_df[ "urls" ].notna()]
        print(exploded_df)
        
        if exploded_df.empty :
            logging.warning( "Database is empty")
        else :
            for _, row in exploded_df.iterrows():
                company_url = row["urls"]
                company_id = row["company_id"]
                article_data = fetch_and_clean_article(company_url )
                if not article_data:
                    logging.warning("Primary method failed, trying with playwright.")
                    article_data = asyncio.run(fetch_and_clean_article_pr(company_url))
                    if not article_data:
                        log_error(conn, "Scraping Error", "Failed to fetch article text", company_url )
                        continue
    
                token = get_token()            
                if not token:
                    log_error(conn, "Authentication Error", "Failed to fetch token", company_url)
                    continue
                    
                analysis = analyze_article(token, article_data["text"])    
                if not analysis:
                    log_error(conn, "Analysis Error", f"Failed to analyze article with company_url: {company_url}", company_url)
                    continue
                    
                data = (
                    company_id, company_url, article_data["title"], article_data["text"],
                    analysis.get("Article Summary"), 'TBD', analysis.get("Sentiment Score"),
                    analysis.get("Sentiment Score Reasoning"),
                    json.dumps(analysis),
                    datetime.now(), datetime.now(),
                    analysis.get("Company Valuation Significance"), analysis.get( "Company Valuation"),
                    analysis.get("Explicit Company Impacts"), analysis.get("Implicit Industry Impacts"),
                    datetime.now(), 1
                )
                insert_into_article_table(conn, data)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching news data for company ID {company_id}: {e}")
        log_error(conn, "API Request Error", str(e), company_id)

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")     
        
    
if __name__ == "__main__":
    if USE_PRODUCTION_API:
        logging.info("Running the main function for production API.")
        main_api()
    else:
        logging.info("Running the main function for text file processing.")
        main_text()
















import requests
url = ""
headers = {
    "Authorization": ""
}

response = requests.get(url, headers=headers)
data = response.json()

target_name = "Raymond Loewy International"
company_details = [
    company for company in data.get("companies", []) if company["companyName"] == target_name]

if company_details:
    print("Company Found:", company_details[0])
else:
    print("Company not found.")




















