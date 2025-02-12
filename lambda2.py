import os
import json
import logging
import psycopg2
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
from newspaper import Article
import re
import base64
from dotenv import load_dotenv
import asyncio
from playwright.async_api import async_playwright
import nest_asyncio

nest_asyncio.apply()
# Load Environment Variables
load_dotenv()

# Database Credentials
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

API_KEY = "PB-Token gPD3qd122Pv2l8lXdxLVMQrGU0NnLeVW"
BASE_URL_NEWS = "https://api.pitchbook.com/entities/{}/news/?trailingRange=30"

logging.basicConfig(level=logging.INFO)

def connect_to_db():
    """Establish a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database Connection Error: {e}")
        return None
    
def log_error(conn, error_type, error_message, error_data, related_item):
    """Logs errors into the database."""
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
    """Extracts the domain from a given URL."""
    match = re.search(r"https?://(?:www\.)?([^.]+(?:\.[^.]+)*)\.com", url)
    return match.group(1) if match else None

def is_javascript_disabled(content):
    """Checks if JavaScript is disabled based on specific keywords."""
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
        "Continue reading your article with",
        "Economic Times",
        "Economic Times Prime",
        "Subscribers Only",
        "Download The Economic Times"
    ]
   
    for keyword in javascript_disabled_keywords:
        if keyword.lower() in content.lower():
            return True
    return False

def get_metadata(html_soup):
    """Extract metadata (dates and article details) from HTML soup."""
    def k_text(text):
        return re.sub(r'^\s*Last Updated:\s*', '', text).strip()
    
    metadata = {}
    # Extract metadata from <meta> tags
    for meta in html_soup.find_all("meta"):
        if meta.get("property") in ["article:published_time", "article:modified_time", "og:updated_time", "og:published_time"]:
            metadata[meta.get("property")] = meta.get("content")
        if meta.get("name") in ["datePublished", "dateModified"]:
            metadata[meta.get("name")] = meta.get("content")
    # Extract JSON-LD metadata
    json_ld_data = html_soup.find("script", type="application/ld+json")
    if json_ld_data:
        try:
            json_data = json.loads(json_ld_data.string)
            if isinstance(json_data, list):
                json_data = json_data[0]  
            metadata["datePublished"] = json_data.get("datePublished", metadata.get("datePublished"))
            metadata["dateModified"] = json_data.get("dateModified", metadata.get("dateModified"))
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON-LD metadata: {e}")

    # Extract time-based metadata from <time> tags
    for time_tag in html_soup.find_all("time"):
        if "jsdtTime" in time_tag.get("class", []):
            text = time_tag.text.strip()
            metadata["dateModified"] = k_text(text)
        if time_tag.get("dateModified"):
            metadata["datePublished"] = time_tag.get("datetime")
           
    # Extract date information from raw page text
    page_text = html_soup.get_text()
    date_match = re.search(r'\b(\d{4}-\d{2}-\d{2}|\d{1,2} \w+ \d{4},? \d{1,2}:\d{2} [APM]{2} \w{3})\b', page_text)
    if date_match:
        metadata["datePublished"] = date_match.group(0)
        
    # Extract additional date information from article body and teaser
    article_body = html_soup.find("div", id="article-body")
    teaser_div = html_soup.find("div", id="teaser")
    text = (article_body.text if article_body else '') + (teaser_div.text if teaser_div else '')
    date_match = re.search(r'\b(\w+ \d{1,2},\s*\d{4},?\s*\d{1,2}:\d{2}\s*[APM]{2}\s*\w{3})\b', text)
    if date_match:
        metadata["datePublished"] = date_match.group(0)

    return metadata

def fetch_and_clean_article(company_url):
    """Fetches and cleans article content using Newspaper3k."""
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
            metadata = get_metadata(soup)

            for tag in soup(['script', 'style', 'font', 'footer', 'aside', 'nav', 'advertisement']):
                tag.decompose()
               
            cleaned_text = ' '
            for para in soup.find_all('p'):
                para_text = para.get_text().strip()
                if para_text:
                    cleaned_text += para_text + '\n'
                   
            final_text= cleaned_text.strip() if cleaned_text else article_text
            formatted_text = re.sub( r'\n{3,}', '\n\n', final_text)
            return {"title": title, "text": formatted_text.strip(), "html": article_html, "metadata": metadata}
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for URL {company_url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

async def fetch_and_clean_article_pr(conn,company_url):
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
        metadata = get_metadata(soup)
        title = soup.title.string if soup.title else "No Title"
        text = " ".join(p.get_text().strip() for p in soup.find_all("p"))

        return {"title": title, "text": text.strip(), "html": article_html, "metadata": metadata}
    except Exception as e:
        logging.error(f"Playwright error fetching URL {company_url}: {e}")
        log_error(conn, "Scraping Error", "Playwright failed to download/parse the URL content", str(e), company_url)
        return {"error": "Scraping failed"}

def analyze_article(token, article_text, company_name, company_source, metadata, company_url):
    """Sends article text to LLM for analysis."""
    format_str = """
        {
             "**Company Name**": "[Company Name]",
             "**Article Title**": "[Article Title]",
             "**Article Published Timestamp in PT**": "[MM/dd/yyyy hh:mm:ss] (check the article_text thouroughly to detect date and time, and convert timestamp to Pacific Time if it’s posted as a different time zone)",
             "**Article Modified Timestamp in PT**": "[MM/dd/yyyy hh:mm:ss] (only include if there is an article updated or modified timestamp, convert timestamp to Pacific Time if it’s posted as a different time zone)",
             "**Article News Source**": "[Article News Source]",
             "**Article Summary**": "[Article Summary]", (provide a detailed summary of the article content in maximum of 200 words and also provide a central theme of the article in maximum of 75 words, which includes vital information like about the company,competitors, legal issues, Lawsuits, product launches, researches, financial information with numbers, political issues and affiliations, investments, acquisions, mergers, divestments, partnerships, joint ventures, product launches, earnings reports,
                and any other relevant information that could impact the valuation, business, and reputation of the company. In short, provide the crux
                of the article in a nutshell with seperate side heading as **Central Theme**.)",
             "**Sentiment Score**": "[Sentiment Score (based on -10 to 10)]",(provide a sentiment score in context of market situation and company's valuation and business significance
                                            based on the information in article, and particularly on the content of the article and also show the scale of the sentiment score),
             "**Sentiment Score Reasoning**": "[Sentiment Score Reasoning]", (explain the reasoning for the sentiment score, and provide vital parts of the article's text that is directly related and influenced
                                                     the score and include those texts in the response. Double check the accuracy of the sentiment score reasoning
                                                     and also the vital parts of the article's text that is directly related and influenced the score.),
             "**Company Valuation Significance**": "[Company Valuation Significance]", (Provide the severity of the article's impact on the company's valuation),
             "**Company Valuation Significance Reasoning**": "[Company Valuation Significance Reasoning]", (explain the reasoning for the company valuation significance score,
                                                                       and provide vital parts of the article's text that is directly related and influenced the score and include
                                                                       those texts in the response. Double check the accuracy of the sentiment score reasoning and also
                                                                       the vital parts of the article's text that is directly related and influenced the score),
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

        - If you don't find article title in the news article content above, then get the title from the url {company_url} for the article title alone, and don't generate the
             article cotent or summary on your own.
        - if you don't find datePublished or dateModified in the news article content above,  
             then use this "metadata" {metadata} for the datePublished and dateModified and it can be in any timezone. Find the timezone and convert it to PST.
        - Detect the date and time accurately and there will be different timezones in different articles
             (namely Eastern time -ET (or) EDT (or) EST, Central Time -CT (or) CDT (or) CST, Pacific Time -PT (or) PST (or) PDT).
             So, make sure to convert all the timestamps provided in the article to Pacific Standard Time (PST) and provide it in the output.
        - Ensure the response is factually accurate, follows the dictionary structure strictly, and maintains the order of the fields.
        - If no content is available, return the dictionary where all fields contain "Insufficient Data to Analyse"
             and return "Not Analyzed" for only [Sentiment Score] field.
       
        """
    payload = json.dumps({
        "messages": [
            {"role": "system", "content": "Assistant is an expert in investment banking, asset maangement and financial services."},
            {"role": "user", "content": f"Analyze this article: {content}"}
        ],
        "max_tokens": 2000,
        "temperature": 0.0
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
                return json.loads(answer) # Parse JSON safely
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
       
        | Metric      | Score | Explanation |
        |------------|-------|-------------|
        | BERT Score | X.XX  | Justification based on key phrase similarity and semantic overlap.|
        | Accuracy   | X.XX  | Explanation considering word overlap, missing details, or extra information.|
       
        ---
       
        ### **Analysis Section**
        1. **What is BERT Score and why is it generated and it's relevance for the generated summary?**  
            - Identify key phrases that match and highlight deviations.
            - Explain whether the generated summary maintains meaning.
            - **Explain it in Layman terms.** Incorporate BERT score wherever needed and explain what that score conveys that is understandable to a layman.
             - *Does the generated summary "mean the same thing" as the reference summary, even if the different words are used?*
             - *Would an average person get the same understanding from both summaries?*
       
        2. **What is the Accuracy Score and why is generated and it's relevance for the generated summary?**  
            - Analyze the correctness of details in `{generateddata}` compared to `{refdata}`.
            - Mention whether the generated summary missed or added any critical details.
            - **Explain it in Layman terms.** Incorporate Accuracy score wherever needed and explain what that score conveys that is understandable to a layman.
             - *If someone only reads the geenrated summary, would they still know the key facts?*
             - *Are any important details missing or changed that might confuse the reader?*
            
        ---
       
        ### **Guidelines:**
        - **Use actual content** of `{refdata}` and `{generateddata}` to compute the scores.
        - **Avoid generic descriptions**—base all responses on real comparisons.
        - **Provide meaningful justifications** for both scores. User should understand what is BERT score and Accuracy and how is it relevant to the
             generated summary. Also, user should understand the relevance and significance of these metrics for the generated summary. Assume user is
             not an expert in NLP and evaluating the quality of the generated summary using metrics like BERT score and Accuracy. Also, assume user is not
             tech savvy and all your explanations should be in layman terms.
        - **Be concise and clear** in your explanations, ensuring they are easy to understand.
        - For expected output format, don't add grid lines for every single point, instead use a single grid line for each metric.
        - These guidelines are for your reference and to be strictly adhered on how you should respond to the user and do not include in the response.
        """
        payload = json.dumps({
            "messages": [
                {"role": "system", "content": "Assistant is an expert in NLP and evaluating the quality of the generated summary using metrics like BERT score and Accuracy."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 2000
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
           
def fetch_news_data(company_id):
    """Fetches news data for a given company ID."""
    try:
        response = requests.get(BASE_URL_NEWS.format(company_id), headers={"Authorization": API_KEY})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching news data for company ID {company_id}: {e}")
        return None

def process_data(conn, company_id, company_name, article):
    """Processes an article for analysis."""
    company_url = article.get("url")
    article_data = fetch_and_clean_article(company_url)

    if not article_data or article_data.get("text") == "" or is_javascript_disabled(article_data.get("text", "")):
        logging.warning(f"JavaScript is disabled for URL {company_url}. Trying with playwright.")
        article_data = asyncio.run(fetch_and_clean_article_pr(conn, company_url))
        
        if is_javascript_disabled(article_data.get("text", "")):
            log_error(conn, "JavaScript Disabled", "JavaScript disabled error not resolved for URL even after trying with playwright, scrapes the error message", article_data.get("text", ""), company_url)
            article_data["text"] = "Javascript is disabled - Capturing Irrelevent data / Advertisements data from the website"
    if not isinstance(article_data, dict):
        article_data = {}
        article_data = {"title" : article_data.get("title", ""), "text": article_data.get("text", ""), "html": article_data.get("html", ""), "metadata": article_data.get("metadata", {})}

    token = get_token()
    if not token:
        logging.error("Failed to fetch authentication token.")
        log_error(conn, "Authentication Error", "Failed to fetch token", "", company_url)

    company_source = extract_domain_regex(company_url)
    company_url_title = article_data["title"]
    if article_data["title"] == "":
        company_url_title = re.sub(r'https?://[^/]+', '', company_url)

    analysis = analyze_article(token, article_data["text"], company_name, company_source, article_data["metadata"], company_url_title)
    confidence_score = article_evaluation_metrics(article_data["text"], analysis)
    sys_name = conn.info.dbname
    user_name = conn.info.user
    def clean_timestamp(timestamp_str):
        if timestamp_str in ["N/A", "None", None, "", "Insufficient Data to Analyse"]:
            return None
        return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S").strftime("%m-%d-%Y %H:%M:%S")

    if analysis:
        pst_now_published = analysis.get("**Article Published Timestamp in PT**", None)
        pst_now_modified = analysis.get("**Article Modified Timestamp in PT**",None)
    else:
        pst_now_published = None
        pst_now_modified = None
    pst_now_published = clean_timestamp(pst_now_published)
    pst_now_modified = clean_timestamp(pst_now_modified)
    execution_timestamp = datetime.now().astimezone().strftime("%m-%d-%Y %H:%M:%S")

    data = (
        company_id, company_url, analysis.get("**Article Title**"), article_data["text"],
        analysis.get("**Article Summary**"), confidence_score, analysis.get("**Sentiment Score**"),
        analysis.get("**Sentiment Score Reasoning**"),
        json.dumps(analysis),
        pst_now_published, pst_now_modified,
        analysis.get("**Company Valuation Significance**"), analysis.get("**Company Valuation Significance Reasoning**"),
        analysis.get("**Explicit Company Impacts**"), analysis.get("**Implicit Industry Impacts**"), analysis.get("**Implicit Impact Peer Companies**"),
        sys_name, user_name,execution_timestamp, 1, article_data["html"])
    return data


def lambda_handler(event, context):
    """AWS Lambda handler function."""
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    batch_number = event.get("batch_number")
    companies = event.get("companies", [])

    logging.info(f"Processing batch {batch_number} with {len(companies)} companies.")

    for company_name, company_id in companies:
        news_data = fetch_news_data(company_id)
        if not news_data:
            continue

        for article in news_data:
            processed_data = process_data(conn, company_id, company_name, article)
            if processed_data:
                insert_into_article_table(conn, processed_data)

    conn.close()
    logging.info("Processing complete.")
    return {"statusCode": 200, "message": f"Batch {batch_number} processed successfully."}
