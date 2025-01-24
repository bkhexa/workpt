def main():
    conn = connect_to_db()
    if not conn:
        logging.error("Exiting: Database connection could not be established.")
        return

    try:
        # Step 1: Fetch company data
        response = requests.get(base_url_entities, headers=headers)
        response.raise_for_status()
        companies_data = response.json()

        if not companies_data or "companies" not in companies_data:
            logging.error("No companies found in the response.")
            return

        total_companies = len(companies_data.get("companies", []))
        logging.info(f"Total companies fetched: {total_companies}")

        skipped_count = 0
        processed_count = 0

        # Step 2: Process each company
        for company in companies_data.get("companies", []):
            company_name = company.get("companyName", "Unknown")
            company_id = company.get("companyId")
            
            if not company_id:
                skipped_count += 1
                logging.error(f"Skipping company with missing company_id: {company_name}")
                continue

            # Step 3: Fetch news data
            try:
                news_response = requests.get(base_url_news.format(company_id), headers=headers)
                news_response.raise_for_status()
                news_data = news_response.json()

                if not news_data or "articles" not in news_data:
                    logging.error(f"No articles found for company ID {company_id}")
                    continue
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching news for company ID {company_id}: {e}")
                continue

            # Step 4: Process each article
            articles_to_insert = []
            for article in news_data.get("articles", []):
                company_url = article.get("url")
                if not company_url:
                    logging.error("Missing URL in article. Skipping...")
                    continue

                article_data = fetch_and_clean_article(company_url) or fetch_and_clean_article_pr(company_url)
                if not article_data:
                    log_error(conn, "Scraping Error", "Failed to fetch article text", company_url)
                    continue
                token = None
                for attempt in range(3):  
                    token = get_token()
                    if token:
                        break
                    logging.warning(f"Retry {attempt + 1}: Failed to fetch token.")
                if not token:
                    log_error(conn, "Authentication Error", "Failed to fetch token", company_url)
                    continue
                analysis = analyze_article(token, article_data.get("text"))
                if not analysis:
                    log_error(conn, "Analysis Error", f"Failed to analyze article with company_url: {company_url}", company_url)
                    continue
                articles_to_insert.append((
                    company_id,
                    company_url,
                    article_data.get("title"),
                    article_data.get("text"),
                    analysis.get("Article Summary", "TBD"),
                    analysis.get("Sentiment Score", "N/A"),
                    json.dumps(analysis),
                    datetime.now()
                ))

            # Step 5: Batch insert articles into database
            try:
                if articles_to_insert:
                    insert_into_article_table_bulk(conn, articles_to_insert)
                    logging.info(f"Inserted {len(articles_to_insert)} articles for company {company_name}.")
            except Exception as e:
                logging.error(f"Failed to insert articles for company {company_id}: {e}")

            processed_count += 1
        logging.info(f"Processing summary: {processed_count} companies processed successfully, {skipped_count} skipped.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching companies data: {e}")
        return

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
