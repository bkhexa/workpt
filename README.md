            if 'choices' in json_res and 'message' in json_res['choices'][0]:
                answer = json_res['choices'][0]['message']['content']
                try:
                    return json.loads(answer)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON from answer: {e}. Answer: {answer}")
                    return None





text read

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
                    elif line.startswith("- "):
                        url = line[2:].strip()
                        if url.lower() != "none":
                            current_company["urls"].append(url)
                    elif line.startswith("No URLs found."):
                        current_company["urls"] = [] 
            
                if current_company:
                    companies.append(current_company)
            
                return companies
            
            file_path = "text.txt"
            with open(file_path, "r") as file:
                text_data = file.read()
            parsed_data = parse_text_to_dataframe(text_data)
            df = pd.DataFrame(parsed_data)
            exploded_df = df.explode("urls").reset_index(drop=True)
            exploded_df = exploded_df[exploded_df["urls"].notna()]





df into table

            def insert_dataframe_to_table(df,conn):
                table_name = "company_master_details"
                company_short_name = "unknown"
                try:
                    # Prepare the insert query
                    insert_query = f"""
                        INSERT INTO {table_name} (company_name,company_short_name, company_id)
                        VALUES (%s, %s,%s)
                        ON CONFLICT (company_id) DO NOTHING;
                    """
                    
                    # Connect to the database
                    cursor = conn.cursor()
                    
                    # Iterate over DataFrame rows
                    for _, row in df.iterrows():
                        cursor.execute(insert_query, (row["company_name"], row["company_id"]))
                    
                    # Commit the transaction
                    conn.commit()
                    print("Data inserted successfully.")
                except Exception as e:
                    print(f"Error: {e}")
                    conn.rollback()  # Rollback in case of error
                finally:
                    cursor.close()




            content = f"""
            I own an investment in one of the companies mentioned in the news article. Based on the article content provided below, analyze it and provide the output in the exact dictionary format outlined below:
            
            {format_str}
            
            News Article Content:
            {article_text}
            
            Please ensure the analysis is:
            - Factually accurate and aligned with the article content.
            - Contextually relevant to investment and business implications.
            - Provided in the exact format specified above.
            - If any field does not have enough data in the article, mention it as "Not Applicable".
            """
