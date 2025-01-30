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



            format_str = """
            {
                "Company Name": "[Company Name mentioned in the article (e.g., Ligado Networks, Inmarsat)]",
                "Article Title": "[Full title of the news article]",
                "Article Published Timestamp in PT": "[MM/dd/yyyy HH:mm:ss a (convert timestamp to PT if available)]",
                "Article Modified Timestamp in PT": "[MM/dd/yyyy HH:mm:ss a (convert to PT if modified timestamp is available)]",
                "Article News Source": "[Name of the source (e.g., SpaceNews)]",
                "Article Summary": "[A concise summary of the article's main points in 2-3 sentences]",
                "Sentiment Score": "[Numerical sentiment score (based on -10 to 10)]",
                "Sentiment Score Reasoning": {
                    "Reasoning": "[Reasoning behind the sentiment score, e.g., positive developments, lawsuits, etc.]",
                    "Reference": "[Provide the specific sentence or section from the article that supports this reasoning]"
                },
                "Company Valuation Significance": {
                    "Value": "[High/Medium/Low]",
                    "Reference": "[Provide the specific sentence or section from the article that justifies this value]"
                },
                "Company Valuation Significance Reasoning": {
                    "Reasoning": "[Explain why the event impacts the company's valuation significantly or not]",
                    "Reference": "[Provide the specific sentence or section from the article that supports this reasoning]"
                },
                "Explicit Company Impacts": {
                    "Impacts": "[Specific impacts on the company, such as lawsuits, partnerships, market reactions]",
                    "Reference": "[Provide the specific sentence or section from the article that describes these impacts]"
                },
                "Implicit Industry Impacts": {
                    "Impacts": "[Potential impacts on the broader industry, if applicable, based on the event]",
                    "Reference": "[Provide the specific sentence or section from the article that describes these industry impacts]"
                },
                "Implicit Impact Peer Companies": {
                    "Companies": "[List of peer companies potentially impacted by the event]",
                    "Reference": "[Provide the specific sentence or section from the article that mentions or suggests these peer companies]"
                }
            }
            """

prompt for score calculation

            
            prompt = f"""
            Evaluate the generated summary `{generateddata}` against the reference summary `{refdata}` using two key metrics: **Perplexity** and **Accuracy**. 
            
            ### Instructions:
            - Provide numerical scores for both metrics.
            - Present the results in a **table format**.
            - Summarize the findings concisely below the table.
            
            ### Definitions:
            1. **Perplexity**: Measures how well a probability model predicts a sample. Lower values indicate better predictions.
            2. **Accuracy**: Measures the percentage of correct predictions. Higher values indicate better performance.
            
            ### Expected Output Format:
            
            | Metric       | Score | Explanation |
            |-------------|-------|-------------|
            | Perplexity  | X.XX  | How well the generated summary aligns with the reference summary. |
            | Accuracy    | X.XX  | The percentage of correct predictions compared to the reference summary. |
            
            ### Additional Insights:
            1. **Explain why the Perplexity score is `{perplexity_score}` based on the relationship between `{refdata}` and `{generateddata}`.**
            2. **Explain why the Accuracy score is `{accuracy_score}` based on the comparison of `{refdata}` and `{generateddata}`.**
            
            Ensure that the explanations are concise and directly related to the scores provided.
            """









prompt with bertscore and accuracy



            prompt = f"""
            Evaluate the **Generated Summary** `{generateddata}` against the **Reference Summary** `{refdata}` using the following two metrics:
            
            1. **BERT Score** - Measures the semantic similarity between the reference and generated summaries using deep contextual embeddings. A **higher BERT Score** (0-1) indicates better similarity.
            2. **Accuracy** - Measures how closely the generated summary matches the reference summary. A **higher accuracy score** means better alignment.
            
            ### **Instructions:**
            - Compute and provide **numerical scores** for **BERT Score** and **Accuracy**.
            - Present the results in a **table format**.
            - After the table, explain how these scores were determined.
            - Keep explanations concise and relevant to `{refdata}` and `{generateddata}`.
            
            ---
            
            ### **Expected Output Format:**
            
            | Metric      | Score | Explanation |
            |------------|-------|-------------|
            | BERT Score | X.XX  | Explanation of how well the generated summary aligns with the reference summary. |
            | Accuracy   | X.XX  | Explanation of how well the generated summary matches the reference summary. |
            
            ### **Additional Analysis:**
            1. **Explain why the BERT Score was computed for this generated summary.**
            2. **Explain why the Accuracy score was computed for this generated summary.**
            
            ---
            
            Ensure that the responses are structured, precise, and based on the comparison between `{refdata}` and `{generateddata}`.
            """






prompt version 2

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
               - **Explain it in layman’s terms:**  
                 - *Does the generated summary "mean the same thing" as the reference summary, even if different words are used?*
                 - *Would an average person get the same understanding from both summaries?*
            
            2. **Why is the Accuracy Score for this generated summary?**  
               - Analyze the correctness of details in `{generateddata}` compared to `{refdata}`.
               - Mention whether the generated summary missed or added any critical details.
               - **Explain it in layman’s terms:**  
                 - *If someone only read the generated summary, would they still know the key facts?*
                 - *Are any important details missing or changed that might confuse a reader?*
            ---
            
            ### **Guidelines:**
            - **Use actual content** of `{refdata}` and `{generateddata}` to compute the scores.
            - **Avoid generic descriptions**—base all responses on real comparisons.
            - **Provide meaningful justifications** for both scores.
            """






            
