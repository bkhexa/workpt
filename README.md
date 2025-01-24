            if 'choices' in json_res and 'message' in json_res['choices'][0]:
                answer = json_res['choices'][0]['message']['content']
                try:
                    return json.loads(answer)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON from answer: {e}. Answer: {answer}")
                    return None





text read

def parse_text_to_json(text):
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

parsed_data = parse_text_to_json(text_data)
json_output = json.dumps(parsed_data, indent=4)
