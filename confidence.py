def calculate_score(source_text,llm_response):
    prompt = f"""
    Evaluate the similarity between the following texts based on meaning, factual accuracy, and relevance.

    Source Text:
    "{source_text}"

    LLM Response:
    "{llm_response}"

    Provide a relevance score from 0 to 100, where:
    - 0 means completely irrelevant
    - 100 means fully aligned in meaning

    Output only the score.
    """
    payload = f"{prompt}"
    response = payload
    
    return response
