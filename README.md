            if 'choices' in json_res and 'message' in json_res['choices'][0]:
                answer = json_res['choices'][0]['message']['content']
                try:
                    return json.loads(answer)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON from answer: {e}. Answer: {answer}")
                    return None
