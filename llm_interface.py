import json
import google.generativeai as genai

class LLMInterface:
    def __init__(self):
        with open('config.json') as f:
            config = json.load(f)
        api_key = config.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in config.json.")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")

    def generate_response(self, context):
        prompt_answer = "Answer: "
        try:
            response = self.model.generate_content(context + prompt_answer)
            return response.text
        except Exception as e:
            print(f"Error generating LLM response: {e}")
            return None
