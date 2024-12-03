import google.generativeai as genai
import json
import os

class LLMInterface:
    def __init__(self, api_key=None):
        if not api_key:
            # Load the API key from the config file
            try:
                with open('config.json') as config_file:
                    config = json.load(config_file)
                api_key = config.get("GEMINI_API_KEY")
                if not api_key:
                    raise ValueError("GEMINI_API_KEY not found in config.json.")
            except FileNotFoundError:
                print("config.json file not found.")
                exit(1)
            except json.JSONDecodeError:
                print("Error decoding config.json. Ensure it is valid JSON.")
                exit(1)
            except Exception as e:
                print(f"Error: {e}")
                exit(1)

        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")

    def generate_response(self, context, prompt="Answer: "):
        try:
            full_prompt = context + prompt
            response = self.model.generate_content(full_prompt)
            return response.text
        except Exception as e:
            print(f"Error generating response: {e}")
            return None
