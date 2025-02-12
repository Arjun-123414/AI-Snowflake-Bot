# groq_utils.py
from groq import Groq
from typing import List, Dict, Tuple
from config import GROQ_API_KEY

def get_groq_response(prompt: str, messages: List[Dict[str, str]]) -> Tuple[str, int]:
    """Generate a response using the Groq API and return the response along with token usage."""
    try:
        # Initialize the Groq client
        client = Groq(api_key=GROQ_API_KEY)

        # Add the user's prompt to the messages
        messages.append({"role": "user", "content": prompt})

        # Call the Groq API
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",  # Replace with desired model
            messages=messages,
            temperature=0.7,  # Adjust as needed
            max_tokens=1024,  # Adjust as needed
            top_p=1,  # Adjust as needed
            stop=None,  # Adjust as needed
            stream=False,  # Set to True for streaming
        )

        # Extract the response content and token usage
        response_content = response.choices[0].message.content
        token_usage = response.usage.total_tokens  # Total tokens used in the request

        return response_content, token_usage

    except Exception as e:
        return f"Error: {str(e)}", 0