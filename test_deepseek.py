import requests
import os

API_KEY = os.getenv("OPENROUTER_API_KEY")  # or paste directly (not recommended)

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "HTTP-Referer": "https://yourproject.com",  # can be fake
    "Content-Type": "application/json",
}

data = {
    "model": "groq/mixtral-8x7b-32768",
    "messages": [
        {"role": "system", "content": "You are an expert email analyst."},
        {"role": "user", "content": "Summarize this email:\n\nHi Paul, we received your report and we'll review it by Friday. Let me know if you have any comments."}
    ]
}

response = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data)
print(response.json()["choices"][0]["message"]["content"])

