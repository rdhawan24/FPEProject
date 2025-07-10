import os
import requests

# Get API Key from environment or paste directly (not recommended for production)
API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    raise ValueError(" Set the OPENROUTER_API_KEY environment variable.")

# Define headers
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "X-Title": "email-analysis",  # optional: set to describe your use case
}

# Define payload
data = {
    "model": "mistralai/mistral-7b-instruct",  # or another OpenRouter-supported model
    "messages": [
        {"role": "system", "content": "You are an expert email analyst."},
        {"role": "user", "content": "Summarize this email:\n\nRob, Markets. my marks show how we believe the market would trade if the market were open today, considering neighboring markets, transmission/wheeling costs, and fuel costs. More of a expected liquidation value vs. the current Ontario market players bids and offers.
            The market on 3yr 7x24 is being shown by a broker: $46-$48, and on the wholesale side I doubt the bid is actually there, or if so it is only a piece deep (50MW). I"}
    ]
}

# Send the request
response = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data)

# Check response
try:
    response.raise_for_status()
    result = response.json()
    print(result["choices"][0]["message"]["content"])
except requests.exceptions.HTTPError as e:
    print(" HTTP error:", e)
    print("Full response:", response.text)
except Exception as e:
    print(" General error:", e)
    print("Full response (if any):", response.text)

