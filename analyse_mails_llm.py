import os
from deepseek4free import DeepSeekClient


def main():
    # 1. Read your API key
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("Please set DEEPSEEK_API_KEY in your env")

    # 2. Initialize the client
    client = DeepSeekClient(api_key=api_key)

    # 3. Send a simple prompt
    prompt = "Summarize in one sentence: 'The quick brown fox jumps over the lazy dog.'"
    response = client.chat(prompt)

    # 4. Print the result
    print("DeepSeek says ▶", response.text)

if __name__ == "__main__":
    main()
