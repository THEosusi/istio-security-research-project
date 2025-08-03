import requests
import json
import time
import string
import os

# token read from environment variables, can put more than two tokens
# token 1 sleep 10 seconds, token 2 sleep 5 seconds
TOKENS = [
    os.getenv("GITHUB_TOKEN1"),
    os.getenv("GITHUB_TOKEN2"),
    os.getenv("GITHUB_TOKEN3"),
]
TOKENS = [token for token in TOKENS if token]

if len(TOKENS) < 2:
    raise ValueError("At least 2 GitHub tokens must be set in environment variables.")

# rotetin index
token_index = 0

def rotate_token():
    global token_index
    token_index = (token_index + 1) % len(TOKENS)

def get_headers():
    return {
        "Authorization": f"token {TOKENS[token_index]}"
    }

def handle_rate_limit(response):
    if response.status_code == 403:
        msg = response.json().get("message", "").lower()
        if "rate limit exceeded" in msg or "secondary rate limit" in msg:
            retry_after = response.headers.get("Retry-After")
            wait_time = int(retry_after) if retry_after else 120
            print(f"Rate limit exceeded. Sleeping for {wait_time} seconds.")
            time.sleep(wait_time)
            return True
    return False

# seeting for characters
characters = string.ascii_lowercase + string.digits + "-"
limited_characters = "abc"
successful_responses = 0

url_template = (
    'https://api.github.com/search/code?q=%22istio.io%22%20%22kind%3A%20AuthorizationPolicy%22'
    '+language:YAML+filename:{name}&page={page}&per_page=100'
)

for char1 in limited_characters:
    full_name_exclude = set()
    print(f"\nProcessing char1: {char1}")

    for char2 in characters:
        filename = f"{char1}{char2}"
        print(f"  Checking filename: {filename}")

        for page in range(1, 11):
            url = url_template.format(name=filename, page=page)
            headers = get_headers()
            response = requests.get(url, headers=headers)

            # rotate token each time we make a request
            rotate_token()

            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])

                if not items:
                    print(f"    No more results for filename {filename}, stopping at page {page}.")
                    break

                for item in items:
                    repo_name = item["repository"]["full_name"]
                    full_name_exclude.add(repo_name)

            else:
                if handle_rate_limit(response):
                    continue  # after sleeping, retry the same page
                else:
                    successful_responses += 1
                    print(f"    Failed: filename {filename}, page {page}")
                    print(f"    Status: {response.status_code}, Response: {response.text}")
                    break

            time.sleep(5)  # interval for API requests code

        if successful_responses >= 6:
            print("Too many failures, stopping.")
            break

    # preserve results
    output_data = {"repositories": list(full_name_exclude)}
    output_filename = f"Authori_{char1}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"  Saved {len(full_name_exclude)} repos to {output_filename}")

    if successful_responses >= 6:
        break

print("Done.")