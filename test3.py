import requests
import json
import time
import string
import os
import pandas as pd

# token read from environment variables, can put more than two tokens
# token 1 sleep 10 seconds, token 2 sleep 5 seconds 4 hours
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

def flatten_item_for_parquet(item):
    """Flatten the item for easier handling in Parquet."""
    flattended = {}
    for key, value in item.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flattended[f"{key}_{sub_key}"] = sub_value
        else:
            flattended[key] = value
    return flattended

def append_item_to_parquet(filename, item):
    """Append a flattened item to a Parquet file."""
    try:
        flattened_item = flatten_item_for_parquet(item)
        new_df = pd.DataFrame([flattened_item])

        if os.path.exists(filename):
            existing_df = pd.read_parquet(filename)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        combined_df.to_parquet(filename, index=False, compression='snappy')
        return True
    except Exception as e:
        print(f"Error appending item to Parquet file {filename}: {e}")
        return False
    


# seeting for characters
characters = string.ascii_lowercase + string.digits + "-"
limited_characters = "abc"
failed_responses = 0

url_template = (
    'https://api.github.com/search/code?q=%22istio.io%22%20%22kind%3A%20AuthorizationPolicy%22'
    '+language:YAML+filename:{name}&page={page}&per_page=100'
)

for char1 in limited_characters:
    full_name_exclude = set()
    saved_count = 0
    print(f"\nProcessing char1: {char1}")

    for char2 in characters:
        filename = f"{char1}{char2}"
        print(f"  Checking filename: {filename}")

        for page in range(1, 11):
            while True:
                url = url_template.format(name=filename, page=page)
                headers = get_headers()
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    items = data.get("items", [])

                    if not items:
                        print(f"    No more results for filename {filename}, stopping at page {page}.")
                        break

                    for item in items:
                        repo_name = item["repository"]["full_name"]

                        if repo_name not in full_name_exclude:
                            full_name_exclude.add(repo_name)
                        # Append item to Parquet file
                            if append_item_to_parquet(f"Authori_{char1}.parquet", item):
                                saved_count += 1
                    rotate_token()
                    time.sleep(10)
                    break
                elif handle_rate_limit(response):
                    continue
                else:
                    failed_responses += 1
                    print(f"    Failed: filename {filename}, page {page}")
                    print(f"    Status: {response.status_code}, Response: {response.text}")
                    break

        if failed_responses >= 6:
            print("Too many failures, stopping.")
            break

    print(f"  completed {char1}:{saved_count} {len(full_name_exclude)} unique repositories found.")

    if  failed_responses >= 6:
        break

print("Done.")