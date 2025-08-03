import requests
import json
import time
import string
import os

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN environment variable is not set.")

url_template = 'https://api.github.com/search/code?q=%22istio.io%22%20%22kind%3A%20AuthorizationPolicy%22+language:YAML+filename:{name}&page={page}&per_page=100'
headers = {
    'Authorization': f'Token {GITHUB_TOKEN}'
}

characters = string.ascii_lowercase + string.digits + "-"
limited_characters = "abc"
successful_responses = 0

def handle_rate_limit(response):
    if response.status_code == 403:
        error_msg = response.json().get('message', '').lower()
        if 'rate limit exceeded' in error_msg or 'secondary rate limit' in error_msg:
            retry_after = response.headers.get('Retry-After')
            wait_time = int(retry_after) if retry_after else 120
            print(f"Rate limit exceeded. Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time + 10)
            return True
    return False

for char1 in limited_characters:
    full_name_exclude = set()
    print(f"Processing char1: {char1}")

    for char2 in characters:
        filename = f"{char1}{char2}"
        print(f"Checking filename: {filename}")

        for page in range(1, 11):
            url = url_template.format(name=filename, page=page)
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])

                if not items:
                    print(f"No more results for filename {filename}, stopping at page {page}.")
                    break

                for item in items:
                    repo_name = item["repository"]["full_name"]
                    full_name_exclude.add(repo_name)

            else:
                if handle_rate_limit(response):
                    continue
                else:
                    successful_responses += 1
                    print(f"Failed at filename {filename}, page {page}, status {response.status_code}")
                    print(f"Response: {response.text}")
                    break

            time.sleep(10)  

        if successful_responses >= 6:
            print("Too many failures, stopping.")
            break

    output_data = {"repositories": list(full_name_exclude)}
    output_filename = f"Authori_{char1}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"{char1}: Found {len(full_name_exclude)} repos. Saved to {output_filename}")

    if successful_responses >= 6:
        break

print("Done.")