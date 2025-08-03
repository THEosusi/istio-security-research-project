import requests
import json
import time 
import itertools
import string

url_template = 'https://api.github.com/search/code?q=%22istio.io%22%20%22kind%3A%20AuthorizationPolicy%22+language:YAML+filename:{name}&page={page}&per_page=100'
headers = {
  'Authorization': 'Token put your token'
}
characters = string.ascii_lowercase + string.digits + "-"
limited_characters = string.ascii_lowercase + string.digits
successful_responses = 0

conservative_mode = False

def handle_secondary_rate_limit(response):

    global conservative_mode

    if response.status_code == 403:
        error_test = response.text.lower()
        if 'secondary rate limit' in error_test:
            print("Secondary rate limit reached, entering conservative mode.")
            conservative_mode = True

            retry_after = response.headers.get('Retry-After', 120)
            wait_time = int(retry_after) + 10

            print(f"Waiting for {wait_time} seconds before retrying.")
            time.sleep(wait_time)
            return True
    return False

for char1 in limited_characters:
    full_name_exclude= set()  # Reset for each char1
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
                    print(f"No more results for this filename, stopping at page {page}.")
                    break
            
                for item in items:
                    repo_name = item["repository"]["full_name"]
                    full_name_exclude.add(repo_name)

                    if len(items) > 30:
                        time.sleep(0.02)

        else:
            successful_responses += 1
            print(f"Failed to fetch data for filename {filename}, page {page}")
            print(f"Response headers: {response.headers}, status code: {response.status_code}, response text: {response.text}, url: {url}")
            break

        if conservative_mode:
            time.sleep(5)
        else:
            time.sleep(2.2)

        if successful_responses >= 6:
            print("Reached failure threshold (6), stopping execution.")
            break
    
    output_data = {
        "repositories": list(full_name_exclude)
    }
    output_filename = f"Authori_{char1}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"Unique Full Names count for {char1}: {len(full_name_exclude)}")
    print(f"Results saved to {output_filename}")

    if successful_responses >= 6:
        break

print("Processing completed.")
print(f"Total successful responses: {successful_responses}")

             



# Loop over the first character (char1)
for char1 in limited_characters:
    full_names_exclude = set()  # Reset for each char1
    print(f"Processing char1: {char1}")

    # Loop over the second character (char2)
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
                    print(f"No more results for this filename, stopping at page {page}.")
                    break

                for item in items:
                    repo_name = item["repository"]["full_name"]
                    full_names_exclude.add(repo_name)
            else:
                successful_responses += 1
                print(f"Failed to fetch data for filename {filename}, page {page}, {response.headers} ,{response} url{url}")
                break
            time.sleep(10)
        time.sleep(11)

        if successful_responses >= 6:
            print("Reached failure threshold (6), stopping execution.")
            break


    output_data = {
        "repositories": list(full_names_exclude)
    }

    output_filename = f"Authori_{char1}.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"Unique Full Names count for {char1}: {len(full_names_exclude)}")
    print(f"Results saved to {output_filename}")


    if successful_responses >= 6:
        break

    time.sleep(7)