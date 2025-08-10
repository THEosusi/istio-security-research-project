import requests
import json
import time
import string
import os
import pandas as pd
from urllib.parse import quote
import sys
from datetime import datetime

# simple log
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"istio_security_log_{timestamp}.txt"

class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, message):
        for file in self.files:
            file.write(message)
            file.flush()

    def flush(self):
        for file in self.files:
            file.flush()

log_handle = open(log_filename, 'w',encoding='utf-8')
sys.stdout = Tee(sys.stdout, log_handle)

# token read from environment variables, can put more than two tokens
# token 1 sleep 10 seconds, token 2 sleep 5 seconds 4 hours
TOKENS = [
    os.getenv("GITHUB_TOKEN1"),
    os.getenv("GITHUB_TOKEN2"),
    os.getenv("GITHUB_TOKEN3"),
    os.getenv("GITHUB_TOKEN4"),
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
            if retry_after:
                wait_time = int(retry_after)
            elif "X-RateLimit-Reset" in response.headers:
                reset_time = int(response.headers["X-RateLimit-Reset"])
                wait_time = reset_time - int(time.time()) + 1
            else:
                wait_time = 60  # default fallback
            print(f"Rate limit exceeded. Sleeping for {wait_time} seconds.")
            time.sleep(max(wait_time, 1))
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
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)

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
    
base_queries = {
    #mTLS-v1
    "mtls_strict_v1": '"security.istio.io/v1" "kind: PeerAuthentication" "mode: STRICT"',
    "mtls_permissive_v1": '"security.istio.io/v1" "kind: PeerAuthentication" "mode: PERMISSIVE"',
    "mtls_disable_v1": '"security.istio.io/v1" "kind: PeerAuthentication" "mode: DISABLE"',
    #mTLS-v1beta1
    "mtls_strict_v1beta1": '"security.istio.io/v1beta1" "kind: PeerAuthentication" "mode: STRICT"',
    "mtls_permissive_v1beta1": '"security.istio.io/v1beta1" "kind: PeerAuthentication" "mode: PERMISSIVE"',
    "mtls_disable_v1beta1": '"security.istio.io/v1beta1" "kind: PeerAuthentication" "mode: DISABLE"',
    #Authentication-v1
    "peer_auth_v1": '"security.istio.io/v1" "kind: PeerAuthentication"',
    "req_auth_v1": '"security.istio.io/v1" "kind: RequestAuthentication"',
    #Authentication-v1beta1
    "peer_auth_v1beta1": '"security.istio.io/v1beta1" "kind: PeerAuthentication"',
    "req_auth_v1beta1": '"security.istio.io/v1beta1" "kind: RequestAuthentication"',
    # Authorization-v1
    "any_authz_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy"',
    "http_traffic_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "methods:"',
    "tcp_traffic_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "ports:"',
    "jwt_authz_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "requestPrincipals:"',
    "provider_authz_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "provider:"',
    "ingress_authz_ip_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "ipBlocks:"',
    "ingress_authz_remote_ip_v1": '"security.istio.io/v1" "kind: AuthorizationPolicy" "remoteIpBlocks:"',
    # Authorization-v1beta1
    "any_authz_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy"',
    "http_traffic_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "methods:"',
    "tcp_traffic_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "ports:"',
    "jwt_authz_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "requestPrincipals:"',
    "provider_authz_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "provider:"',
    "ingress_authz_ip_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "ipBlocks:"',
    "ingress_authz_remote_ip_v1beta1": '"security.istio.io/v1beta1" "kind: AuthorizationPolicy" "remoteIpBlocks:"',
    
    # mTLS-v1alpha1
    "mtls_strict_v1alpha1": '"authentication.istio.io/v1alpha1" "kind: Policy" "STRICT"',
    "mtls_permissive_v1alpha1": '"authentication.istio.io/v1alpha1" "kind: Policy" "PERMISSIVE"',
    # authentication-v1alpha1
    "peer_auth_v1alpha1": '"authentication.istio.io/v1alpha1" "kind: Policy" "peers"',
    "req_auth_v1alpha1": '"authentication.istio.io/v1alpha1" "kind: Policy" "origins"',
    # authorization-v1alpha1
    "cluster_authz_v1alpha1": '"rabc.istio.io/v1alpha1"',
    # all
    "all_all_all": '"istio.io/v1"',
}

def create_search_url(query_test, filename_pattern, page=1):
    """
    Create a GitHub search URL for the given query and filename pattern.
    """
    query = quote(query_test)
    url = (
        f'https://api.github.com/search/code?q={query}+language:YAML+filename:{filename_pattern}&page={page}&per_page=100'
    )
    return url




# seeting for characters
characters = string.ascii_lowercase + string.digits + "-"
limited_characters = string.ascii_lowercase + string.digits
failed_responses = 0

#create main output directory if it doesn't exist
out_base_dir= "istio_security_data"
os.makedirs(out_base_dir, exist_ok=True)

for query_key, query_test in base_queries.items():
    print(f"\n{'='*60}")
    print(f"Processing query: {query_key}")
    print(f"Query: {query_test}")
    print(f"{'='*60}\n")
    
    query_failed_responses = 0
    full_name_exclude = set()
    saved_count = 0
    for char1 in limited_characters:
        print(f"\nProcessing char1: {char1} for query {query_key}")

        for char2 in characters:
            filename = f"{char1}{char2}"
            print(f"  Checking filename: {filename}")

            no_more_results = False
            filename_total_items = 0

            for page in range(1, 11):
                time.sleep(0.1)
                if no_more_results:
                    break
                while True:
                    url = create_search_url(query_test, filename, page)
                    headers = get_headers()
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        data = response.json()
                        items = data.get("items", [])

                        if not items:
                            print(f"    No more results for filename {filename}, stopping at page {page}.")
                            no_more_results = True
                            rotate_token()
                            time.sleep(1.6)
                            break
                        
                        filename_total_items += len(items)
                        for item in items:
                            time.sleep(0.005)
                            repo_name = item["repository"]["full_name"]

                            if repo_name not in full_name_exclude:
                                full_name_exclude.add(repo_name)
                            #create folder for the query if it doesn't exist
                                category=query_key.split('_')[0] #e.g., mtls, authz
                                version=query_key.split('_')[-1] #e.g., v1, v1beta1
                            #create output path:
                                folder_path = os.path.join(out_base_dir, category, version)
                                output_filename= os.path.join(folder_path, f"{query_key}_{char1}.parquet")
                                
                                if append_item_to_parquet(output_filename, item):
                                    saved_count += 1

                        rotate_token()
                        time.sleep(1.6)
                        break
                    elif handle_rate_limit(response):
                        continue
                    else:
                        query_failed_responses += 1
                        print(f"    Failed: filename {filename}, page {page}")
                        print(f"    Status: {response.status_code}, Response: {response.text}")
                        break
            
            if filename_total_items > 0:
                print(f"  {filename}: Found {filename_total_items} items.")
            else:
                print(f"  {filename}: No items found.")

            if query_failed_responses >= 10:
                print("Too many failures in one query, stopping.")
                break

        print(f"Query {query_key} completed with {len(full_name_exclude)} unique repositories found.")

print("Done.")

