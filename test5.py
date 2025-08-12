import os
import time
import pandas as pd
import requests
import sys
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"github_repo_log_{timestamp}.txt"
class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, message):
        for f in self.files:
            f.write(message)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()
log_handle = open(log_filename, "w", encoding="utf-8")
sys.stdout = Tee(sys.stdout, log_handle)

TOKENS = [
    os.getenv("GITHUB_TOKEN1"),
    os.getenv("GITHUB_TOKEN2"),
    os.getenv("GITHUB_TOKEN3"),
    os.getenv("GITHUB_TOKEN4"),
]
TOKENS = [t for t in TOKENS if t]
if len(TOKENS) < 2:
    raise ValueError("At least 2 GitHub tokens are required.")
token_index = 0
def rotate_token():
    global token_index
    token_index = (token_index + 1) % len(TOKENS)
def get_headers():
    return {"Authorization": f"token {TOKENS[token_index]}"}

def handle_rate_limit(resp):
    if resp.status_code == 403:
        msg = resp.json().get("message", "").lower()
        if "rate limit" in msg:
            reset_time = resp.headers.get("X-RateLimit-Reset")
            if reset_time:
                wait_time = int(reset_time) - int(time.time()) + 1
            else:
                wait_time = 60
            print(f"Rate limit hit. Waiting {wait_time}s...")
            time.sleep(max(wait_time, 1))
            return True
    return False

def clean_data_for_parquet(data):

    if not data:
        return None
    
    problematic_fields = ['custom_properties']
    cleaned_data = data.copy()

    for field in problematic_fields:
        if field in cleaned_data:
            if cleaned_data is None or cleaned_data[field] == {}:
                del cleaned_data[field]
            else:
                cleaned_data[field] = str(cleaned_data[field])

    for key, value in cleaned_data.items():
        if isinstance(value, list) and not value:
            cleaned_data[key] = None
        elif isinstance(value, dict) and len(value) == 0:
            cleaned_data[key] = None

    return cleaned_data



def get_repo_info(full_name):
    url = f"https://api.github.com/repos/{full_name}"
    while True:
        resp = requests.get(url, headers=get_headers())
        if resp.status_code == 200:
            data= resp.json()
            return clean_data_for_parquet(data)
        elif resp.status_code == 404:
            print(f"Repository {full_name} not found.")
            return None
        elif handle_rate_limit(resp):
            continue
        else:
            print(f"Failed to fetch {full_name}: {resp.status_code}")
            return None

base_folder = "istio_repository"

for root, dirs, files in os.walk(base_folder):
    if root == base_folder:
        continue

    parquet_files = [f for f in files if f.endswith(".parquet") and not f.endswith("_repos.parquet")]
    if not parquet_files:
        continue

    folder_name = os.path.basename(root)
    output_path = os.path.join(base_folder, f"{folder_name}_repos.parquet")

    print(f"Processing folder: {root}")

    all_repo_names = set()
    for file in parquet_files:
        parquet_path = os.path.join(root, file)
        print(f"Reading {file}...")
        df = pd.read_parquet(parquet_path)
        repo_names= df["repository_full_name"].unique()
        all_repo_names.update(repo_names)
        print(f"Found {len(repo_names)} unique repositories in thefile.")

    repo_name= sorted(list(all_repo_names))
    print(f"Total unique repositories in folder: {len(repo_name)}")

    if os.path.exists(output_path):
        existing_df = pd.read_parquet(output_path)
        done_names = set(existing_df["full_name"])
        repo_name = [r for r in repo_name if r not in done_names]
        combined_df = existing_df
    else:
        combined_df= pd.DataFrame()

    print(f"Remaining repositories to process: {len(repo_name)}")

    for idx, repo in enumerate(repo_name,1):
        data = get_repo_info(repo)
        if data:
            new_row = pd.DataFrame([data])
            combined_df = pd.concat([combined_df, new_row], ignore_index=True)
            combined_df.to_parquet(output_path, index=False, compression="snappy")
            pass
        rotate_token()
        time.sleep(0.01) 

    print(f"Processed {len(combined_df)} repositories in {output_path}.")
print("All done.")
log_handle.close()

"""
Reading mtls_disable_v1beta1_a.parquet...
Found 48 unique repositories in thefile.
Total unique repositories in folder: 48
Remaining repositories to process: 48
Processed 48 repositories in istio_repository/v1beta1_repos.parquet.
All done.
Exception ignored in: <__main__.Tee object at 0x7ca5397ebbb0>
Traceback (most recent call last):
  File "/home/kohsuke/aalto/istio-security-research-project/test5.py", line 19, in flush
    f.flush()
ValueError: I/O operation on closed file.
"""