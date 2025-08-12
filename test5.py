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

def get_repo_info(full_name):
    url = f"https://api.github.com/repos/{full_name}"
    while True:
        resp = requests.get(url, headers=get_headers())
        if resp.status_code == 200:
            return resp.json()
        elif handle_rate_limit(resp):
            rotate_token()
            continue
        else:
            print(f"Failed to fetch {full_name}: {resp.status_code}")
            return None

input_folder = "./"  
for file in os.listdir(input_folder):
    if file.endswith(".parquet") and not file.endswith("_repos.parquet"):
        parquet_path = os.path.join(input_folder, file)
        output_path = parquet_path.replace(".parquet", "_repos.parquet")

        print(f"\n=== Processing {file} ===")
        df = pd.read_parquet(parquet_path)
        repo_names = sorted(df["repository_full_name"].unique())

        if os.path.exists(output_path):
            existing_df = pd.read_parquet(output_path)
            done_names = set(existing_df["full_name"])
            repo_names = [r for r in repo_names if r not in done_names]
            combined_df = existing_df
        else:
            combined_df = pd.DataFrame()

        print(f"Total repos to fetch: {len(repo_names)}")

        for idx, repo in enumerate(repo_names, 1):
            data = get_repo_info(repo)
            if data:
                new_row = pd.DataFrame([data])
                combined_df = pd.concat([combined_df, new_row], ignore_index=True)
            
                combined_df.to_parquet(output_path, index=False, compression="snappy")
            rotate_token()
            time.sleep(1.2)  

        print(f"Saved {len(combined_df)} repo details to {output_path}")

print("All done.")