import pandas as pd
import json
import os
import glob


def count_repositories_in_parquet_files(file_pattern):
    files = glob.glob(file_pattern)
    if not files:
        print(f"No files found matching pattern: {file_pattern}")
        return 0

    repo_names = set()

    for file in files:
        try:
            df = pd.read_parquet(file)

            for _, row in df.iterrows():
                if 'repository_full_name' in row:
                    repo_names.add(row['repository_full_name'])
                elif 'api_repository' in row:
                    try:
                        repo_data = json.loads(row['api_repository'])
                        full_name = repo_data.get('full_name')
                        if full_name:
                            repo_names.add(full_name)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Error reading file {file}: {e}")
            continue

    return len(repo_names)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        pattern = sys.argv[1]
        count = count_repositories_in_parquet_files(pattern)
        print(f"Total unique repositories: {count}")
    else:
        print("Usage: python count_repos.py 'Authori_*.parquet'")