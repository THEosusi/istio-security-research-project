import pandas as pd


file_path = "./istio_repository/peer/v1alpha1/peer_auth_v1alpha1_e.parquet"

df = pd.read_parquet(file_path)

total_count = len(df)

first_rows = df.head(2)

print(f"Total items: {total_count}")
print("\nFirst 1-2 items:")
print(first_rows)