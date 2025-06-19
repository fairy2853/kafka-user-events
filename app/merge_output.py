import os
import pandas as pd
import pyarrow.parquet as pq

output_folder = "../output_aggregated"
os.makedirs(output_folder, exist_ok=True)

folder = "../tmp/output_parquet"

files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]

dfs = [pq.read_table(file).to_pandas() for file in files]

df = pd.concat(dfs, ignore_index=True)

df['event_time'] = pd.to_datetime(df['timestamp'], unit='s')
df['event_minute'] = df['event_time'].dt.floor('T')

result = df.groupby(['event_minute', 'event_type']).size().reset_index(name='events_count')
print(result.head(20))

region_count = df.groupby(['event_minute', 'region']).size().reset_index(name='events_count')
print(region_count.head(20))

unique_users = df.groupby('event_minute')['user_id'].nunique().reset_index(name='unique_users_count')
print(unique_users.head(20))


df.to_parquet(os.path.join(output_folder, "merged_logs.parquet"), index=False)

result.to_parquet(os.path.join(output_folder, "events_per_minute_and_type.parquet"), index=False)

unique_users.to_parquet(os.path.join(output_folder, "unique_users_per_minute.parquet"), index=False)

region_count.to_parquet(os.path.join(output_folder, "events_per_minute_and_region.parquet"), index=False)