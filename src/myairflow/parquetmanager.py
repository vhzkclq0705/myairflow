def convert_csv_to_parquet(timestamp):
    import pandas as pd
    dir_path = "/Users/joon/data/"
    df = pd.read_csv(f"{dir_path}{timestamp}/data.csv")
    df.to_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')

def save_agg_csv(timestamp):
    import pandas as pd
    dir_path = "/Users/joon/data/"
    df = pd.read_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')
    gdf = df.groupby(["name", "value"]).size().reset_index(name="count")
    gdf.to_csv(f"{dir_path}{timestamp}/agg.csv", index=False)
        