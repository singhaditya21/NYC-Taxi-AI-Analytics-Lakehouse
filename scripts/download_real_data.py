import os
import requests

def download_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # 2026-01 Yellow Taxi (closest full month with data typically available)
    # Note: NYC TLC usually has a 2-month delay. Let's try 2026-01 first, fallback to 2025-12.
    datasets = [
        ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet", "yellow_tripdata.parquet"),
        ("https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv", "taxi_zone_lookup.csv")
    ]

    for url, filename in datasets:
        file_path = os.path.join(data_dir, filename)
        if os.path.exists(file_path):
            print(f"{filename} already exists. Skipping download.")
            continue
            
        print(f"Downloading {url} to {file_path}...")
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Successfully downloaded {filename}.")
            else:
                print(f"Failed to download {filename}. HTTP Status: {response.status_code}")
                # Fallback to 2025-12 if 2026-01 fails
                if "2026-01" in url:
                    fallback_url = url.replace("2026-01", "2025-12")
                    print(f"Trying fallback: {fallback_url}")
                    resp = requests.get(fallback_url, stream=True)
                    if resp.status_code == 200:
                        with open(file_path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=8192):
                                f.write(chunk)
                        print(f"Successfully downloaded fallback {filename}.")
                    else:
                        print(f"Fallback failed too.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_data()
