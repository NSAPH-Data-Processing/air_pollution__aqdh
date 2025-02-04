import hydra
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    url = f"{cfg.url}{cfg.filename}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Use stream=True to download the content in chunks
    response = session.get(url, headers=headers, stream=True)

    if response.status_code == 200:
        # Get total file size from headers
        total_size = int(response.headers.get('content-length', 0))
        chunk_size = 1024  # 1 KB per chunk
        output_filename = f"data/input/raw/{cfg.filename}"
        
        with open(output_filename, "wb") as f, tqdm(
            total=total_size, unit='B', unit_scale=True, desc="Downloading"
        ) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    pbar.update(len(chunk))
        print("Download completed successfully.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

if __name__ == "__main__":
    main()

# import hydra
# import urllib.request
# import wget
# from tqdm import tqdm


# def tqdm_bar(current, total, width=80):
#     """
#     # Custom progress bar function using tqdm.
#     # This function will be called repeatedly by wget.download() with:
#     #   current: number of bytes downloaded so far
#     #   total: total number of bytes (from Content-Length)
#     """
#     # Initialize the progress bar if it hasn't been already
#     if not hasattr(tqdm_bar, 'pbar'):
#         tqdm_bar.pbar = tqdm(total=total, unit='B', unit_scale=True, desc="Downloading")
#     # Update the progress bar by the difference
#     tqdm_bar.pbar.update(current - tqdm_bar.pbar.n)
#     # Close the progress bar when done
#     if current >= total:
#         tqdm_bar.pbar.close()

# @hydra.main(config_path="../conf", config_name="config", version_base=None)
# def main(cfg):
#     url = f"{cfg.url}{cfg.filename}"
#     output_filename = f"data/input/raw/{cfg.filename}"
    
#     # Set up a custom opener with a custom User-Agent header.
#     opener = urllib.request.build_opener()
#     opener.addheaders = [
#         (
#             "User-Agent",
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/58.0.3029.110 Safari/537.3"
#         )
#     ]
#     urllib.request.install_opener(opener)

#     # Download the file using wget.download() with our custom progress bar
#     wget.download(url, out=output_filename, bar=tqdm_bar)
#     print("\nDownload completed successfully.")

# if __name__ == "__main__":
#     main()


import hydra
from pySmartDL import SmartDL

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # Build the URL using the configuration values.
    url = f"{cfg.url}{cfg.filename}"
    
    request_args = {
        'headers': {
            'User-Agent': (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/58.0.3029.110 Safari/537.3"
            )
        }
    }
    
    # Set the destination path using the filename from your config.
    output_filename = f"data/input/raw/{cfg.filename}"
    
    # Create the SmartDL object.
    # The requests_params parameter is passed to the underlying requests call,
    # so your custom headers are used.
    obj = SmartDL(
        url,
        output_filename,
        progress_bar=True,  # Enables a built-in progress bar.
        request_args=request_args,  # Pass your headers here.
        timeout=30,
        verify=False  # Disable certificate verification for testing purposes
    )
    
    # Start the download (blocking call).
    obj.start(blocking=True)
    
    # Check if the download was successful.
    if obj.isSuccessful():
        print("Download completed successfully.")
    else:
        print("Download failed. Exception:", obj.get_exception())

if __name__ == "__main__":
    main()