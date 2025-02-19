import zipfile

def main():
    # Define the paths
    zip_file_path = 'data/input/raw/aqdh-o3-concentrations-contiguous-us-1-km-v1-10-2000-2016-200001-geotiff.zip'
    extract_to_path = 'data/intermediate/'

    # Unzip the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_path)

    print(f"Unzipped {zip_file_path} to {extract_to_path}")

if __name__ == "__main__":
    main()