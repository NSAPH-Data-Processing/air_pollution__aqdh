import zipfile
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # Define the paths
    replacements = {
        "pollutant_code": cfg.pollutant_code, #cfg.pollutant_code[cfg.pollutant],
        "yyyy": cfg.yyyy,
        "mm": cfg.mm
    }
    zip_filename = cfg.zip_filename.format(**replacements)

    zip_file_path = f"data/input/raw/{zip_filename}.zip"
    extract_to_path = f"data/intermediate/{zip_filename}/"

    # Unzip the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_path)

    print(f"Unzipped {zip_file_path} to {extract_to_path}")

if __name__ == "__main__":
    main()