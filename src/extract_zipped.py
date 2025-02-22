import zipfile
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # Define the paths
    replacements = {
        "pollutant": cfg.pollutant,
        "yyyymm": f"{cfg.year}{cfg.month:02d}"
    }
    filename = cfg.filename.format(**replacements)

    zip_file_path = f"data/input/raw/{filename}.zip"
    extract_to_path = f"data/intermediate/{filename}/"

    # Unzip the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_path)

    print(f"Unzipped {zip_file_path} to {extract_to_path}")

if __name__ == "__main__":
    main()