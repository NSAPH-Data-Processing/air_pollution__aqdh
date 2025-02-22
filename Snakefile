import calendar
from omegaconf import OmegaConf
import hydra

conda: "environment.yaml"
configfile: "snake_config.yaml"

# Load Hydra config
cfg = OmegaConf.load("conf/config.yaml")  # Directly load YAML as a dict

# Define parameters
pollutant_list = config.get("pollutant_list")
yyyy_list = config.get("yyyy_list")
mm_list = config.get("mm_list")
# yyyymmdd_list = [
#     f"{y}{m:02d}{d:02d}"
#     for y in config.get("years")
#     for m in config.get("months")
#     for d in range(1, calendar.monthrange(y, m)[1] + 1)
# ]

# Rule to generate all required files
rule all:
    input:
        expand(
            "data/input/raw/" + cfg.zip_filename + ".zip", 
            pollutant=pollutant_list, 
            yyyy=yyyy_list,
            mm=mm_list
        )

# Rule to download air pollution data
rule download_air_pollution:
    output:
        "data/input/raw/" + cfg.zip_filename + ".zip"
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
        echo "Downloading geotiff for pollutant {wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}"
        python src/download_air_pollution.py pollutant={wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """
