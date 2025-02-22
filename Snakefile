import calendar
from omegaconf import OmegaConf
import hydra

conda: "environment.yaml"
configfile: "snake_config.yaml"

# load hydra configuration
# Load Hydra config
cfg = OmegaConf.load("config.yaml")  # Directly load YAML as a dict

# Define parameters
pollutant_list = config.get("pollutant_list")
years = [int(y) for y in config.get("years")]
months = [int(m) for m in config.get("months")]
yyyymm_list = [f"{y}{m:02d}" for y in years for m in months]
yyyymmdd_list = [
    f"{y}{m:02d}{d:02d}"
    for y in years
    for m in months
    for d in range(1, calendar.monthrange(y, m)[1] + 1)
]

# Rule to generate all required files
rule all:
    input:
        expand(
            cfg.filename + ".zip", 
            pollutant=pollutant_list, 
            yyyymm=yyyymm_list
        )

# Rule to download air pollution data
rule download_air_pollution:
    output:
        cfg.filename + ".zip"
    shell:
        """
        echo "Downloading geotiff for pollutant {wildcards.pollutant} and yyyymm {wildcards.yyyymm}"
        python src/download_air_pollution.py pollutant={wildcards.pollutant} yyyymm={wildcards.yyyymm}
        """
