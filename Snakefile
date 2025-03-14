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

rule all:
    input:
        expand(
            f"data/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{{yyyy}}.parquet",
            yyyy=yyyy_list
        )

rule download_air_pollution:
    output:
        "data/input/raw/" + cfg.zip_filename + ".zip"
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
        echo "Downloading geotiff for pollutant {wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}"
        python src/download_air_pollution.py pollutant_code={wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

rule unzip_air_pollution:
    input:
        "data/input/raw/" + cfg.zip_filename + ".zip"
    output:
        directory("data/intermediate/" + cfg.zip_filename + "/")
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
         echo "Unzipping {input} -> {output}"
         python src/extract_zipped.py pollutant_code={wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

rule aggregate_air_pollution:
    input:
        lambda wildcards: "data/intermediate/" + cfg.zip_filename.format(pollutant_code=cfg.pollutant_code_map[wildcards.pollutant], yyyy=wildcards.yyyy, mm=wildcards.mm) + "/"
    output:
        f"data/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet"
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
        echo "Aggregating {input}"
        PYTHONPATH='.' python src/aggregate_air_pollution.py pollutant={wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

rule merge_air_pollution:
    input:
        lambda wildcards: expand(
            f"data/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet",
            pollutant=pollutant_list,
            yyyy=wildcards.yyyy,
            mm=mm_list
        )
    output:
        f"data/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{{yyyy}}.parquet"
    shell:
        """
        echo "Merging {input}"
        python src/merge_pollutants.py yyyy={wildcards.yyyy}
        """