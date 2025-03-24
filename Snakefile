import calendar
from omegaconf import OmegaConf
import hydra

conda: "environment.yaml"
configfile: "snake_config.yaml"

# Load Hydra config
#cfg = OmegaConf.load("conf/config.yaml")  # Directly load YAML as a dict
with hydra.initialize(version_base=None, config_path="conf"):
    cfg = hydra.compose(config_name="config")

# Define parameters
pollutant_list = config.get("pollutant_list") #potential conflict with pollutant_list = cfg.pollutant_code_map.keys()
yyyy_list = config.get("yyyy_list")
mm_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

rule all:
    input:
        expand(
            f"{cfg.datapaths.base_path}/output/yearly/air_pollution__aqdh__{cfg.shp_id}_yearly__{{yyyy}}.parquet",
            yyyy=yyyy_list
        )

rule download_air_pollution:
    output:
        f"{cfg.datapaths.base_path}/input/raw/" + cfg.zip_filename + ".zip"
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
        echo "Downloading geotiff for pollutant {wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}"
        python src/download_air_pollution.py pollutant_code={wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

rule unzip_air_pollution:
    input:
        f"{cfg.datapaths.base_path}/input/raw/" + cfg.zip_filename + ".zip"
    output:
        directory(f"{cfg.datapaths.base_path}/intermediate/" + cfg.zip_filename + "/")
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    shell:
        """
         echo "Unzipping {input} -> {output}"
         python src/extract_zipped.py pollutant_code={wildcards.pollutant_code} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

rule aggregate_air_pollution:
    input:
        lambda wildcards: f"{cfg.datapaths.base_path}/intermediate/" + cfg.zip_filename.format(pollutant_code=cfg.pollutant_code_map[wildcards.pollutant], yyyy=wildcards.yyyy, mm=wildcards.mm) + "/"
    output:
        f"{cfg.datapaths.base_path}/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet"
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
            f"{cfg.datapaths.base_path}/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet",
            pollutant=pollutant_list,
            yyyy=wildcards.yyyy,
            mm=mm_list
        )
    output:
        f"{cfg.datapaths.base_path}/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{{yyyy}}.parquet"
    shell:
        """
        echo "Merging {input}"
        python src/merge_pollutants.py yyyy={wildcards.yyyy}
        """

rule daily_to_yearly:
    input:
        f"{cfg.datapaths.base_path}/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{{yyyy}}.parquet"
    output:
        f"{cfg.datapaths.base_path}/output/yearly/air_pollution__aqdh__{cfg.shp_id}_yearly__{{yyyy}}.parquet"
    shell:
        """
        echo "Averaging from daily to yearly {input}"
        python src/daily2yearly.py yyyy={wildcards.yyyy}
        """