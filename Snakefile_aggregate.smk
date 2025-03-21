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
pollutant_list = config.get("pollutant_list")
yyyy_list = config.get("yyyy_list")
mm_list = config.get("mm_list")

# Invert the dictionary
#inv_pollutant_code_map = {v: k for k, v in cfg.pollutant_code_map.items()}

# yyyymmdd_list = [
#     f"{y}{m}{d:02d}"
#     for y in config.get("yyyy_list")
#     for m in config.get("mm_list")
#     for d in range(1, calendar.monthrange(int(y), int(m))[1] + 1)
# ]

# Rule to generate all required files
rule all:
    input:
        expand(
            f"{cfg.datapaths.base_path}/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet",
            pollutant=pollutant_list,
            yyyy=yyyy_list,
            mm=mm_list
        )
    # input:
    #     expand(
    #         "data/intermediate/" + cfg.zip_filename + "/",
    #         pollutant_code=[cfg.pollutant_code[pollutant] for pollutant in pollutant_list],
    #         yyyy=yyyy_list,
    #         mm=mm_list
    #     )

# def zip_filename(wildcards):
#     replacements = {
#         "pollutant_code": cfg.pollutant_code[wildcards.pollutant],
#         "yyyy": wildcards.yyyy,
#         "mm": wildcards.mm # input has to be 2 digits month:02d
#     }
#     zip_filename = cfg.zip_filename.format(**replacements)
#     return "data/input/raw/" + zip_filename + ".zip"
# 
# Rule to download air pollution data
# rule download_air_pollution:
#     output:
#         lambda wildcards: zip_filename(wildcards)
#     wildcard_constraints:
#         yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
#     shell:
#         """
#         echo "Downloading geotiff for pollutant {wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}"
#         python src/download_air_pollution.py pollutant={wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
#         """

# rule download_air_pollution:
#     output:
#         "data/input/raw/" + cfg.zip_filename + ".zip"
#     wildcard_constraints:
#         yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
#     params:
#         pollutant= lambda wildcards: inv_pollutant_code[wildcards.pollutant_code]  # Resolves pollutant_code dynamically
#     shell:
#         """
#         echo "Downloading geotiff for pollutant {params.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}"
#         python src/download_air_pollution.py pollutant={params.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
#         """

# rule unzip_air_pollution:
#     input:
#         "data/input/raw/" + cfg.zip_filename + ".zip"
#     output:
#         directory("data/intermediate/" + cfg.zip_filename + "/")
#     wildcard_constraints:
#         yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
#     params:
#         pollutant= lambda wildcards: inv_pollutant_code[wildcards.pollutant_code]  # Resolves pollutant_code dynamically
#     shell:
#         """
#          echo "Unzipping {input} -> {output}"
#          python src/extract_zipped.py pollutant={params.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
#         """

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