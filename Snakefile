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

# Invert the dictionary
inv_pollutant_code = {v: k for k, v in cfg.pollutant_code.items()}

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
            "data/input/raw/" + cfg.zip_filename + ".zip",
            pollutant_code=[cfg.pollutant_code[pollutant] for pollutant in pollutant_list],
            yyyy=yyyy_list,
            mm=mm_list
        )

# def zip_filename(wildcards):
#     replacements = {
#         "pollutant_code": cfg.pollutant_code[wildcards.pollutant],
#         "yyyy": wildcards.yyyy,
#         "mm": wildcards.mm # input has to be 2 digits month:02d
#     }
#     zip_filename = cfg.zip_filename.format(**replacements)
#     return "data/input/raw/" + zip_filename + ".zip"

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

# Rule to download air pollution data
rule download_air_pollution:
    output:
        "data/input/raw/" + cfg.zip_filename + ".zip"
    wildcard_constraints:
        yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
    params:
        pollutant= lambda wildcards: inv_pollutant_code[wildcards.pollutant_code]  # Resolves pollutant_code dynamically
    shell:
        """
        echo "Downloading geotiff for pollutant {params.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}"
        python src/download_air_pollution.py pollutant={params.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
        """

# rule unzip_air_pollution:
#     input:
#         lambda wildcards: zip_filename(wildcards)  # Ensures correct filename resolution
#     output:
#         directory("data/intermediate/" + cfg.zip_filename.format(
#             pollutant_code="{pollutant_code}",  # Leave as wildcard
#             yyyy="{yyyy}",
#             mm="{mm}"
#         ) + "/")
#     wildcard_constraints:
#         yyyy = r"\d{4}"  # Ensures yyyy is exactly 4 digits
#     params:
#         pollutant_code = lambda wildcards: cfg.pollutant_code[wildcards.pollutant]  # Resolves pollutant_code dynamically
#     shell:
#         """
#         echo "Unzipping {input} -> {output}"
#         python src/extract_zipped.py pollutant={wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
#         """

# rule aggregate_air_pollution:
#     input:
#         unzip_filename
#     output:
#         f"data/intermediate/{{pollutant}}_aqdh__{cfg.shp_id}_daily__{{yyyy}}{{mm}}.parquet"
#     wildcard_constraints:
#         yyyy = r"\d{4}",  # Ensures yyyy is exactly 4 digits
#         mm = r"\d{2}"  # Ensures mm is exactly 2 digits
#     shell:
#         """
#         echo "Aggregating {input}"
#         python src/aggregate_air_pollution.py pollutant={wildcards.pollutant} yyyy={wildcards.yyyy} mm={wildcards.mm}
#         """