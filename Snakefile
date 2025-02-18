conda: "environment.yaml"
configfile: "snake_config.yaml"

# Define parameters
pollutant_list = config.get("pollutant_list")
years = [str(y) for y in config.get("years")]
months = [f"{m:02d}" for m in config.get("months")]
yyyymm_list = [y + m for y in years for m in months]

# Rule to generate all required files
rule all:
    input:
        expand(
            "data/input/raw/aqdh-{pollutant}-concentrations-contiguous-us-1-km-v1-10-2000-2016-{yyyymm}-geotiff.zip", 
            pollutant=pollutant_list, 
            yyyymm=yyyymm_list
        )

# Rule to download air pollution data
rule download_air_pollution:
    output:
        "data/input/raw/aqdh-{pollutant}-concentrations-contiguous-us-1-km-v1-10-2000-2016-{yyyymm}-geotiff.zip"
    shell:
        """
        echo "Downloading geotiff for pollutant {wildcards.pollutant} and yyyymm {wildcards.yyyymm}"
        python src/download_air_pollution.py pollutant={wildcards.pollutant} yyyymm={wildcards.yyyymm}
        """

