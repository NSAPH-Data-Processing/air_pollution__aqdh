#!/bin/bash
#
#SBATCH -p sapphire # partition (queue)
#SBATCH -c 16 # number of cores per job in the array
#SBATCH --mem 64GB # memory per job in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)

snakemake --snakefile Snakefile_aggregate.smk --cores 8 --config pollutant_list="['pm25']" yyyy_list="['2015']"