#!/bin/bash
#
#SBATCH -p sapphire # partition (queue)
#SBATCH -c 16 # number of cores per job in the array
#SBATCH --mem 64GB # memory per job in the array
#SBATCH -t 0-12:00 # time (D-HH:MM)

snakemake --cores 8 --config pollutant_list="['pm25']" yyyy_list="['2001']"