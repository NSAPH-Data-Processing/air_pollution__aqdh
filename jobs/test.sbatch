#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 2 # number of cores per job in the array
#SBATCH --mem 4GB # memory per job in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)

snakemake --cores 8 --config pollutant_list="['o3']" --dry-run