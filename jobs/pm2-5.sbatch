#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 16 # number of cores per job in the array
#SBATCH --mem 64GB # memory per job in the array
#SBATCH -t 1-00:00 # time (D-HH:MM)

snakemake --cores 1 --config pollutant_list="['pm2-5']"