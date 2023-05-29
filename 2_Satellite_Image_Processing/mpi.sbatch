#!/bin/bash

#SBATCH --job-name=mpi_area_extraction     # job name
#SBATCH --output=final.out    # output log file
#SBATCH --ntasks=10         # 1 CPU core to drive GPU
#SBATCH --partition=broadwl
#SBATCH --constraint=fdr
#SBATCH --account=macs30123 # specify the account

# Load Python and MPI modules
module load python/anaconda-2019.03
module load intelmpi/2018.2.199+intel-18.0

# Run the program
mpirun -n 10 python3 mpi_area_extraction.py