#!/bin/bash
#SBATCH --job-name=scrap
#SBATCH -A plafnet2
#SBATCH -p plafnet2
#SBATCH -c 10
#SBATCH --gres=gpu:1
#SBATCH --time=4-00:00:00
#SBATCH --output scrap.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=harsha.vasamsetti@research.iiit.ac.in
#SBATCH -w gnode114

echo "mkdir"
mkdir /scratch/harsha.vasamsetti

echo "chdir"
cd /scratch/harsha.vasamsetti/latest/code-2

echo $(ls)
source /home2/harsha.vasamsetti/miniconda3/bin/activate slices

echo "training"
python /scratch/harsha.vasamsetti/latest/code-2/main2.py

echo "done"