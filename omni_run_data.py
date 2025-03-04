import subprocess
from pathlib import Path

datasets = ["MovieLens100k"]
outputFolder = "out"

if __name__ == "__main__":
    for dataset in datasets:
        scriptPath = f"./PreProcess_{dataset}.sh"
        script = "#!/bin/bash\n" \
                 "#SBATCH --nodes=1\n" \
                 "#SBATCH --cpus-per-task=1\n" \
                 "#SBATCH --partition=short,medium\n" \
                 "#SBATCH --time=00:30:00\n" \
                 "#SBATCH --mem=32G\n" \
                 f"#SBATCH --output=./{outputFolder}/%x_%j.out\n" \
                 "module load singularity\n" \
                 "singularity exec --pwd /mnt --bind ./:/mnt ./data_loader.sif python -u " \
                 f"./preprocess_data.py --dataset {dataset}"
        with open(scriptPath, "w", newline="\n") as file:
            file.write(script)
        subprocess.run(["sbatch", scriptPath])
        Path(scriptPath).unlink()