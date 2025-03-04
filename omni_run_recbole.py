import json
import subprocess
from pathlib import Path

outputFolder = "out"

if __name__ == "__main__":
    settings = json.load(open(f"./settings.json"))
    seed = settings["seed"]
    for dataset in settings["datasets"]:
        for algorithm in settings["algorithms"]:
            scriptPath = f"./RecBole_{algorithm}_{dataset}.sh"
            script = "#!/bin/bash\n" \
                    "#SBATCH --nodes=1\n" \
                    "#SBATCH --cpus-per-task=1\n" \
                    "#SBATCH --partition=gpu\n" \
                    "#SBATCH --gres=gpu:1\n" \
                    "#SBATCH --time=12:00:00\n" \
                    "#SBATCH --mem=48G\n" \
                    f"#SBATCH --output=./{outputFolder}/%x_%j.out\n" \
                    "module load singularity\n" \
                    "singularity exec --pwd /mnt --bind ./:/mnt ./container.sif python -u " \
                    f"./recbole_predict.py --dataset {dataset} --algorithm {algorithm} --seed {seed}"
            with open(scriptPath, "w", newline="\n") as file:
                file.write(script)
            subprocess.run(["sbatch", scriptPath])
            Path(scriptPath).unlink()