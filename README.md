# APS

This repository contains the scripts that were used for the experiments for my thesis "Algorithm Performance Spaces for Strategic Dataset Selection".

To reproduce the experiments, the following steps must be taken:

1. The datasets are listed in metadata.csv and need to be downloaded without extraction in the ./data folder.
2. The datasets are pre processed by either locally running preprocess_data.py, or using the OMNI-Cluster with omni_run_data.py
3. Training and Evaluating the algorithms is done by either locally running recbole_predict.py, or using the OMNI-Cluster with omni_run_recbole.py
4. The output from the experiments are stored in ./out or ./omni-out and can be summarized in a .csv by running evaluate.py
5. To plot all mini-APS, the PCA graph or calculate the Diversity-APS based on the results, plot_aps.py, plot_pca.py and calc_diversity.py can respectively be used.

The singularity container for the OMNI-Cluster is defined in container.def. Recbole settings are found in recbole_settings.yaml, the general experimental settings are listed in settings.json. The hyperparamers used for the hyperparameter-optimization are defined inside hyperparams.py.