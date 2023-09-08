#!/usr/bin/env python3
import os
import pathlib
import subprocess
import argparse
from glob import glob
import yaml


def get_arguments() -> [str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config",
        type=str,
        help="YAML configuration file with 'logs', 'results_directory' and optionally a 'legacy' flag",
    )
    args = parser.parse_args()
    return args.config


def main():
    config_path = get_arguments()
    with open(config_path, encoding="utf-8") as file:
        config = yaml.safe_load(file)

    assert "logs" in config
    logs = config["logs"]
    assert isinstance(logs, str) or isinstance(logs, list)

    assert "results_directory" in config
    results_directory = config["results_directory"]
    assert isinstance(results_directory, str)

    legacy = bool(config["legacy"] if "legacy" in config else False)

    out_directory = os.path.join(results_directory, "out")
    error_directory = os.path.join(results_directory, "error")
    log_file_list = os.path.join(results_directory, "log_file_list.txt")
    module_directory = "/homes/dlrice/uniprot-log-file-parser"

    pathlib.Path(out_directory).mkdir(parents=True, exist_ok=True)
    pathlib.Path(error_directory).mkdir(parents=True, exist_ok=True)

    log_globs = [logs] if isinstance(logs, str) else logs
    log_list = []
    for log_glob in log_globs:
        log_list += glob(log_glob)

    with open(log_file_list, "w", encoding="utf-8") as file:
        print("\n".join(log_list), file=file)

    n_logs = len(log_list)

    sbatch = f"""
    sbatch \
    --time 2:00:00 \
    --cpus-per-task 1 \
    --partition datamover \
    --array=1-{n_logs} \
    --chdir={module_directory} \
    --output={out_directory}/parse_%A_%a.o \
    --error={out_directory}/parse_%A_%a.e \
    --ntasks=1 \
    --mem=8G \
    parse_array_task.batch \
    {log_file_list} \
    {results_directory} \
    {legacy if legacy else ''}
    """

    result = subprocess.run(
        [sbatch], shell=True, capture_output=True, text=True, check=True
    )
    print(result.stdout)
    print(result.stderr)


if __name__ == "__main__":
    main()
