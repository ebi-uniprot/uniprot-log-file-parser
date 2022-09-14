# Overview

- This repository is designed to work with legacy UniProt log files.
- `bin/config.sh` contains the configuration for running a `submit_map.sh` job.
- `bin/submit_map.sh` submit an LSF job array to parse all of the files generated in `$LOG_FILE_LIST`.
- `bin/submit_reduce.sh` after all of the map job is complete run this and pass the `MAP_DIRECTORY` as the argument.
