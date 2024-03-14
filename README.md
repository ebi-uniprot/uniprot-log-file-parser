# Examples

## Submit parse job from SLURM

```
srun -t 1:00:00 -p datamover --mem=1G --pty bash -l
./submit_parse.py ../yaml/uniprotkb.yaml
```

## Merge parquets

```
python -m uniprot_log_file_parser.merge \
'/hps/nobackup/martin/uniprot/users/dlrice/rest-log-parquets/uniprotkb/parquet'
```

## Parse and save single log file as parquets by day

```
python -m uniprot_log_file_parser.parse_to_parquet \
--out_dir '/path/to/save/parquet/files' \
--log_path '/path/to/file.log'
```

## Parse and save single log legacy file as parquets by day

```
python -m uniprot_log_file_parser.parse_to_parquet \
--out_dir '/path/to/save/parquet/files' \
--log_path '/path/to/legacy/file.log' \
--legacy
```

# TODO

- [x] Create single configuration file in YAML
- [x] Create single point of execution for parse submission
- [ ] Create merge slurm job
