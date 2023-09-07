# Examples

### Parse and save single log file as parquets by day

```
python -m uniprot_log_file_parser.parse_to_parquet \
--out_dir '/path/to/save/parquet/files' \
--log_path '/path/to/file.log'
```

### Parse and save single log legacy file as parquets by day

```
python -m uniprot_log_file_parser.parse_to_parquet \
--out_dir '/path/to/save/parquet/files' \
--log_path '/path/to/legacy/file.log' \
--legacy
```

### Merge parsed parquets by month

```
python -m uniprot_log_file_parser.merge \
--working_dir '/path/to/parquet/files'
```

# TODO

- [ ] Create single configuration file in YAML
- [ ] Create single point of execution for parse submission
- [ ] Create merge slurm job
