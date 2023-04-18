# TODO

- [x] Use 512 hash
- [x] Store index for UA rather than string
- [ ] Finish log insertion
  - [ ] Save all csvs
  - [ ] Merge to get user agent family id
- [ ] Rewrite merges to use duckdb
- [ ] Create method enum https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
- [ ] Check for unmatched UA strings
- [ ] Store:
  - [ ] Daily total number of requests
  - [ ] Daily total number of bytes
  - [ ] 200s, 400s, 500s etc
- [ ] Create as a CLI
- [ ] Create a backup script to save as gzipped csv
- [ ] Exclude kube liveness probes & health checks
