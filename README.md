# TODO

- [x] Use 512 hash
- [x] Store index for UA rather than string
- [x] Finish log insertion
  - [x] Save all csvs
  - [x] Merge to get user agent family id
- [x] Create method enum https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
- [x] Check for unmatched UA strings
- [ ] Rewrite merges to use duckdb
- [x] Store:
  - [x] Daily total number of requests
  - [x] Daily total number of bytes
  - [x] 200s, 400s, 500s etc
- [x] Create as a CLI
- [x] Create a backup script to save as gzipped csv
- [x] Don't store today's log
- [ ] Where to store the duckdb?
