#!/usr/bin/env python3
import csv
import re
from collections import defaultdict
import sys
import os
import json

input_path = sys.argv[1]

FIELDNAMES = [
    'date',
    'api',
    'resource',
    'referer',
]

p = re.compile(r'^\/uniprot\/(?P<accession>([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])(-[0-9]+)?)', re.IGNORECASE | re.DOTALL)

results = {'api': defaultdict(int), 'browser': defaultdict(int)}

with open(input_path, encoding='utf-8') as f:
    reader = csv.DictReader(f, fieldnames=FIELDNAMES)
    for row in (reader):
        m = p.match(row['resource'])
        if m:
            accession = m.group('accession')
            user_type = 'api' if row['api'] == "1" else 'browser'
            results[user_type][accession] += 1

input_file_path, input_file_name = os.path.split(input_path)
file_name_root = os.path.splitext(input_file_name)[0]
out_dir = os.path.abspath(os.path.join(
    input_file_path, '..', 'uniprotkb-entries'))
os.makedirs(out_dir, exist_ok=True)
output_file = os.path.join(out_dir, f'{file_name_root}.json')

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)
