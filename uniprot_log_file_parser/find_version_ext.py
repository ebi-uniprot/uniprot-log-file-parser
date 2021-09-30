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

p = re.compile(r'\/uniprot\/.*\.(?P<ext>txt|fasta|rss|list|tab)\?version=')

results = {'api': defaultdict(int), 'browser': defaultdict(int)}

with open(input_path, encoding='utf-8') as f:
    reader = csv.DictReader(f, fieldnames=FIELDNAMES)
    for row in (reader):
        m = p.match(row['resource'])
        if m:
            ext = m.group('ext')
            if row['api'] == "1":
                results['api'][ext] += 1
            else:
                results['browser'][ext] += 1

input_file_path, input_file_name = os.path.split(input_path)
file_name_root = os.path.splitext(input_file_name)[0]
version_dir = os.path.abspath(os.path.join(
    input_file_path, '..', 'version-ext'))
os.makedirs(version_dir, exist_ok=True)
output_file = os.path.join(version_dir, f'{file_name_root}.json')

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=4)
