from collections import Counter
from glob import glob
import os.path
import json
import csv

def sort_dict_by_value(d, reverse=True):
    return {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=reverse)}


def write_counts_to_csv(counts, out):
    with open(out, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['accession', 'count'])
        for accession, count in counts.items():
            writer.writerow([accession, count])
    

def main():
    version_dir = '/nfs/public/rw/homes/uni_adm/tmp/log_parsing/parsed-no-bots/uniprotkb-entries/'
    api = Counter()
    browser = Counter()
    for file in glob(os.path.join(version_dir, '*.json')):
        print(file)
        with open(file) as f:
            r = json.load(f)
            api += Counter(r['api'])
            browser += Counter(r['browser'])
    
    api = sort_dict_by_value(api)
    browser = sort_dict_by_value(browser)

    write_counts_to_csv(api, os.path.join(version_dir, 'api.csv'))
    write_counts_to_csv(browser, os.path.join(version_dir, 'browser.csv'))