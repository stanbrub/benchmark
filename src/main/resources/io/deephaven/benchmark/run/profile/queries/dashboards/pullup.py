# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending 
#
# Takes the adhoc-style directory structure and generates csv with all sets and runs. The
# resulting files are 'benchmark-events.csv', 'benchmark-metrics.csv', 'benchmark-platform.csv', 
# 'benchmark-results.csv', and 'benchmark-result_sets.csv'. These files look, for example, like
# the tables loaded from "load_table_or_empty('metrics', storage_uri, 'adhoc', actor, prefix)"
# function seen in other benchmark queries. The csv files are written to the "/data" directory.
#
# Requirements: Deephaven 41.7 or greater

from urllib.request import urlopen; import os, gzip, shutil, re
from deephaven import csv as dhcsv
from deephaven.parquet import write as pq_write

def glob_to_regex(pat):
    return '^' + re.escape(pat).replace(r'\*', '.*').replace(r'\?', '.') + '$'

actor = "Final-gc-report"
dest_actor = "gc-fun"
prefix = "gc_"
exclude_sets = ['*_p110_*']
output_dir = f'/data/deephaven-benchmark/demo/{dest_actor}'

root = 'file:///data' if os.path.exists('/data/deephaven-benchmark') else 'https://storage.googleapis.com'
with urlopen(f'{root}/deephaven-benchmark/benchmark_functions.dh.py') as r:
    exec(r.read().decode(), globals(), locals())
    storage_uri = f'{root}/deephaven-benchmark'

os.makedirs(output_dir, exist_ok=True)

for name in ['metrics', 'events', 'platform', 'results', 'result_sets']:
    data = load_table_or_empty(name, storage_uri, 'adhoc', actor, prefix)
    table = data[0] if name == 'result_sets' else data
    if exclude_sets:
        table = table.where(' && '.join(f'!set_id.matches(`{glob_to_regex(pat)}`)' for pat in exclude_sets))
    if dest_actor != actor:
        table = table.update_view([f'set_id = set_id.replaceFirst(`{actor}`, `{dest_actor}`)'])
    output_csv = f'{output_dir}/benchmark-{name}.csv'
    dhcsv.write(table, output_csv)
    with open(output_csv, 'rb') as f_in, gzip.open(output_csv + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(output_csv)
    print(f'Wrote csv: {output_csv}.gz')
    output_pq = f'{output_dir}/benchmark-{name}.parquet'
    pq_write(table, output_pq, compression_codec_name='ZSTD')
    print(f'Wrote parquet: {output_pq}')

print("Done!")
