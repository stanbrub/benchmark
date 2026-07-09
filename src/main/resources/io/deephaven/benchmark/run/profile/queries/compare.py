# Copyright (c) 2022-2026 Deephaven Data Labs and Patent Pending 
#
# Supporting Deephaven queries to use the benchmark_snippet to investigate product comparisons.
# - Generate a table that shows the comparisons between products for each benchmark
# Requirements: Deephaven 0.23.0 or greater

from urllib.request import urlopen; import os

benchmark_set_arg = 'deephaven/42.000.00'

root = 'file:///nfs' if os.path.exists('/nfs/deephaven-benchmark') else 'https://storage.googleapis.com'
with urlopen(root + '/deephaven-benchmark/benchmark_tables.dh.py') as r:
    benchmark_storage_uri_arg = root + '/deephaven-benchmark'
    benchmark_category_arg ='compare'
    benchmark_actor_filter_arg = os.path.dirname(benchmark_set_arg)
    benchmark_set_filter_arg = os.path.basename(benchmark_set_arg)
    exec(r.read().decode(), globals(), locals())

product_compare = bench_results_sets.view([
    'Product=benchmark_name.replaceAll(`[ ].*$`,``)', 'Benchmark=benchmark_name.replaceAll(`^[^ ]+[ ]`,``)',
    'Rate=op_rate'
]).sort(['Benchmark','Product'])

from deephaven import numpy as dhnp
products = dhnp.to_numpy(product_compare.select_distinct(['Product']))
products = [str(prod[0]) for prod in products]

product_compare = product_compare.group_by(['Benchmark']).view(['Benchmark'] + [
    products[0] + '=Rate[0]'
] + [
    products[i] + '=(double)(Rate[' + str(i) + '] - Rate[0]) / Rate[0]' for i in range(1, len(products))
]).format_columns([products[i] + '=Decimal(`0.0%`)' for i in range(1, len(products))])

bench_results = bench_metrics = bench_platforms = bench_results_sets = bench_results_change = None