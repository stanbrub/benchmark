# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending
#
# Compare the median op_rate of the last N days of nightly benchmarks against the median
# op_rate of a single release version. For each benchmark, the median run is selected by
# op_rate, and the date/run_id of that median run is preserved. Gain is calculated as the
# percent change from the release version median to the nightly median, so negative gain
# means the nightly is a regression against the release.
#
# Two csv files are written, each sorted worst regression first: the per-benchmark gains, and
# the same gains grouped by benchmark name prefix (the test class that runs them). The group csv
# is the input to make-regression-matrix.sh, which resolves nightly images and builds a rerun
# matrix for the benchmark workflows.
#
# Requirements: Deephaven 41.7 or greater

from urllib.request import urlopen; import os, re
from deephaven import csv as dhcsv, numpy as dhnp

nightly_days = 7                     # Number of most recent nightly dates to include
release_version = '42.000.00'        # Release version to compare the nightly median against
actor = 'deephaven'                  # Benchmark actor (directory) for both categories
max_gain = -0.05                     # Only keep benchmarks with gain below this (regressions)
image_repo = 'ghcr.io/deephaven/server'          # GHCR repo the benchmarks are run against
output_csv = '/data/nightly-vs-release.csv'
output_group_csv = '/data/nightly-vs-release-groups.csv'

# Use the local mirror only when it is a real mirror, not just the remote-csv cache directory
root = 'file:///nfs' if os.path.exists('/nfs/deephaven-benchmark/benchmark_functions.dh.py') else 'https://storage.googleapis.com'
with urlopen(f'{root}/deephaven-benchmark/benchmark_functions.dh.py') as r:
    exec(r.read().decode(), globals(), locals())
    storage_uri = f'{root}/deephaven-benchmark'

# Get the run paths for the most recent max_sets sets matching the given filters
def find_runs(category, set_filter, max_sets):
    paths = get_run_paths(storage_uri, category, actor, set_filter, max_sets)
    if not paths:
        raise Exception(f'No runs found for {category}/{actor} matching {set_filter}')
    return paths

# Load raw benchmark results for the given run paths
def load_results(category, paths):
    return merge_run_tables(storage_uri, paths, category, 'benchmark-results.csv', convert_result)

# Get the engine version recorded by a run, which is the image tag rather than the set label
# (set '42.000.00' is published as ghcr.io/deephaven/server:42.0)
def engine_version(category, path):
    platform = merge_run_tables(storage_uri, [path], category, 'benchmark-platform.csv', convert_platform)
    version = platform.where(['origin=`deephaven-engine`', 'name=`deephaven.version`']).view(['value'])
    return str(dhnp.to_numpy(version)[0][0])

# Reduce results to one median row per benchmark (by op_rate), keeping that row's date and run_id
def median_by_benchmark(results, prefix):
    return results.where(['op_rate > 0']) \
        .sort(['benchmark_name', 'origin', 'op_rate']) \
        .group_by(['benchmark_name', 'origin']) \
        .view(['benchmark_name', 'origin',
            f'{prefix}_Rate=(long)mid_item(op_rate)',
            f'{prefix}_Millis=(long)mid_item(timestamp)',
            f'{prefix}_SetId=(String)mid_item(set_id)',
            f'{prefix}_Run=(String)mid_item(run_id)',
            f'{prefix}_Runs=count(op_rate)']) \
        .update_view([
            f'{prefix}_Date=formatDate(epochMillisToInstant({prefix}_Millis), timeZone(`ET`))',
            f'{prefix}_Set={prefix}_SetId.replaceFirst(`.*/`, ``)'])

nightly_runs = find_runs('nightly', get_default_set_filter('nightly'), nightly_days)
release_runs = find_runs('release', re.escape(release_version), 1)

# The release image needs no digest lookup, its version tag is fixed
release_image = f'{image_repo}:{engine_version("release", release_runs[0])}'
print(f'Release {release_version} ran {release_image}')

nightly_results = load_results('nightly', nightly_runs)
release_results = load_results('release', release_runs)

nightly_medians = median_by_benchmark(nightly_results, 'Nightly')
release_medians = median_by_benchmark(release_results, 'Release')

nightly_vs_release = nightly_medians.natural_join(
    release_medians, on=['benchmark_name', 'origin'],
    joins=['Release_Rate', 'Release_Date', 'Release_Set', 'Release_Run']
).where([
    '!isNull(Release_Rate)'
]).view([
    'Benchmark=benchmark_name', 'Gain=gain(Release_Rate, Nightly_Rate)',
    'Nightly_Rate', 'Nightly_Date', 'Nightly_Run',
    'Release_Rate', 'Release_Version=Release_Set', 'Release_Date', 'Release_Run'
]).where([
    'Gain < max_gain'
]).sort(['Gain'])

# Group the regressions by benchmark name prefix (the part before the first dash or space,
# e.g. 'EmsTime'), since benchmarks are run by prefix-named test class rather than individually.
# Each row carries the worst gain in the group along with that benchmark's date and run.
# Release_Image is constant, but carrying it here keeps the csv self-contained for the matrix.
group_gains = nightly_vs_release.update_view([
    'BenchmarkPrefix=Benchmark.replaceFirst(`[- ].*`, ``)'
]).sort(['Gain']).group_by(['BenchmarkPrefix']).view([
    'BenchmarkPrefix', 'Worst_Gain=Gain[0]', 'Worst_Benchmark=Benchmark[0]',
    'Regressions=(int)len(Gain)',
    'Nightly_Rate=Nightly_Rate[0]', 'Nightly_Date=Nightly_Date[0]', 'Nightly_Run=Nightly_Run[0]',
    'Release_Rate=Release_Rate[0]', 'Release_Date=Release_Date[0]', 'Release_Run=Release_Run[0]',
    'Release_Image=release_image'
]).sort(['Worst_Gain'])

for table, path in [(nightly_vs_release, output_csv), (group_gains, output_group_csv)]:
    dhcsv.write(table, path)
    print(f'Wrote csv: {path}')
