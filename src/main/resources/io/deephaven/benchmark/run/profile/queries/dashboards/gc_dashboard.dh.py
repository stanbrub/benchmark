# Copyright (c) 2022-2026 Deephaven Data Labs and Patent Pending 
#
# Deephaven GC Comparison dashboard for visualizing garbage collection behavior across
# benchmark data sets (e.g. G1 vs ZGC). The dashboard shows benchmark operation rates,
# GC event durations, GC pause times, and GC CPU time side-by-side for each benchmark.
# Data is loaded using the events functions in benchmark_functions.dh.py.
#
# Requirements: Deephaven 0.36.1 or greater
#
# ruff: noqa: F821
from urllib.request import urlopen; import os
from deephaven import ui, merge
from deephaven.ui import use_memo, use_state
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle

# Resolve the benchmark data root — prefer local NFS, then user-local data, then GCloud
local_data = '/home/stan/Data/Deephaven/deephaven-edge/data'
if os.path.exists('/nfs/deephaven-benchmark'):
    root = 'file:///nfs'
elif os.path.exists(f'{local_data}/deephaven-benchmark'):
    root = f'file://{local_data}'
else:
    root = 'https://storage.googleapis.com'

with urlopen(f'{root}/deephaven-benchmark/benchmark_functions.dh.py') as r:
    exec(r.read().decode(), globals(), locals())
    storage_uri = f'{root}/deephaven-benchmark'

# ---------------------------------------------------------------------------
# Helper: get distinct set_ids ordered by timestamp
# ---------------------------------------------------------------------------
def get_setids(table):
    col_names = [c.name for c in table.columns]
    sort_col = 'timestamp' if 'timestamp' in col_names else 'start' if 'start' in col_names else 'set_id'
    setids = table.sort([sort_col]).select_distinct(['set_id'])
    return [row.set_id for row in setids.iter_tuple()]

# ---------------------------------------------------------------------------
# Helper: readable label for a set_id (strip actor prefix, keep set name)
# ---------------------------------------------------------------------------
def set_label(prefix, setid):
    text = re.sub('^.*/','', setid[len(prefix):]) if setid.startswith(prefix) else setid
    text = normalize_name(text)
    return re.sub('^_+', '', text)

# ---------------------------------------------------------------------------
# Dashboard input: actor + set-label filter
# ---------------------------------------------------------------------------
def use_dashboard_input():
    actor, set_actor = use_state('')
    prefix, set_prefix = use_state('')
    user_input, set_user_input = use_state({'actor': '', 'prefix': ''})

    def update_user_input():
        set_user_input({'actor': actor, 'prefix': prefix})

    input_panel = ui.flex(
        ui.text_field(label='Actor', label_position='side', value=actor, on_change=set_actor),
        ui.text_field(label='Set Label Prefix', label_position='side', value=prefix, on_change=set_prefix),
        ui.button('Apply', on_press=lambda: update_user_input()),
        direction='row'
    )
    return user_input, input_panel

# ---------------------------------------------------------------------------
# Load benchmark result-sets + raw results (same as adhoc)
# ---------------------------------------------------------------------------
def load_results_tables(parent_table, actor, prefix):
    bench_result_sets, bench_results = load_table_or_empty('result_sets', storage_uri, 'adhoc', actor, prefix)

    setids = get_setids(bench_result_sets)
    setprefix = f'{actor}/{prefix}'

    bench = bench_result_sets.select_distinct(['Benchmark=benchmark_name'])
    rate1 = None
    for setid in setids:
        setcol = set_label(setprefix, setid)
        varcol = 'Var_' + setcol
        ratecol = 'Rate_' + setcol
        changecol = 'Change_' + setcol
        right = bench_result_sets.where([f'set_id=`{setid}`'])
        bench = bench.natural_join(right, on=['Benchmark=benchmark_name'],
            joins=[varcol + '=variability', ratecol + '=op_rate'])
        if rate1 is None:
            rate1 = ratecol
        else:
            bench = bench.update([changecol + '=(float)gain(' + rate1 + ',' + ratecol + ')'])
    bench = format_columns(bench, pct_cols=('Var_', 'Change_'), int_cols=('Rate',))
    return bench, bench_results

# ---------------------------------------------------------------------------
# Load events tables
# ---------------------------------------------------------------------------
def load_events_tables(parent_table, actor, prefix):
    bench_events = load_table_or_empty('events', storage_uri, 'adhoc', actor, prefix)
    return bench_events

# ---------------------------------------------------------------------------
# Memo wrappers
# ---------------------------------------------------------------------------
def load_table_memo(table_name, parent_table, user_input):
    table_func = globals()[f'load_{table_name}_tables']
    return use_memo(
        lambda: table_func(parent_table, user_input['actor'], user_input['prefix']), [user_input])

# ---------------------------------------------------------------------------
# Chart: Op-Rate comparison (line chart per set across runs)
# ---------------------------------------------------------------------------
def use_rate_chart(result_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    setids = get_setids(result_table)
    setprefix = f'{actor}/{prefix}'
    selected_benchmark = row_selection['Benchmark']['value']

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.chart_title(title=f'{selected_benchmark} — Op Rate by Run')
        lbl = set_label(setprefix, setid)
        chart_table = result_table.where([f'benchmark_name=`{selected_benchmark}`', f'set_id=`{setid}`']) \
            .sort(['timestamp']).update('run=i+1')
        ui_figure = ui_figure.plot_xy(series_name=lbl, t=chart_table, x='run', y='op_rate')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Chart: GC Collection Duration histogram (bar chart — total duration per GC name per set)
# ---------------------------------------------------------------------------
def use_gc_duration_chart(events_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    selected_benchmark = row_selection['Benchmark']['value']
    setids = get_setids(events_table)
    setprefix = f'{actor}/{prefix}'

    gc_events = events_table.where([
        f'benchmark_name=`{selected_benchmark}`',
        'type=`jdk.GarbageCollection`'
    ])

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.axes(plot_style=PlotStyle.BAR) \
                .chart_title(title=f'{selected_benchmark} — GC Collection Duration (ns)')
        lbl = set_label(setprefix, setid)
        set_gc = gc_events.where([f'set_id=`{setid}`']) \
            .view(['name', 'duration=(double)duration']) \
            .agg_by([agg.sum_('total_duration=duration')], by=['name'])
        ui_figure = ui_figure.plot_cat(series_name=lbl, t=set_gc, category='name', y='total_duration')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Chart: GC Pause Total Duration (bar chart — total pause time per set)
# ---------------------------------------------------------------------------
def use_gc_pause_chart(events_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    selected_benchmark = row_selection['Benchmark']['value']
    setids = get_setids(events_table)
    setprefix = f'{actor}/{prefix}'

    pause_events = events_table.where([
        f'benchmark_name=`{selected_benchmark}`',
        'type=`jdk.GCPhasePause`'
    ])

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.axes(plot_style=PlotStyle.BAR) \
                .chart_title(title=f'{selected_benchmark} — GC Pause Duration (ns)')
        lbl = set_label(setprefix, setid)
        set_pause = pause_events.where([f'set_id=`{setid}`']) \
            .view(['name', 'duration=(double)duration']) \
            .agg_by([agg.sum_('total_duration=duration'), agg.count_('event_count')], by=['name'])
        ui_figure = ui_figure.plot_cat(series_name=lbl, t=set_pause, category='name', y='total_duration')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Chart: GC Concurrent Phase Duration (bar chart — total concurrent time per set)
# ---------------------------------------------------------------------------
def use_gc_concurrent_chart(events_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    selected_benchmark = row_selection['Benchmark']['value']
    setids = get_setids(events_table)
    setprefix = f'{actor}/{prefix}'

    concurrent_events = events_table.where([
        f'benchmark_name=`{selected_benchmark}`',
        'type=`jdk.GCPhaseConcurrent`'
    ])

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.axes(plot_style=PlotStyle.BAR) \
                .chart_title(title=f'{selected_benchmark} — GC Concurrent Phase Duration (ns)')
        lbl = set_label(setprefix, setid)
        set_conc = concurrent_events.where([f'set_id=`{setid}`']) \
            .view(['name', 'duration=(double)duration']) \
            .agg_by([agg.sum_('total_duration=duration')], by=['name'])
        ui_figure = ui_figure.plot_cat(series_name=lbl, t=set_conc, category='name', y='total_duration')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Chart: GC Event Count comparison (bar chart — number of GC events per name per set)
# ---------------------------------------------------------------------------
def use_gc_count_chart(events_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    selected_benchmark = row_selection['Benchmark']['value']
    setids = get_setids(events_table)
    setprefix = f'{actor}/{prefix}'

    gc_events = events_table.where([
        f'benchmark_name=`{selected_benchmark}`',
        'type=`jdk.GarbageCollection`'
    ])

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.axes(plot_style=PlotStyle.BAR) \
                .chart_title(title=f'{selected_benchmark} — GC Event Count')
        lbl = set_label(setprefix, setid)
        set_gc = gc_events.where([f'set_id=`{setid}`']) \
            .agg_by([agg.count_('event_count')], by=['name'])
        ui_figure = ui_figure.plot_cat(series_name=lbl, t=set_gc, category='name', y='event_count')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Chart: UGP Elapsed Time line chart (scatter over time per set)
# ---------------------------------------------------------------------------
def use_ugp_chart(events_table, row_selection, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    selected_benchmark = row_selection['Benchmark']['value']
    setids = get_setids(events_table)
    setprefix = f'{actor}/{prefix}'

    ugp_events = events_table.where([
        f'benchmark_name=`{selected_benchmark}`',
        'type=`ugp.delta`',
        'name=`elapsedTime`'
    ])

    ui_figure = Figure()
    for i, setid in enumerate(setids):
        if i == 0:
            ui_figure = ui_figure.chart_title(title=f'{selected_benchmark} — UGP Elapsed Time (ns)')
        lbl = set_label(setprefix, setid)
        set_ugp = ugp_events.where([f'set_id=`{setid}`']) \
            .sort(['start']).update(['seq=i+1', 'elapsed=(double)value'])
        ui_figure = ui_figure.plot_xy(series_name=lbl, t=set_ugp, x='seq', y='elapsed')
    return ui.flex(ui_figure.show())

# ---------------------------------------------------------------------------
# Summary table: aggregate GC stats per set per benchmark
# ---------------------------------------------------------------------------
def build_gc_summary(events_table, user_input):
    actor = user_input['actor']; prefix = user_input['prefix']
    if not actor or not prefix:
        return empty_table(0).update([
            'benchmark_name=(String)null', 'set_id=(String)null',
            'gc_count=(long)0', 'total_pause_ns=(double)0',
            'total_concurrent_ns=(double)0', 'total_gc_duration_ns=(double)0'
        ])

    gc_coll = events_table.where(['type=`jdk.GarbageCollection`']) \
        .view(['benchmark_name', 'set_id', 'duration=(double)duration']) \
        .agg_by([agg.sum_('total_gc_duration_ns=duration'), agg.count_('gc_count')],
                by=['benchmark_name', 'set_id'])

    pause = events_table.where(['type=`jdk.GCPhasePause`']) \
        .view(['benchmark_name', 'set_id', 'duration=(double)duration']) \
        .agg_by([agg.sum_('total_pause_ns=duration')], by=['benchmark_name', 'set_id'])

    concurrent = events_table.where(['type=`jdk.GCPhaseConcurrent`']) \
        .view(['benchmark_name', 'set_id', 'duration=(double)duration']) \
        .agg_by([agg.sum_('total_concurrent_ns=duration')], by=['benchmark_name', 'set_id'])

    summary = gc_coll.natural_join(pause, on=['benchmark_name', 'set_id'], joins=['total_pause_ns']) \
        .natural_join(concurrent, on=['benchmark_name', 'set_id'], joins=['total_concurrent_ns'])
    return summary

# ---------------------------------------------------------------------------
# Main dashboard component
# ---------------------------------------------------------------------------
@ui.component
def gc_dashboard():
    user_input, input_form = use_dashboard_input()
    row_selection, set_row_selection = use_state({'Benchmark': {'value': ''}})

    set_table, result_table = load_table_memo('results', None, user_input)
    events_table = load_table_memo('events', None, user_input)

    gc_summary = use_memo(
        lambda: build_gc_summary(events_table, user_input), [user_input, events_table])

    rate_chart = use_rate_chart(result_table, row_selection, user_input)
    gc_duration_chart = use_gc_duration_chart(events_table, row_selection, user_input)
    gc_pause_chart = use_gc_pause_chart(events_table, row_selection, user_input)
    gc_concurrent_chart = use_gc_concurrent_chart(events_table, row_selection, user_input)
    gc_count_chart = use_gc_count_chart(events_table, row_selection, user_input)
    ugp_chart = use_ugp_chart(events_table, row_selection, user_input)

    return ui.column([
        ui.row(ui.panel(input_form, title='Data Set'), height='7'),
        ui.row(
            ui.panel(
                ui.table(set_table, on_row_press=set_row_selection, density='regular'),
                title='Benchmark Comparison'),
            ui.panel(
                ui.table(gc_summary, density='regular'),
                title='GC Summary'),
            height='35'),
        ui.row(
            ui.stack(
                ui.panel(rate_chart, title='Op Rate'),
                ui.panel(ugp_chart, title='UGP Elapsed Time'),
                active_item_index=0
            ),
            ui.stack(
                ui.panel(gc_duration_chart, title='GC Duration'),
                ui.panel(gc_count_chart, title='GC Event Count'),
                active_item_index=0
            ),
            height='29'),
        ui.row(
            ui.stack(
                ui.panel(gc_pause_chart, title='GC Pause Duration'),
            ),
            ui.stack(
                ui.panel(gc_concurrent_chart, title='GC Concurrent Duration'),
            ),
            height='29'),
    ])

GC_Dashboard = ui.dashboard(gc_dashboard())
