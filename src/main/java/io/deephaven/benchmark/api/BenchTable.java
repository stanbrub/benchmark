/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.api;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import io.deephaven.benchmark.generator.*;
import io.deephaven.benchmark.metric.Metrics;
import io.deephaven.benchmark.util.Ids;
import io.deephaven.benchmark.util.Log;
import io.deephaven.benchmark.util.Numbers;
import io.deephaven.benchmark.util.Timer;

/**
 * Represents the configuration of table name and columns.
 */
final public class BenchTable implements Closeable {
    final Bench bench;
    final String tableName;
    final ColumnDefs columns = new ColumnDefs();
    private long rowCount = 0;
    private int durationSecs = -1;
    private int rowPauseMillis = -1;
    private String compression = null;
    private Generator generator = null;
    private boolean isFixed = false;
    private String defaultDistro = null;
    private String[] columnGrouping = null;

    BenchTable(Bench bench, String tableName) {
        this.tableName = tableName;
        this.bench = bench;
    }

    /**
     * Add a column definition for the table schema
     * 
     * @param name the name of the column
     * @param type the type of the column ( <code>string | long | int | double | float</code> )
     * @param valuesDef range or combination of range and string
     * @return this instance
     */
    public BenchTable add(String name, String type, String valuesDef) {
        columns.add(name, type, valuesDef);
        return this;
    }

    /**
     * Add a column definition for the table schema
     * 
     * @param name the name of the column
     * @param type the type of the column ( <code>string | long | int | double | float</code> )
     * @param valuesDef range or combination of range and string
     * @param distribution the name of the distribution ( <code>random | ascending | descending | runlength </code> )
     * @return this instance
     */
    public BenchTable add(String name, String type, String valuesDef, String distribution) {
        columns.add(name, type, valuesDef, distribution);
        return this;
    }

    /**
     * Override the profile's row count (e.g. scale.row.count)
     * 
     * @param generatedRowCount how many rows the table should have
     * @return this instance
     */
    public BenchTable withRowCount(long generatedRowCount) {
        rowCount = generatedRowCount;
        return this;
    }

    /**
     * Set column names to be used for grouping where applicable (ex. parquet generation)
     * 
     * @param columnNames the grouping column names
     * @return this instance
     */
    public BenchTable withColumnGrouping(String... columnNames) {
        columnGrouping = columnNames;
        return this;
    }

    /**
     * Override the profile's run duration (e.g. default.completion.timeout=5 minutes)
     * 
     * @param duration the maximum run duration
     * @param unit the unit of time for the duration
     * @return this instance
     */
    public BenchTable withRunDuration(int duration, ChronoUnit unit) {
        durationSecs = (int) Duration.of(duration, unit).toSeconds();
        return this;
    }

    /**
     * Override the pause between producing records (e.g. generator.pause.per.row=0 millis). (Note: Usually, this should
     * be left alone, since adding even 1 milli can make record generation take inordinately long.)
     * 
     * @param duration the pause between sending records
     * @param unit the unit of time for the duration
     * @return this instance
     */
    public BenchTable withRowPause(int duration, ChronoUnit unit) {
        rowPauseMillis = (int) Duration.of(duration, unit).toMillis();
        return this;
    }

    /**
     * Override the default compression codec for record generation and parquet
     * 
     * @param codec the compression codec <code>(zstd | lz4 | lzo | gzip | snappy | none)</code>
     * @return this instance
     */
    public BenchTable withCompression(String codec) {
        compression = codec;
        return this;
    }

    /**
     * Configure this table to use a row count based on the column data ranges rather than the
     * <code>scale.row.count</code> property.
     * <p>
     * Note: This property will be ignored if <code>withRowCount()</code> is used or if no columns are defined with a
     * <code>descending</code> or <code>ascending</code> distribution
     * 
     * @param isFixed true to fix the row count, otherwise false (default)
     * @return this instance
     */
    public BenchTable withFixedRowCount(boolean isFixed) {
        this.isFixed = isFixed;
        return this;
    }

    /**
     * Set a default column data distribution to use for columns that have no distribution set.
     * 
     * @param distro the name of the generator distribution <code>random | ascending | descending | runlength</code>
     * @return this instance
     */
    public BenchTable withDefaultDistribution(String distro) {
        defaultDistro = distro;
        return this;
    }

    /**
     * Generate the table asynchronously through Kafka using Avro serialization
     */
    public void generateAvro() {
        columns.setDefaultDistribution(getDefaultDistro());
        var future = generateWithAvro();
        bench.addFuture(future);
    }

    /**
     * Generate the table asynchronously through Kafka using JSON serialization
     */
    public void generateJson() {
        columns.setDefaultDistribution(getDefaultDistro());
        var future = generateWithJson();
        bench.addFuture(future);
    }

    /**
     * Generate the table asynchronously through Kafka using Avro serialization
     */
    public void generateProtobuf() {
        columns.setDefaultDistribution(getDefaultDistro());
        var future = generateWithProtobuf();
        bench.addFuture(future);
    }

    /**
     * Generate the table synchronously to a parquet file in the engine's data directory. If a parquet file already
     * exists in the Deephaven data directory that matches this table definition, use it and skip generation.
     * 
     * @return true if file was generated, otherwise false
     */
    public boolean generateParquet() {
        columns.setDefaultDistribution(getDefaultDistro());
        String q = replaceTableAndGeneratorFields(useExistingParquetQuery);

        AtomicBoolean usedExistingParquet = new AtomicBoolean(false);
        bench.query(q).fetchAfter("used_existing_parquet_" + tableName, table -> {
            usedExistingParquet.set(table.getValue(0, "UsedExistingParquet").toString().equalsIgnoreCase("true"));
        }).execute();

        if (usedExistingParquet.get()) {
            Log.info("Using existing table '%s' with %s rows", tableName, getRowCount());
            return false;
        }
        Log.info("Generating table '%s' with %s rows", tableName, getRowCount());
        var timer = Timer.start();

        if (rowPauseMillis < 0)
            withRowPause(0, ChronoUnit.MILLIS);

        bench.awaitCompletion(generateWithAvro());
        Log.info("Produce Data Duration: " + timer.duration().toMillis());
        timer = Timer.start();

        q = replaceTableAndGeneratorFields(kafkaToParquetQuery);
        bench.query(q).execute();

        Log.info("DH Write Table Duration: " + timer.duration().toMillis());
        return true;
    }

    /**
     * Shutdown and cleanup any running generator
     */
    public void close() {
        if (generator != null)
            generator.close();
    }

    private Future<Metrics> generateWithAvro() {
        String bootstrapServer = bench.property("client.redpanda.addr", "localhost:9092");
        String schemaRegistry = "http://" + bench.property("client.schema.registry.addr", "localhost:8081");
        generator = new AvroKafkaGenerator(bootstrapServer, schemaRegistry, tableName, columns, getCompression());
        return generator.produce(getRowPause(), getRowCount(), getRunDuration());
    }

    private Future<Metrics> generateWithJson() {
        String bootstrapServer = bench.property("client.redpanda.addr", "localhost:9092");
        String schemaRegistry = "http://" + bench.property("client.schema.registry.addr", "localhost:8081");
        generator = new JsonKafkaGenerator(bootstrapServer, schemaRegistry, tableName, columns, getCompression());
        return generator.produce(getRowPause(), getRowCount(), getRunDuration());
    }

    private Future<Metrics> generateWithProtobuf() {
        String bootstrapServer = bench.property("client.redpanda.addr", "localhost:9092");
        String schemaRegistry = "http://" + bench.property("client.schema.registry.addr", "localhost:8081");
        generator = new ProtobufKafkaGenerator(bootstrapServer, schemaRegistry, tableName, columns, getCompression());
        return generator.produce(getRowPause(), getRowCount(), getRunDuration());
    }

    private int getRowPause() {
        if (rowPauseMillis >= 0)
            return rowPauseMillis;
        return (int) bench.propertyAsDuration("generator.pause.per.row", "1 millis").toMillis();
    }

    private String getDefaultDistro() {
        if (defaultDistro != null)
            return defaultDistro;
        return (String) bench.property("default.data.distribution", "random");
    }

    private long getRowCount() {
        if (rowCount > 0)
            return rowCount;

        long count = isFixed ? columns.getMaxValueCount() : 0;
        if (count > 0)
            return count;

        return bench.propertyAsIntegral("scale.row.count", "10000");
    }

    private int getRunDuration() {
        if (durationSecs >= 0)
            return durationSecs;
        return (int) bench.propertyAsDuration("default.completion.timeout", "1 minute").toSeconds();
    }

    private String getCompression() {
        String codec = (compression != null) ? compression : bench.property("record.compression", "NONE");
        return codec.trim().toUpperCase();
    }

    private String getColumnGrouping() {
        var cg = columnGrouping;
        return (cg == null || cg.length == 0) ? "None" : ("'" + String.join("','", columnGrouping) + "'");
    }

    private String getTableDefinition() {
        return "row.count=" + getRowCount() + "\n"
                + "compression=" + getCompression() + "\n"
                + "column.grouping=" + getColumnGrouping() + "\n"
                + columns.describe();
    }

    private String getTableDefinitionHash() {
        var hashCode = getTableDefinition().hashCode();
        return Numbers.toBase62(Math.abs(hashCode));
    }

    private String getTableDefinitionId() {
        var defHash = getTableDefinitionHash();
        return "benchmark." + Ids.uniqueName(defHash);
    }

    private String replaceTableAndGeneratorFields(String query) {
        query = generatorDefValues + query;

        String codec = getCompression();
        codec = codec.equals("NONE") ? "UNCOMPRESSED" : codec;
        codec = codec.equals("LZ4") ? "LZ4_RAW" : codec;
        String compression = String.format(", compression_codec_name='%s'", codec);

        return query.replace("${table.name}", tableName)
                .replace("${compression.codec}", compression)
                .replace("${max.dict.keys}", ", max_dictionary_keys=2000000")
                .replace("${max.dict.bytes}", ", max_dictionary_size=16777216")
                .replace("${target.page.bytes}", ", target_page_size=2097152")
                .replace("${table.columns}", columns.getQuotedColumns())
                .replace("${table.rowcount}", Long.toString(getRowCount()))
                .replace("${table.duration}", Long.toString(getRunDuration()))
                .replace("${table.definition}", getTableDefinition())
                .replace("${table.definition.id}", getTableDefinitionId())
                .replace("${table.definition.hash}", getTableDefinitionHash())
                .replace("${column.grouping}", getColumnGrouping());
    }

    static final String generatorDefValues = """
        # Define files and generator configuration
        table_parquet = '/data/${table.name}.parquet'
        table_gen_parquet = '/data/${table.definition.id}.gen.parquet'
        table_gen_def_text = '''${table.definition}'''
        table_gen_def_file = '/data/${table.definition.id}.gen.def'
        table_gen_glob = '/data/benchmark.${table.definition.hash}.*.*.gen.def'
        """;

    static final String useExistingParquetQuery = """
        # Determine if a Parquet file already exists that fits the table configuration
        import os, glob
        from deephaven import new_table
        from deephaven.column import string_col

        def findMatchingGenParquet(gen_def_text):
            for path in glob.glob(table_gen_glob):
                with open(path) as f:
                    if f.read() == gen_def_text:
                        return os.path.splitext(os.path.splitext(path)[0])[0]
            return None

        if os.path.exists(table_parquet):
            os.remove(table_parquet)

        usedExisting = False
        matching_gen_parquet = findMatchingGenParquet(table_gen_def_text)
        if matching_gen_parquet is not None and os.path.exists(str(matching_gen_parquet) + '.gen.parquet'):
            os.link(str(matching_gen_parquet) + '.gen.parquet', table_parquet)
            usedExisting = True

        used_existing_parquet_${table.name} = new_table([string_col("UsedExistingParquet", [str(usedExisting)])])
        """;

    static final String kafkaToParquetQuery = """
        # Create a Parquet file from a Kafka topic
        import jpy, os
        from deephaven import kafka_consumer as kc
        from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
        from deephaven.parquet import write
        from deephaven.table import Table
        from deephaven.update_graph import exclusive_lock

        ${table.name} = kc.consume(
            { 'bootstrap.servers' : '${kafka.consumer.addr}', 'schema.registry.url' : 'http://${schema.registry.addr}' },
            '${table.name}', partitions=None, offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
            key_spec=KeyValueSpec.IGNORE, value_spec=kc.avro_spec('${table.name}_record', schema_version='1'),
            table_type=TableType.append()).view(formulas=[${table.columns}])

        def wait_ticking_table_update(table: Table, row_count: int):
            with exclusive_lock(table):
                while table.size < row_count:
                    table.j_table.awaitUpdate()

        wait_ticking_table_update(${table.name}, ${table.rowcount})

        if os.path.exists(table_parquet):
            os.remove(table_parquet)

        with open(table_gen_def_file, 'w') as f:
            f.write(table_gen_def_text)
        column_grouping=${column.grouping}
        if column_grouping: ${table.name} = ${table.name}.sort([${column.grouping}])
        write(${table.name}, table_gen_parquet ${compression.codec} ${max.dict.keys} ${max.dict.bytes} ${target.page.bytes})
        os.link(table_gen_parquet, table_parquet)

        del ${table.name}

        from deephaven import garbage_collect
        garbage_collect()
        """;

}
