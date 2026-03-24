/* Copyright (c) 2022-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.generator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import io.deephaven.benchmark.metric.Metrics;
import io.deephaven.benchmark.util.Log;
import io.deephaven.benchmark.util.Threads;
import blue.strategic.parquet.*;

/**
 * Generator that produces rows to a local Parquet file according to the provided column definitions.<pr/> Note: This
 * generator MUST generate the same row and column data in the same order and types as the non-local
 * <code>AvroKafkaGenerator</code> when the two generators have the same column definitions. (The "same data" is defined
 * by how it looks in Deephaven tables, not byte-for-byte in the files.)
 */
public class LocalParquetGenerator implements Generator {
    final private ExecutorService queue = Threads.single("LocalParquetGenerator");
    final private Path parquetOut;
    final private ParquetWriter<Row> writer;
    final private ColumnDefs columnDefs;
    final private String topic;
    final private MessageType schema;
    final private AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Initialize with kafka server and schema registry locations, kafka topic, column definitions, and compression
     * scheme
     * 
     * @param bootstrapServers the kafka external location (ex. localhost:9092)
     * @param schemaRegistryUrl the ReST schema registry location (ex. localhost:8081)
     * @param topic the kafka topic to produce record to (ex. mytable)
     * @param columnDefs the column definitions specifying what the data looks like
     * @param compression one of Kafka's <code>ProducerConfig.COMPRESSION_TYPE_CONFIG</code> schemes
     */
    public LocalParquetGenerator(String parquetFile, String topic, ColumnDefs columnDefs, String compression) {
        this.topic = topic;
        this.columnDefs = columnDefs;
        this.schema = MessageTypeParser.parseMessageType(getSchemaMessage(topic, columnDefs));
        this.parquetOut = Paths.get(parquetFile);
        this.writer = createParquetWriter(schema, parquetOut);
    }

    /**
     * Produce a maximum number of records to a Kafka topic asynchronously.
     * 
     * @param perRecordPauseMillis wait time between each record sent
     * @param maxRecordCount maximum records to produce
     * @param maxDurationSecs maximum duration to produce (May prevent maximum records from being produces)
     */
    public Future<Metrics> produce(int perRecordPauseMillis, long maxRecordCount, int maxDurationSecs) {
        checkClosed();
        var r = new Callable<Metrics>() {
            @Override
            public Metrics call() {
                final long maxDuration = maxDurationSecs * 1000;
                final long beginTime = System.currentTimeMillis();
                long recCount = 0;
                long duration = 0;
                boolean isDone = false;
                var rec = new Row(schema, new ArrayList<>(columnDefs.getCount()));
                while (!isClosed.get() && !isDone) {
                    try {
                        if (recCount >= maxRecordCount) {
                            isDone = true;
                            continue;
                        }
                        // Build a record with the column defs for Parquet row write
                        for (int i = 0, n = columnDefs.getCount(); i < n; i++) {
                            var v = columnDefs.nextValue(i, recCount, maxRecordCount);
                            rec.addValue(v);
                        }
                        // Write the record to Parquet file
                        writer.write(rec);
                        rec.clear();

                        if (++recCount % 10_000_000 == 0)
                            Log.info("Produced %s records to topic '%s'", recCount, topic);
                        duration = System.currentTimeMillis() - beginTime;
                        if (duration > maxDuration)
                            isDone = true;
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to write to topic: " + topic, ex);
                    }
                }
                Log.info("Produced %s records to topic: %s", recCount, topic);
                var metrics = new Metrics("test-runner", "generate." + topic).set("duration.secs", duration / 1000.0)
                        .set("record.count", recCount).set("send.rate", recCount / (duration / 1000.0));
                return metrics;
            }
        };
        return queue.submit(r);
    }

    /**
     * Close the producer and shutdown any async threads created during production
     */
    public void close() {
        if (isClosed.get())
            return;
        isClosed.set(true);
        queue.shutdown();
        try {
            writer.close();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to close Parquet writer for topic: " + topic, ex);
        }
    }

    private void checkClosed() {
        if (isClosed.get())
            throw new RuntimeException("Generator is closed");
    }

    private ParquetWriter<Row> createParquetWriter(MessageType schema, Path parquetOut) {
        try {
            Dehydrator<Row> dehydrator = (row, valueWriter) -> {
                row.write(valueWriter);
            };
            return ParquetWriter.writeFile(schema, parquetOut.toFile(), dehydrator);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create Parquet writer for topic: " + topic, ex);
        }
    }

    private String getSchemaMessage(String topic, ColumnDefs fieldDefs) {
        var schema = """
        message ${topic} {
            ${fields}
        }
        """;
        var fields = "";
        for (Map.Entry<String, String> e : fieldDefs.toTypeMap().entrySet()) {
            var name = e.getKey();
            var type = e.getValue();
            fields += String.format("required %s %s %s;\n", getFieldType(type), name, getCharEncoding(type));
        }
        schema = schema.replace("${topic}", topic);
        return schema.replace("${fields}", fields);
    }

    private String getFieldType(String type) {
        return switch (type) {
            case "long" -> "int64";
            case "int" -> "int32";
            case "double" -> "double";
            case "float" -> "float";
            case "string" -> "binary";
            case "timestamp-millis" -> "google.protobuf.Timestamp";
            default -> throw new RuntimeException("Unsupported generator data type: " + type);
        };
    }

    private String getCharEncoding(String type) {
        return switch (type) {
            case "string" -> "(UTF8)";
            default -> "";
        };
    }

    record Row(MessageType schema, List<Object> values) {
        public void addValue(Object value) {
            values.add(value);
        }

        public void write(ValueWriter valueWriter) {
            for (int i = 0, n = values.size(); i < n; i++) {
                valueWriter.write(schema.getFieldName(i), values.get(i));
            }
        }

        public void clear() {
            values.clear();
        }
    }

}
