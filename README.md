# Verify #

Verify is a framework designed to work from the command line and through popular Java IDEs using the JUnit framework as a runner.  It is geared towards scale testing interfaces capable of ingesting table data, transforming it, and returning table data.  It represents a follow-on (not a replacement) to the [Bencher Project](https://github.com/deephaven/bencher) that benchmarks many of the query features of [Deephaven Core Community](https://deephaven.io/community/).

For the present, tests are geared towards testing [Deephaven Core Community](https://deephaven.io/community/) through the Barrage Java Client.  Tests focus on querying static parquet files, streamed Kafka topics, replayed data.

The typical workflow of a Verify test is... *Configure table/column generation* --> *Execute Query* --> *Measure Timed Result Retrieval*.  This is done inside a Junit test class.

Tests are designed to scale by change a scale property value, so the same test can be used for multiple runs at scale for comparison.  For ease of comparison, collected results are rates rather than elapsed time.  Below are some results that have been collected against a Docker install of Deephaven Community Core engine and Redpanda/Kafka service.

Test setup:
- Windows 11 with 64G RAM and 16 CPU threads
- WSL 2 limited to 44G RAM and 12 CPU threads
- Bencher runs both test and Engine/Redpanda in WSL
- Verify runs test on Windows and Engine/Redpanda in WSL
- None of the tests appear to be CPU-bound
- Verify uses ZSTD compression for Kafka producer and also writing parquet files

The first table represents data collected for the same queries using Bencher and Verify.

|test-description|bencher-rate|verify-rate|
|----------------|------------|-----------|
|stock join 10m auto increment|339430.53|363266.50|
|stock join 10m parquet view|579667.06|569670.70|
|stock join 100m auto increment|358337.90|398665.25|
|stock join 100m parquet view|579667.06|726485.50|


The second table represents data produced to a Kafka broker and consumed by the Deephaven query script.

|test-description|verify-rate|producer-rate|
|----------------|-----------|-------------|
|stock join 10m kafka append|224411.48|415973.38|
|stock join 100m kafka append|275232.62|384831.48|
|consumer count kafka append|441027.97|442944.34|




