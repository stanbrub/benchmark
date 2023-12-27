package io.deephaven.benchmark.tests.standard.file;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

/**
 * Standard tests for writing/reading multi-column data with different codec/compression. To save time, the parquet
 * generated by the "write" tests is used by the "read" tests.
 */
@TestMethodOrder(OrderAnnotation.class)
class ParquetMultiColTest {
    final String[] usedColumns = {"str10K", "long10K", "int10K", "short10K", "bigDec10K", "intArr5", "intVec5"};
    final FileTestRunner runner = new FileTestRunner(this);

    @BeforeEach
    void setup() {
        runner.setScaleFactors(5, 1);
    }

    @Test
    @Order(1)
    void writeMultiColSnappy() {
        runner.runParquetWriteTest("ParquetWrite- Snappy Multi Col -Static", "SNAPPY", usedColumns);
    }

    @Test
    @Order(2)
    void readMultiColSnappy() {
        runner.runParquetReadTest("ParquetRead- Snappy Multi Col -Static");
    }

    @Test
    @Order(3)
    void writeMultiColZstd() {
        runner.runParquetWriteTest("ParquetWrite- Zstd Multi Col -Static", "ZSTD", usedColumns);
    }

    @Test
    @Order(4)
    void readMultiColZstd() {
        runner.runParquetReadTest("ParquetRead- Zstd Multi Col -Static");
    }

    @Test
    @Order(5)
    void writeMultiColLzo() {
        runner.runParquetWriteTest("ParquetWrite- Lzo Multi Col -Static", "LZO", usedColumns);
    }

    @Test
    @Order(6)
    void readMultiColLzo() {
        runner.runParquetReadTest("ParquetRead- Lzo Multi Col -Static");
    }

    @Test
    @Order(7)
    void writeMultiColLz4Raw() {
        runner.runParquetWriteTest("ParquetWrite- Lz4Raw Multi Col -Static", "LZ4_RAW", usedColumns);
    }

    @Test
    @Order(8)
    void readMultiColLz4Raw() {
        runner.runParquetReadTest("ParquetRead- Lz4Raw Multi Col -Static");
    }

    @Test
    @Order(9)
    void writeMultiColGzip() {
        runner.runParquetWriteTest("ParquetWrite- Gzip Multi Col -Static", "GZIP", usedColumns);
    }

    @Test
    @Order(10)
    void readMultiColGzip() {
        runner.runParquetReadTest("ParquetRead- Gzip Multi Col -Static");
    }

    @Test
    @Order(11)
    void writeMultiColNone() {
        runner.runParquetWriteTest("ParquetWrite- No Codec Multi Col -Static", "NONE", usedColumns);
    }

    @Test
    @Order(12)
    void readMultiColNone() {
        runner.runParquetReadTest("ParquetRead- No Codec Multi Col -Static");
    }

    @Test
    @Order(13)
    void writeMultiColDefaultSnappy() {
        runner.useParquetDefaultSettings();
        runner.runParquetWriteTest("ParquetWrite- Snappy Multi Col Defaults -Static", "SNAPPY", usedColumns);
    }

    @Test
    @Order(14)
    void readMultiColDefaultSnappy() {
        runner.useParquetDefaultSettings();
        runner.runParquetReadTest("ParquetRead- Snappy Multi Col Defaults -Static");
    }

}