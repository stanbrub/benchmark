/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.run;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.stream.IntStream;
import io.deephaven.benchmark.util.Filer;
import io.deephaven.benchmark.util.Numbers;

/**
 * Generates an SVG file from a template that contains variables (e.g. <code>${My Agg=>op_duration}</code>) referencing
 * benchmark data from the benchmark result produced by the {@code ResultSummary}. If the benchmark results summary
 * contains more than one run, the values for the newest run are used.
 * <p>
 * Note: This class is used to transform the specific template format defined in {@code benchmark-summary.template.svg}
 * against the {@code benchmark-results.csv} file. It is not general-purpose nor bulletproof.
 * 
 * @see {@code benchmark-summary.template.svg} for an example of a template that works.
 */
class SvgSummary {
    final String varRegex = "\\$\\{([^}]+)\\}";
    final Map<String, Row> benchmarks;
    final Map<String, Row> platformProps;
    final String svgTemplate;
    final Path outputDir;
    final Path svgFile;

    /**
     * Configure this generator to correlate benchmark data with a summary template
     * 
     * @param benchmarkCsv the benchmark result data referenced by the template
     * @param svgTemplate the template containing references to the benchmark data
     * @param svgFile the svg file path to produce
     */
    SvgSummary(URL platformCsv, URL benchmarkCsv, URL svgTemplate, Path svgFile) {
        this.benchmarks = readSummaryCsv(benchmarkCsv, "op_rate", true, "benchmark_name");
        this.platformProps = readSummaryCsv(platformCsv, "value", false, "origin", "name");
        this.svgTemplate = Filer.getURLText(svgTemplate);
        this.outputDir = svgFile.getParent();
        this.svgFile = svgFile;
    }

    /**
     * Generate SVG files based on benchmark results CSV applied to a template
     */
    void summarize() {
        if (!Files.exists(outputDir)) {
            System.out.println("Skipping SVG summary because of missing output directory: " + outputDir);
            return;
        }

        var template = replacePlatformVars(svgTemplate);
        var out = Pattern.compile(varRegex).matcher(template).replaceAll(match -> {
            var split = match.group(1).split("=>");
            if (split.length < 2)
                return "$0";
            var lookupName = split[0].trim();
            var columnName = split[1].trim();
            var platformProp = platformProps.get(lookupName);
            var benchmark = benchmarks.get(lookupName);
            if (platformProp != null)
                return platformProp.getValue(columnName);
            if (benchmark != null)
                return Numbers.formatNumber(benchmark.getValue(columnName));
            return "$0";

        });

        Filer.putFileText(svgFile, out);
    }

    private Map<String, Row> readSummaryCsv(URL csv, String sortColumn, boolean isNumber, String... keyColumns) {
        var header = new HashMap<String, Integer>();
        var groups = new HashMap<String, List<Row>>();
        var csvLines = Filer.getURLText(csv).lines().toList();
        for (int i = 0, n = csvLines.size(); i < n; i++) {
            String[] values = csvLines.get(i).split(",");
            if (i == 0) {
                IntStream.range(0, values.length).forEach(pos -> header.put(values[pos], pos));
            } else {
                var row = new Row(header, values, keyColumns);
                var group = groups.get(row.getVarName());
                if (group == null)
                    groups.put(row.getVarName(), group = new ArrayList<Row>());
                group.add(row);
            }
        }
        var benchmarks = new HashMap<String, Row>();
        var comparator = new RowComparator(sortColumn, isNumber);
        for (List<Row> group : groups.values()) {
            Collections.sort(group, comparator);
            var midRow = group.get(group.size() / 2);
            benchmarks.put(midRow.getVarName(), midRow);
        }
        return benchmarks;
    }

    private String replacePlatformVars(String str) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        str = str.replace("${run_date}", dtf.format(LocalDateTime.now()));
        str = str.replace("${os_name}", "Ubuntu 22.04.1 LTS".toLowerCase().replace(" ", "-"));
        return str.replace("${benchmark_count}", "" + benchmarks.size());
    }

    class RowComparator implements Comparator<Row> {
        final String sortKey;
        final boolean isNumber;

        RowComparator(String sortKey, boolean isNumber) {
            this.sortKey = sortKey;
            this.isNumber = isNumber;
        }

        @Override
        public int compare(Row r1, Row r2) {
            var v1 = r1.getValue(sortKey);
            var v2 = r2.getValue(sortKey);
            if (!isNumber || v1.isBlank() || v2.isBlank())
                return v1.compareTo(v2);
            var d1 = (Double) Numbers.parseNumber(v1).doubleValue();
            var d2 = (Double) Numbers.parseNumber(v2).doubleValue();
            return d1.compareTo(d2);
        }
    }

    record Row(Map<String, Integer> header, String[] values, String... varColumns) {
        String getValue(String colName) {
            Integer index = header.get(colName);
            if (index == null)
                throw new RuntimeException("Undefined benchmark column name: " + colName);
            return (index < values.length) ? values[index].trim() : "";
        }

        String getVarName() {
            var name = "";
            for (int i = 0, n = varColumns.length; i < n; i++) {
                var v = getValue(varColumns[i]);
                name += (i == 0) ? v : (">>" + v);
            }
            return name;
        }
    }

}
