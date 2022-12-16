package io.deephaven.verify.producer;

import java.util.Map;
import java.util.concurrent.Future;

import io.deephaven.verify.util.Metrics;

public interface Generator {
	public Future<Metrics> produce(int perRecordPauseMillis, long maxRecordCount, int maxDurationSecs);
	public void produce(Map<String,Object> row);
	public void close();
}
