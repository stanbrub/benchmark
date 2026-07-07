# Training Dashboard

The training dashboard can be used to view the data collected from the GC effort so far. It is structure to be accessed via a python snippet that loads the dashboard and data from a GCloud bucket. This only works for a DHC docker image, or a native DHC/Python install that has a writable "/data" directory.

## Docker Compose Setup

The following is an example docker compose file.
```
services:
  deephaven:
    image: ghcr.io/deephaven/server:edge
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
      - ./minio:/minio
    environment:
      - "START_OPTS=-Xmx24G -Ddeephaven.console.type=python -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"
```

## Running the Training Dashboard

The following is a code snippet that runs the dashboard and data from the demo directory in the benchmarking GCloud bucket. It will cache the parquet files locally and use them to make the dashboards. (The "nfs" part is for detecting if we are running on a Deephaven Demo server, which doesn't have HTTP access.)

```
from urllib.request import urlopen; import os

root = 'file:///nfs' if os.path.exists('/nfs/deephaven-benchmark') else 'https://storage.googleapis.com'
with urlopen(f'{root}/deephaven-benchmark/train_dashboard.dh.py') as r:
    exec(r.read().decode(), globals(), locals())
```


