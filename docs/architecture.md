# Architecture — Module Dependency & Data Flow

## Module Dependency Graph

```mermaid
graph TD
    CFG[config.py]

    subgraph core
        CLIENT[hl_client.py<br/>REST + WS wrapper]
        DENSITY[liq_density.py<br/>density map]
        COLLECTOR[ws_collector.py<br/>live streamer]
        SIGNAL[signal.py<br/>cluster detector]
    end

    subgraph analysis
        BACKTEST[backtest.py<br/>historical edge test]
        EDGETEST[edge_test.py<br/>live-data edge test]
    end

    subgraph storage
        RAW[(data/raw/<br/>minutely parquet)]
    end

    CFG --> CLIENT
    CFG --> COLLECTOR
    CFG --> DENSITY

    CLIENT --> COLLECTOR
    CLIENT --> BACKTEST

    COLLECTOR --> DENSITY
    COLLECTOR --> RAW

    DENSITY --> SIGNAL
    DENSITY --> EDGETEST

    RAW --> BACKTEST
    RAW --> EDGETEST

    SIGNAL -.->|future: strategy| STRAT[strategy.py<br/>⛔ not built until edge confirmed]
```

## Data Flow — Live Collection

```mermaid
sequenceDiagram
    participant WS as HL WebSocket
    participant COL as ws_collector
    participant DEN as liq_density
    participant SIG as signal
    participant DSK as data/raw/

    WS->>COL: trade / liquidation event (JSON)
    COL->>COL: validate + normalise fields
    COL->>DEN: update(price, liq_size, side)
    COL->>DSK: buffer → flush minutely parquet batch
    DEN->>SIG: snapshot(density_map)
    SIG-->>SIG: detect cluster formation
    SIG-->>SIG: detect price cross event
    Note over SIG: emits CrossEvent(symbol, cluster_level, direction, ts)
```

## Data Flow — Historical Backtest (Step 1)

```mermaid
sequenceDiagram
    participant REST as HL REST API
    participant CLI as hl_client
    participant BT as backtest
    participant DSK as data/raw/

    REST->>CLI: GET trades / funding snapshots
    CLI->>DSK: save raw parquet
    DSK->>BT: load N days of data
    BT->>BT: reconstruct liq_density proxy
    BT->>BT: label CrossEvents
    BT->>BT: chi-squared / binomial test
    BT-->>BT: report p-value, edge vs baseline
```

## Storage Layout

```
data/
└── raw/
    ├── trades/
    │   └── {symbol}/
    │       └── {YYYY-MM-DD}/
    │           └── {HH-MM}.parquet   # 1 file per minute
    └── liq_events/
        └── {symbol}/
            └── {YYYY-MM-DD}/
                └── {HH-MM}.parquet
```
