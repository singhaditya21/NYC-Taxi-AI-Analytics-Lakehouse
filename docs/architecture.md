# Medallion Lakehouse Architecture
- **Bronze**: Raw parquets ingested daily or streamed via Kafka.
- **Silver**: Cleansed events, dropped nulls.
- **Gold**: dbt aggregates for AI consumption.\n