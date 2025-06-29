# Release v0.0.87

## New Features

### 🎉 Customizable PARTITION BY Support for ClickHouse Tables

- **New Configuration Option**: Added `partition_bys` config section with database/table filtering capabilities (similar to existing `indexes` configuration)
- **Custom Expressions**: Override the default `intDiv(id, 4294967)` partitioning with user-defined partition logic
- **Snowflake ID Support**: Specifically addresses issues with Snowflake-style IDs creating excessive partitions that trigger `max_partitions_per_insert_block` limits
- **Time-based Partitioning**: Enable efficient time-based partitioning patterns like `toYYYYMM(created_at)`
- **Backward Compatible**: Maintains existing behavior when not configured

## Configuration Example

```yaml
partition_bys:
  - databases: '*'
    tables: ['orders', 'user_events']
    partition_by: 'toYYYYMM(created_at)'
  - databases: ['analytics']
    tables: ['*']
    partition_by: 'toYYYYMMDD(event_date)'
```

## Problem Solved

Fixes the issue where large Snowflake-style IDs (e.g., `1849360358546407424`) with default partitioning created too many partitions, causing replication failures due to ClickHouse's `max_partitions_per_insert_block` limit.

Users can now specify efficient partitioning strategies based on their data patterns and requirements.

## Tests

- Added comprehensive test coverage to verify custom partition functionality
- Ensures both default and custom partition behaviors work correctly
- Validates backward compatibility

---

**Closes**: #161
**Pull Request**: #164