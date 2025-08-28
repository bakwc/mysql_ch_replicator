# Percona MySQL Integration Tests

## Overview

This directory contains integration tests specifically designed for Percona MySQL Server features and optimizations. These tests ensure that the MySQL ClickHouse Replicator works correctly with Percona-specific extensions and configurations.

## Test Coverage

### Performance Features
- **Query Response Time Plugin**: Tests replication compatibility with query performance monitoring
- **Slow Query Log Enhancement**: Validates replication with extended slow query logging
- **InnoDB Optimizations**: Tests transaction handling with Percona InnoDB improvements

### Security & Audit Features
- **Audit Log Plugin**: Ensures replication works with audit logging enabled
- **Enhanced Security**: Tests compatibility with Percona security features

### Storage Engine Features
- **InnoDB Enhancements**: Tests Percona-specific InnoDB optimizations
- **Character Set Handling**: Validates character set compatibility with Percona configurations
- **GTID Consistency**: Tests Global Transaction Identifier handling with Percona features

## Configuration

### Docker Compose Service
```yaml
percona_db:
  image: percona:8.4.3
  environment:
    MYSQL_DATABASE: admin
    MYSQL_ROOT_HOST: "%"
    MYSQL_ROOT_PASSWORD: admin
  ports:
    - "9308:3306"
  volumes:
    - ./tests/configs/docker/test_percona.cnf:/etc/mysql/my.cnf:ro
```

### Test Configuration
- **Config File**: `tests/configs/replicator/tests_config_percona.yaml`
- **Port**: 9308 (to avoid conflicts with MySQL and MariaDB)
- **Data Directory**: `/app/binlog_percona/`

## Running Percona Tests

### All Percona Tests
```bash
pytest tests/integration/percona/
```

### Specific Feature Tests
```bash
pytest tests/integration/percona/test_percona_features.py::TestPerconaFeatures::test_percona_audit_log_compatibility
```

### With Percona Configuration
```bash
./main.py --config tests/configs/replicator/tests_config_percona.yaml db_replicator --db test_db
```

## Test Scenarios

### 1. Audit Log Compatibility (`test_percona_audit_log_compatibility`)
- Creates basic table and inserts test data
- Verifies replication works with audit log enabled
- Validates data integrity across all records

### 2. Slow Query Log Compatibility (`test_percona_slow_query_log_compatibility`)
- Tests complex queries that might trigger slow query logging
- Uses JSON metadata and multiple indexes
- Ensures replication handles slow queries correctly

### 3. Query Response Time Plugin (`test_percona_query_response_time_compatibility`)
- Tests performance-sensitive table structures
- Uses different data sizes to test response time variations
- Validates replication with fulltext and performance indexes

### 4. InnoDB Optimizations (`test_percona_innodb_optimizations`)
- Tests large binary data and JSON transactions
- Validates batch processing with InnoDB optimizations
- Ensures transaction consistency during replication

### 5. GTID Consistency (`test_percona_gtid_consistency`)
- Tests Global Transaction Identifier handling
- Validates INSERT, UPDATE, DELETE operations with GTID
- Ensures transaction ordering with Percona GTID features

### 6. Character Set Handling (`test_percona_character_set_handling`)
- Tests multiple character sets (utf8mb4, latin1)
- Validates Unicode, emoji, and special character preservation
- Ensures collation compatibility with Percona configurations

## Best Practices

### Test Design
1. **Percona-Specific**: Focus on features unique to Percona MySQL
2. **Performance Testing**: Include performance-sensitive scenarios
3. **Security Integration**: Test security features alongside replication
4. **Real-World Scenarios**: Use realistic data patterns and operations

### Configuration Management
1. **Isolated Environment**: Use dedicated port and data directory
2. **Feature Flags**: Enable/disable Percona features as needed
3. **Performance Tuning**: Include Percona-optimized settings

### Error Handling
1. **Plugin Dependencies**: Handle missing Percona plugins gracefully
2. **Version Compatibility**: Test across different Percona versions
3. **Feature Detection**: Verify feature availability before testing

## Troubleshooting

### Common Issues
- **Plugin Not Available**: Some Percona features may not be enabled
- **Configuration Conflicts**: Ensure Percona-specific settings are correct
- **Performance Variations**: Test results may vary with different hardware

### Debugging
- Check Percona MySQL error logs: `SHOW GLOBAL VARIABLES LIKE 'log_error';`
- Verify plugin status: `SHOW PLUGINS;`
- Monitor performance: `SELECT * FROM INFORMATION_SCHEMA.QUERY_RESPONSE_TIME;`

### Health Checks
- Percona service health: `mysqladmin ping`
- Plugin availability: `SHOW PLUGINS LIKE '%audit%';`
- Replication status: Check binlog and GTID positions

## Integration with Main Test Suite

These tests are automatically included when running the full test suite:
```bash
./run_tests.sh  # Includes Percona tests
```

The Percona database service is added to the docker-compose-tests.yaml and will start alongside MySQL, MariaDB, and ClickHouse during testing.

## Future Enhancements

- **XtraDB Features**: Add tests for XtraDB-specific optimizations
- **Percona Toolkit**: Integration tests with Percona Toolkit utilities
- **ProxySQL Integration**: Tests with ProxySQL load balancing
- **Percona Monitoring**: Integration with Percona Monitoring and Management