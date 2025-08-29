# MySQL ClickHouse Replicator - Claude Code Guide

## Overview

This project is a real-time replication system that synchronizes data from MySQL databases to ClickHouse for analytics and reporting. The replicator uses MySQL binary logs (binlog) to capture changes and applies them to ClickHouse tables with appropriate schema transformations.

## 🏗️ Project Architecture

### Core Components

- **Binlog Replicator**: Reads MySQL binary logs and captures change events
- **Database Replicator**: Processes events and applies changes to ClickHouse
- **Schema Manager**: Handles DDL operations and schema evolution
- **Connection Pools**: Manages database connections efficiently
- **State Management**: Tracks replication position for resume capability

### Key Technologies

- **Python 3.12** - Primary development language
- **MySQL 8.0+** - Source database (also supports MariaDB/Percona)
- **ClickHouse 25.7+** - Target analytics database
- **Docker Compose** - Development and testing environment
- **PyTest** - Testing framework with 65+ integration tests

## 🧪 Testing Architecture

### Test Organization

```
tests/
├── integration/           # End-to-end integration tests
│   ├── data_types/       # MySQL data type replication
│   ├── ddl/              # DDL operation handling
│   ├── data_integrity/   # Data consistency validation
│   ├── edge_cases/       # Complex scenarios & bug reproductions
│   ├── percona/          # Percona MySQL specific tests
│   └── process_management/ # Process lifecycle & recovery
├── base/                 # Reusable test base classes
├── fixtures/             # Test data and schema generators
├── utils/                # Test utilities and helpers
└── configs/              # Test configuration files
```

### Running Tests

**⚠️ CRITICAL**: Always use the test script when ready to test the full suite after fixing little tests:
```bash
./run_tests.sh
```

**Never run individual pytest commands** - the script handles Docker container setup, database initialization, and cleanup.

### Recent Test Fixes Applied

The following critical issues were identified and resolved:

1. **DDL Syntax Compatibility**: Fixed `IF NOT EXISTS` syntax errors in MySQL DDL operations
2. **ENUM Value Handling**: Resolved ENUM normalization issues in replication
3. **Race Conditions**: Fixed IndexError in data synchronization waits
4. **Database Context**: Corrected database mapping and context issues
5. **State Recovery**: Improved error handling for corrupted state files

## 📊 Data Type Support

### Supported MySQL Types

- **Numeric**: INT, BIGINT, DECIMAL, FLOAT, DOUBLE (including UNSIGNED variants)
- **String**: VARCHAR, TEXT, LONGTEXT with full UTF-8 support
- **Date/Time**: DATE, DATETIME, TIMESTAMP with timezone handling
- **JSON**: Native JSON column support with complex nested structures
- **Binary**: BINARY, VARBINARY, BLOB with proper encoding
- **Enums**: ENUM values (normalized to lowercase in ClickHouse)
- **Geometric**: Limited support for POLYGON and spatial types

### ClickHouse Mapping

The replicator automatically maps MySQL types to appropriate ClickHouse equivalents:
- `INT` → `Int32`
- `BIGINT` → `Int64` 
- `VARCHAR(n)` → `String`
- `JSON` → `String` (with JSON parsing)
- `ENUM` → `String` (normalized to lowercase)

## 🔧 Development Workflow

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Git

### Setup Development Environment

```bash
# Clone repository
git clone <repository-url>
cd mysql-ch-replicator

# Build and start services
docker-compose up -d

# Run tests to verify setup
./run_tests.sh
```

### Making Changes

1. **Branch Strategy**: Create feature branches from `master`
2. **Testing**: Run `./run_tests.sh` before and after changes
3. **Code Style**: Follow existing patterns and conventions
4. **Documentation**: Update relevant docs and comments

### Configuration

The replicator uses YAML configuration files:

```yaml
# Example configuration
mysql:
  host: localhost
  port: 3306
  user: root
  password: admin

clickhouse:
  host: localhost
  port: 9123
  database: analytics

replication:
  resume_stream: true
  initial_only: false
  include_tables: ["user_data", "transactions"]
```

## 🚀 Deployment

### Docker Deployment

The project includes production-ready Docker configurations:

```yaml
# docker-compose.yml excerpt
services:
  mysql-ch-replicator:
    build: .
    environment:
      - CONFIG_PATH=/app/config/production.yaml
    volumes:
      - ./config:/app/config
      - ./data:/app/data
    depends_on:
      - mysql
      - clickhouse
```

### Health Monitoring

The replicator exposes health endpoints:
- `GET /health` - Overall service health
- `GET /metrics` - Replication metrics and statistics
- `POST /restart_replication` - Manual restart trigger

## 🐛 Troubleshooting

### Common Issues

**Replication Lag**:
- Check MySQL binlog settings
- Monitor ClickHouse insertion performance
- Verify network connectivity

**Schema Mismatches**:
- Review DDL replication logs
- Check column type mappings
- Validate character set configurations

**Connection Issues**:
- Verify database connectivity
- Check connection pool settings
- Review authentication credentials

### Debugging

Enable debug logging:
```yaml
logging:
  level: DEBUG
  handlers:
    - console
    - file
```

Inspect state files:
```bash
# Check replication position
cat data/state.json

# Review process logs
tail -f logs/replicator.log
```

## 📈 Performance Optimization

### MySQL Configuration

```sql
-- Enable binlog for replication
SET GLOBAL log_bin = ON;
SET GLOBAL binlog_format = ROW;
SET GLOBAL binlog_row_image = FULL;
```

### ClickHouse Tuning

```sql
-- Optimize for analytics workloads  
SET max_threads = 8;
SET max_memory_usage = 10000000000;
SET allow_experimental_window_functions = 1;
```

### Monitoring Metrics

Key metrics to monitor:
- **Replication Lag**: Time delay between MySQL write and ClickHouse availability
- **Event Processing Rate**: Events processed per second
- **Error Rate**: Failed operations per time period
- **Memory Usage**: Peak and average memory consumption

## 🔒 Security Considerations

### Database Security

- Use dedicated replication users with minimal privileges
- Enable SSL/TLS connections
- Regularly rotate credentials
- Monitor access logs

### Network Security

- Use private networks for database connections
- Implement firewall rules
- Consider VPN for remote deployments
- Monitor network traffic

## 📚 Additional Resources

### Key Files

- `mysql_ch_replicator/` - Core replication logic
- `tests/` - Comprehensive test suite
- `docker-compose-tests.yaml` - Test environment setup
- `run_tests.sh` - Primary test execution script

### External Dependencies

- `mysql-connector-python` - MySQL database connectivity
- `clickhouse-connect` - ClickHouse client library  
- `PyMySQL` - Alternative MySQL connector
- `pytest` - Testing framework

### Development Standards

- **Code Coverage**: Aim for >90% test coverage
- **Documentation**: Document all public APIs
- **Error Handling**: Comprehensive error recovery
- **Logging**: Structured logging for observability

---

This system provides robust, real-time replication from MySQL to ClickHouse with comprehensive testing, error handling, and monitoring capabilities. For questions or contributions, please refer to the project repository and existing test cases for examples.