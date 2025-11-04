# MySQL ClickHouse Replicator - Claude Code Guide

## ⚠️ CRITICAL DATABASE RULES

**NEVER DELETE THE FINAL DATABASE (`mysql_ch_replicator_rematter_default`)**

The replication system uses a two-database strategy:
1. **Temporary Database** (`mysql_ch_replicator_rematter_default_tmp`): Initial replication target
2. **Final Database** (`mysql_ch_replicator_rematter_default`): Production database that gets swapped

**How It Works:**
- System replicates all tables to `_tmp` database
- Once complete, `_tmp` database is renamed to final database name
- The final database should persist across runs for real-time updates

**What You Can Delete:**
- ✅ `mysql_ch_replicator_rematter_default_tmp` - Safe to delete for fresh start
- ✅ State files in `./data/binlog/rematter_default/*.pckl` - Safe to delete for fresh start
- ❌ `mysql_ch_replicator_rematter_default` - **NEVER DELETE** - This is the production database

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

## 🧪 Testing Architecture - **FIXED PARALLEL EXECUTION & DATABASE ISOLATION**

### Test Organization

```
tests/
├── integration/           # End-to-end integration tests (65+ tests)
│   ├── data_types/       # MySQL data type replication
│   ├── ddl/              # DDL operation handling
│   ├── data_integrity/   # Data consistency validation
│   ├── edge_cases/       # Complex scenarios & bug reproductions
│   ├── percona/          # Percona MySQL specific tests
│   ├── performance/      # Stress testing & concurrent operations
│   ├── dynamic/          # Property-based testing scenarios
│   └── process_management/ # Process lifecycle & recovery
├── unit/                 # Unit tests (connection pooling, etc.)
├── base/                 # Reusable test base classes
├── fixtures/             # Test data and schema generators
├── utils/                # Test utilities and helpers
└── configs/              # Test configuration files
```

### Running Tests

**⚠️ CRITICAL**: Always use the test script for ALL test verification:
```bash
./run_tests.sh                    # Full parallel suite
./run_tests.sh --serial           # Sequential mode
./run_tests.sh -k "test_name"     # Specific tests
```

**✅ FIXED ISSUES**:
- **Directory Creation Race Conditions**: Fixed Docker volume mount issues with `/app/binlog/` directory
- **Connection Pool Configuration**: Updated all tests to use correct ports (9306/9307/9308)
- **Database Detection Logic**: Fixed timeout issues by detecting both final and `{db_name}_tmp` databases
- **Parallel Test Isolation**: Worker-specific paths and database names for safe parallel execution

**Current Status**: 126 passed, 47 failed, 11 skipped (68.5% pass rate)

### Key Infrastructure Achievements
- **Process Startup**: Enhanced timeout and retry logic for better reliability
- **Database Detection**: Improved handling of temporary to final database transitions  
- **Dynamic Isolation**: Complete parallel test safety with worker-specific databases
- **Error Handling**: Enhanced diagnostics and error reporting

**Infrastructure Status**: ✅ Complete parallel testing infrastructure operational

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

### Key Files & Documentation

- `mysql_ch_replicator/` - Core replication logic
- `tests/` - Comprehensive test suite with 65+ integration tests
- `tests/CLAUDE.md` - Complete testing guide with development patterns
- `TESTING_GUIDE.md` - Comprehensive testing documentation and best practices
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

## Task Master AI Instructions
**Import Task Master's development workflow commands and guidelines, treat as if import is in the main CLAUDE.md file.**
@./.taskmaster/CLAUDE.md
