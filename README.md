# mysql_ch_replicator
![tests](https://github.com/bakwc/mysql_ch_replicator/actions/workflows/tests.yaml/badge.svg)
[![Release][release-image]][releases]
[![License][license-image]][license]

[release-image]: https://img.shields.io/github/v/release/bakwc/mysql_ch_replicator?style=flat
[releases]: https://github.com/bakwc/mysql_ch_replicator/releases

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license]: LICENSE

![img](https://raw.githubusercontent.com/bakwc/mysql_ch_replicator/master/mysql_ch.jpg)

`mysql_ch_replicator` is a powerful and efficient tool designed for real-time replication of MySQL databases to ClickHouse.

With a focus on high performance, it utilizes batching heavily and uses C++ extension for faster execution. This tool ensures seamless data integration with support for migrations, schema changes, and correct data management.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
  - [Requirements](#requirements)
  - [Installation](#installation-1)
  - [Docker Installation](#docker-installation)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [One Time Data Copy](#one-time-data-copy)
  - [Configuration](#configuration)
    - [Required settings](#required-settings)
    - [Optional settings](#optional-settings)
  - [Advanced Features](#advanced-features)
    - [Migrations & Schema Changes](#migrations--schema-changes)
    - [Recovery Without Downtime](#recovery-without-downtime)
- [Development](#development)
  - [Running Tests](#running-tests)
- [Contribution](#contribution)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Features

- **Real-Time Replication**: Keeps your ClickHouse database in sync with MySQL in real-time.
- **High Performance**: Utilizes batching and ports slow parts to C++ (e.g., MySQL internal JSON parsing) for optimal performance (±20K events / second on a single core).
- **Supports Migrations/Schema Changes**: Handles adding, altering, and removing tables without breaking the replication process (*for most cases, [details here](https://github.com/bakwc/mysql_ch_replicator#migrations--schema-changes)).
- **Recovery without Downtime**: Allows for preserving old data while performing initial replication, ensuring continuous operation.
- **Correct Data Removal**: Unlike MaterializedMySQL, `mysql_ch_replicator` ensures physical removal of data.
- **Comprehensive Data Type Support**: Accurately replicates most data types, including JSON, booleans, and more. Easily extensible for additional data types.
- **Multi-Database Handling**: Replicates the binary log once for all databases, optimizing the process compared to `MaterializedMySQL`, which replicates the log separately for each database.

## Installation
### Requirements
 - Linux / MacOS
 - python3.9 or higher

### Installation

To install `mysql_ch_replicator`, use the following command:

```bash
pip install mysql_ch_replicator
```

You may need to also compile C++ components if they're not pre-built for your platform.

### Docker Installation

Alternatively, you can use the pre-built Docker image from DockerHub:

```bash
docker pull fippo/mysql-ch-replicator:latest
```

To run the container:

```bash
docker run -d \
  -v /path/to/your/config.yaml:/app/config.yaml \
  -v /path/to/your/data:/app/data \
  fippo/mysql-ch-replicator:latest \
  --config /app/config.yaml run_all
```

Make sure to:
1. Mount your configuration file using the `-v` flag
2. Mount a persistent volume for the data directory
3. Adjust the paths according to your setup

## Usage

### Basic Usage

For realtime data sync from MySQL to ClickHouse:

1. Prepare config file. Use `example_config.yaml` as an example.
2. Configure MySQL and ClickHouse servers:
 - MySQL server configuration file `my.cnf` should include following settings (required to write binary log in raw format, and enable password authentication):
<details>
  <summary>🛠 MySQL Config</summary>

```ini
[mysqld]
# ... other settings ...
gtid_mode = on
enforce_gtid_consistency = 1
binlog_expire_logs_seconds = 864000
max_binlog_size            = 500M
binlog_format              = ROW
```
 - For MariaDB use following settings:
```ini
[mysqld]
# ... other settings ...
gtid_strict_mode = ON
gtid_domain_id = 0
server_id = 1
log_bin = /var/log/mysql/mysql-bin.log
binlog_expire_logs_seconds = 864000
max_binlog_size = 500M
binlog_format = ROW
```

For `AWS RDS` you need to set following settings in `Parameter groups`:

```
binlog_format                       ROW
binlog_expire_logs_seconds          86400
```

</details>

 - ClickHouse server config `override.xml` should include following settings (it makes clickhouse apply final keyword automatically to handle updates correctly):

 <details>
   <summary>🛠 ClickHouse Config</summary>
 
```xml
<clickhouse>
    <!-- ... other settings ... -->
    <profiles>
        <default>
            <!-- ... other settings ... -->
            <final>1</final>
            <max_query_size>300000000</max_query_size>
            <max_ast_elements>1000000</max_ast_elements>
            <max_expanded_ast_elements>1000000</max_expanded_ast_elements>
        </default>
    </profiles>
</clickhouse>
```

**!!! Double check final setting is applied !!!**

Execute the following command in clickhouse:

`SELECT name, value, changed FROM system.settings WHERE name = 'final'`
Setting should be set to 1. If not, you should:
 * double check the `override.xml` is applied
 * try to modify `users.xml` instead
</details>

3. Start the replication:

```bash
mysql_ch_replicator --config config.yaml run_all
```

This will keep data in ClickHouse updating as you update data in MySQL. It will always be in sync.

### One Time Data Copy

If you just need to copy data once, and don't need continuous synchronization for all changes, you should do following:

1. Prepare config file. Use `example_config.yaml` as an example.
2. Run one-time data copy:

```bash
mysql_ch_replicator --config config.yaml db_replicator --db mysql_db_name --initial_only=True
```
Where `mysql_db_name` is the name of the database you want to copy.

Don't be afraid to interrupt process in the middle. It will save the state and continue copy after restart.

__Hint__: _set `initial_replication_threads` to a number of cpu cores to accelerate initial replication_

### Configuration

`mysql_ch_replicator` can be configured through a configuration file. Here is the config example:

```yaml
mysql:
  host: 'localhost'
  port: 8306
  user: 'root'
  password: 'root'

clickhouse:
  host: 'localhost'
  port: 8323
  user: 'default'
  password: 'default'
  connection_timeout: 30        # optional
  send_receive_timeout: 300     # optional

binlog_replicator:
  data_dir: '/home/user/binlog/'  # a new EMPTY directory (for internal storage of data by mysql_ch_replicator itself)
  records_per_file: 100000
  binlog_retention_period: 43200  # optional, how long to keep binlog files in seconds, default 12 hours

databases: 'database_name_pattern_*'
tables: '*'


# OPTIONAL SETTINGS

initial_replication_threads: 4                        # optional

exclude_databases: ['database_10', 'database_*_42']   # optional
exclude_tables: ['meta_table_*']                      # optional

target_databases:                                     # optional
  source_db_in_mysql_1: destination_db_in_clickhouse_1
  source_db_in_mysql_2: destination_db_in_clickhouse_2
  ...

log_level: 'info'               # optional       
optimize_interval: 86400        # optional
auto_restart_interval: 3600     # optional

indexes:                        # optional
  - databases: '*'
    tables: ['test_table']
    index: 'INDEX name_idx name TYPE ngrambf_v1(5, 65536, 4, 0) GRANULARITY 1'

partition_bys:                  # optional
  - databases: '*'
    tables: ['test_table']
    partition_by: 'toYYYYMM(created_at)'

http_host: '0.0.0.0'    # optional
http_port: 9128         # optional

types_mapping:          # optional
  'char(36)': 'UUID'

ignore_deletes: false    # optional, set to true to ignore DELETE operations

mysql_timezone: 'UTC'    # optional, timezone for MySQL timestamp conversion (default: 'UTC')

```

#### Required settings

- `mysql` MySQL connection settings
- `clickhouse` ClickHouse connection settings
- `binlog_replicator.data_dir` Create a new empty directory, it will be used by script to store it's state
- `databases` Databases name pattern to replicate, e.g. `db_*` will match `db_1` `db_2` `db_test`, list is also supported

#### Optional settings
- `initial_replication_threads` - number of threads for initial replication, by default 1, set it to number of cores to accelerate initial data copy
- `tables` - tables to filter, list is also supported
- `exclude_databases` - databases to __exclude__, string or list, eg `'table1*'` or `['table2', 'table3*']`. If same database matches `databases` and `exclude_databases`, exclude has higher priority.
- `exclude_tables` - databases to __exclude__, string or list. If same table matches `tables` and `exclude_tables`, exclude has higher priority.
- `target_databases` - if you want database in ClickHouse to have different name from MySQL database
- `log_level` - log level, default is `info`, you can set to `debug` to get maximum information (allowed values are `debug`, `info`, `warning`, `error`, `critical`)
- `optimize_interval` - interval (seconds) between automatic `OPTIMIZE table FINAL` calls. Default 86400 (1 day). This is required to perform all merges guaranteed and avoid increasing of used storage and decreasing performance.
- `auto_restart_interval` - interval (seconds) between automatic db_replicator restart. Default 3600 (1 hour). This is done to reduce memory usage.
- `binlog_retention_period` - how long to keep binlog files in seconds. Default 43200 (12 hours). This setting controls how long the local binlog files are retained before being automatically cleaned up.
- `indexes` - you may want to add some indexes to accelerate performance, eg. ngram index for full-test search, etc. To apply indexes you need to start replication from scratch.
- `partition_bys` - custom PARTITION BY expressions for tables. By default uses `intDiv(id, 4294967)` for integer primary keys. Useful for time-based partitioning like `toYYYYMM(created_at)`.
- `http_host`, `http_port` - http endpoint to control replication, use `/docs` for abailable commands
- `types_mappings` - custom types mapping, eg. you can map char(36) to UUID instead of String, etc.
- `ignore_deletes` - when set to `true`, DELETE operations in MySQL will be ignored during replication. This creates an append-only model where data is only added, never removed. In this mode, the replicator doesn't create a temporary database and instead replicates directly to the target database.
- `mysql_timezone` - timezone to use for MySQL timestamp conversion to ClickHouse DateTime64. Default is `'UTC'`. Accepts any valid timezone name (e.g., `'America/New_York'`, `'Europe/London'`, `'Asia/Tokyo'`). This setting ensures proper timezone handling when converting MySQL timestamp fields to ClickHouse DateTime64 with timezone information.

Few more tables / dbs examples:

```yaml
databases: ['my_database_1', 'my_database_2']
tables: ['table_1', 'table_2*']
```

### Advanced Features

#### Migrations & Schema Changes

`mysql_ch_replicator` supports the following:

- **Adding Tables**: Automatically starts replicating data from newly added tables.
- **Altering Tables**: Adjusts replication strategy based on schema changes.
- **Removing Tables**: Handles removal of tables without disrupting the replication process.

**WARNING**. While 95% of operations supported, there could be still some unhandled operations. We try to support all of them, but for your safety, please write the CI/CD test that will check your migrations. Test should work a following way:
 - Aplly all your mysql migrations
 - Try to insert some record into mysql (to any table)
 - Check that this record appears in ClickHouse

**Known Limitations**
1. Migrations not supported during initial replication. You should either wait for initial replication finish and then apply migrations, or restart initial replication from scratch (by removing state file).
2. Primary key changes not supported. This is a ClickHouse level limitation, it does not allow to make any changes realted to primary key.

#### Recovery Without Downtime

In case of a failure or during the initial replication, `mysql_ch_replicator` will preserve old data and continue syncing new data seamlessly. You could remove the state and restart replication from scratch.

## Development

To contribute to `mysql_ch_replicator`, clone the repository and install the required dependencies:

```bash
git clone https://github.com/your-repo/mysql_ch_replicator.git
cd mysql_ch_replicator
pip install -r requirements.txt
```

### Running Tests

1. Use docker-compose to install all requirements:
```bash
sudo docker compose -f docker-compose-tests.yaml up
```
2. Run tests with:
```bash
sudo docker exec -w /app/ -it mysql_ch_replicator-replicator-1 python3 -m pytest -v -s test_mysql_ch_replicator.py
```
3. To run a single test:
```bash
sudo docker exec -w /app/ -it mysql_ch_replicator-replicator-1 python3 -m pytest -v -s test_mysql_ch_replicator.py -k test_your_test_name
```

## Contribution

Contributions are welcome! Please open an issue or submit a pull request for any bugs or features you would like to add.

## License

`mysql_ch_replicator` is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Acknowledgements

Thank you to all the contributors who have helped build and improve this tool.
