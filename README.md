# mysql_ch_replicator
![tests](https://github.com/bakwc/mysql_ch_replicator/actions/workflows/tests.yaml/badge.svg)
[![Release][release-image]][releases]
[![License][license-image]][license]

[release-image]: https://img.shields.io/badge/release-0.0.35-blue.svg?style=flat
[releases]: https://github.com/bakwc/mysql_ch_replicator/releases

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license]: LICENSE

![img](https://raw.githubusercontent.com/bakwc/mysql_ch_replicator/master/mysql_ch.jpg)

`mysql_ch_replicator` is a powerful and efficient tool designed for real-time replication of MySQL databases to ClickHouse.

With a focus on high performance, it utilizes batching heavily and uses C++ extension for faster execution. This tool ensures seamless data integration with support for migrations, schema changes, and correct data management.

## Features

- **Real-Time Replication**: Keeps your ClickHouse database in sync with MySQL in real-time.
- **High Performance**: Utilizes batching and ports slow parts to C++ (e.g., MySQL internal JSON parsing) for optimal performance.
- **Supports Migrations/Schema Changes**: Handles adding, altering, and removing tables without breaking the replication process.
- **Recovery without Downtime**: Allows for preserving old data while performing initial replication, ensuring continuous operation.
- **Correct Data Removal**: Unlike MaterializedMySQL, `mysql_ch_replicator` ensures physical removal of data.
- **Comprehensive Data Type Support**: Accurately replicates most data types, including JSON, booleans, and more. Easily extensible for additional data types.
- **Multi-Database Handling**: Replicates the binary log once for all databases, optimizing the process compared to `MaterializedMySQL`, which replicates the log separately for each database.

## Installation

To install `mysql_ch_replicator`, use the following command:

```bash
pip install mysql_ch_replicator
```

You may need to also compile C++ components if they're not pre-built for your platform.

## Usage

### Basic Usage

For realtime data sync from MySQL to ClickHouse:

1. Prepare config file. Use `example_config.yaml` as an example.
2. Configure MySQL and ClickHouse servers:
 - MySQL server configuration file `my.cnf` should include following settings (required to write binary log in raw format, and enable password authentication):
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

 - ClickHouse server config `override.xml` should include following settings (it makes clickhouse apply final keyword automatically to handle updates correctly):
```xml
<clickhouse>
    <!-- ... other settings ... -->
    <profiles>
        <default>
            <!-- ... other settings ... -->
            <final>1</final>
        </default>
    </profiles>
</clickhouse>
```

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
mysql_ch_replicator --config config.yaml db_replicator --database mysql_db_name --initial_only=True
```
Where `mysql_db_name` is the name of the database you want to copy.

Don't be afraid to interrupt process in the middle. It will save the state and continue copy after restart.

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
  data_dir: '/home/user/binlog/'
  records_per_file: 100000

databases: 'database_name_pattern_*'
tables: '*'

exclude_databases: ['database_10', 'database_*_42']   # optional
exclude_tables: ['meta_table_*']                      # optional

log_level: 'info'   # optional             
```

#### Required settings

- `mysql` MySQL connection settings
- `clickhouse` ClickHouse connection settings
- `binlog_replicator.data_dir` Create a new empty directory, it will be used by script to store it's state
- `databases` Databases name pattern to replicate, e.g. `db_*` will match `db_1` `db_2` `db_test`, list is also supported

#### Optional settings
- `tables` - tables to filter, list is also supported
- `exclude_databases` - databases to __exclude__, string or list, eg `'table1*'` or `['table2', 'table3*']`. If same database matches `databases` and `exclude_databases`, exclude has higher priority.
- `exclude_tables` - databases to __exclude__, string or list. If same table matches `tables` and `exclude_tables`, exclude has higher priority.
- `log_level` - log level, default is `info`, you can set to `debug` to get maximum information (allowed values are `debug`, `info`, `warning`, `error`, `critical`)

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

## Contribution

Contributions are welcome! Please open an issue or submit a pull request for any bugs or features you would like to add.

## License

`mysql_ch_replicator` is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Acknowledgements

Thank you to all the contributors who have helped build and improve this tool.
