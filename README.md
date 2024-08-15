# mysql_ch_replicator

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

To start the replication process:

1. Prepare config file. Use `example_config.yaml` as an example.
2. Start the replication:

```bash
mysql_ch_replicator --config config.yaml run_all
```

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

binlog_replicator:
  data_dir: '/home/user/binlog/'
  records_per_file: 100000

databases: 'database_name_pattern_*'
```


- `mysql` MySQL connection settings
- `clickhouse` ClickHouse connection settings
- `binlog_replicator.data_dir` Directory for store binary log and application state
- `databases` Databases name pattern to replicate, eg `db_*` will match `db_1` `db_2` `db_test`

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

For running test you will need:
1. MySQL and ClickHouse server
2. `config.yaml` that will be used during tests
3. Run tests with:

```bash
pytest -v -s test_mysql_ch_replicator.py
```

## Contribution

Contributions are welcome! Please open an issue or submit a pull request for any bugs or features you would like to add.

## License

`mysql_ch_replicator` is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Acknowledgements

Thank you to all the contributors who have helped build and improve this tool.
