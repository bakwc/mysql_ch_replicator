import os
from pathlib import Path

from mysql_ch_replicator.config import Settings


def test_env_vars_override_config():
    config_file = Path(__file__).parent / 'tests_config_env_vars.yaml'
    
    os.environ['MYSQL_HOST'] = 'mysql.env.host'
    os.environ['MYSQL_PORT'] = '8306'
    os.environ['MYSQL_USER'] = 'env_mysql_user'
    os.environ['MYSQL_PASSWORD'] = 'env_mysql_pass'
    os.environ['MYSQL_CHARSET'] = 'utf8'
    
    os.environ['CLICKHOUSE_HOST'] = 'clickhouse.env.host'
    os.environ['CLICKHOUSE_PORT'] = '8323'
    os.environ['CLICKHOUSE_USER'] = 'env_ch_user'
    os.environ['CLICKHOUSE_PASSWORD'] = 'env_ch_pass'
    
    settings = Settings()
    settings.load(str(config_file))
    
    assert settings.mysql.host == 'mysql.env.host'
    assert settings.mysql.port == 8306
    assert settings.mysql.user == 'env_mysql_user'
    assert settings.mysql.password == 'env_mysql_pass'
    assert settings.mysql.charset == 'utf8'
    
    assert settings.clickhouse.host == 'clickhouse.env.host'
    assert settings.clickhouse.port == 8323
    assert settings.clickhouse.user == 'env_ch_user'
    assert settings.clickhouse.password == 'env_ch_pass'
    
    del os.environ['MYSQL_HOST']
    del os.environ['MYSQL_PORT']
    del os.environ['MYSQL_USER']
    del os.environ['MYSQL_PASSWORD']
    del os.environ['MYSQL_CHARSET']
    del os.environ['CLICKHOUSE_HOST']
    del os.environ['CLICKHOUSE_PORT']
    del os.environ['CLICKHOUSE_USER']
    del os.environ['CLICKHOUSE_PASSWORD']


def test_config_without_env_vars():
    config_file = Path(__file__).parent / 'tests_config_env_vars.yaml'
    
    settings = Settings()
    settings.load(str(config_file))
    
    assert settings.mysql.host == 'mysql.local'
    assert settings.mysql.port == 3306
    assert settings.mysql.user == 'mysql_user'
    assert settings.mysql.password == 'mysql_pass'
    assert settings.mysql.charset == 'utf8mb4'
    
    assert settings.clickhouse.host == 'clickhouse.local'
    assert settings.clickhouse.port == 9000
    assert settings.clickhouse.user == 'ch_user'
    assert settings.clickhouse.password == 'ch_pass'


def test_partial_env_vars_override():
    config_file = Path(__file__).parent / 'tests_config_env_vars.yaml'
    
    os.environ['MYSQL_PASSWORD'] = 'env_mysql_pass'
    os.environ['CLICKHOUSE_HOST'] = 'clickhouse.env.host'
    
    settings = Settings()
    settings.load(str(config_file))
    
    assert settings.mysql.host == 'mysql.local'
    assert settings.mysql.port == 3306
    assert settings.mysql.user == 'mysql_user'
    assert settings.mysql.password == 'env_mysql_pass'
    assert settings.mysql.charset == 'utf8mb4'
    
    assert settings.clickhouse.host == 'clickhouse.env.host'
    assert settings.clickhouse.port == 9000
    assert settings.clickhouse.user == 'ch_user'
    assert settings.clickhouse.password == 'ch_pass'
    
    del os.environ['MYSQL_PASSWORD']
    del os.environ['CLICKHOUSE_HOST']


def test_config_without_mysql_clickhouse_sections():
    config_file = Path(__file__).parent / 'tests_config_env_vars_no_creds.yaml'
    
    os.environ['MYSQL_HOST'] = 'mysql.env.host'
    os.environ['MYSQL_PORT'] = '8306'
    os.environ['MYSQL_USER'] = 'env_mysql_user'
    os.environ['MYSQL_PASSWORD'] = 'env_mysql_pass'
    
    os.environ['CLICKHOUSE_HOST'] = 'clickhouse.env.host'
    os.environ['CLICKHOUSE_PORT'] = '8323'
    os.environ['CLICKHOUSE_USER'] = 'env_ch_user'
    os.environ['CLICKHOUSE_PASSWORD'] = 'env_ch_pass'
    
    settings = Settings()
    settings.load(str(config_file))
    
    assert settings.mysql.host == 'mysql.env.host'
    assert settings.mysql.port == 8306
    assert settings.mysql.user == 'env_mysql_user'
    assert settings.mysql.password == 'env_mysql_pass'
    
    assert settings.clickhouse.host == 'clickhouse.env.host'
    assert settings.clickhouse.port == 8323
    assert settings.clickhouse.user == 'env_ch_user'
    assert settings.clickhouse.password == 'env_ch_pass'
    
    del os.environ['MYSQL_HOST']
    del os.environ['MYSQL_PORT']
    del os.environ['MYSQL_USER']
    del os.environ['MYSQL_PASSWORD']
    del os.environ['CLICKHOUSE_HOST']
    del os.environ['CLICKHOUSE_PORT']
    del os.environ['CLICKHOUSE_USER']
    del os.environ['CLICKHOUSE_PASSWORD']

