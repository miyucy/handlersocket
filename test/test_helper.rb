require 'minitest/autorun'
Bundler.require :default
require 'handlersocket'
require 'yaml'
require 'securerandom'

$conf  = YAML.load_file File.expand_path('../database.yml', __FILE__)
$mysql = Mysql2::Client.new($conf['my'])
$mysql.query "DROP DATABASE IF EXISTS #{$conf['db']}"
$mysql.query "CREATE DATABASE IF NOT EXISTS #{$conf['db']}"
$mysql.query "USE #{$conf['db']}"
$mysql.query 'CREATE TABLE table1 (k VARCHAR(32) PRIMARY KEY, v VARCHAR(32))'
$mysql.query 'CREATE TABLE table2 (k INTEGER PRIMARY KEY, v1 VARCHAR(32) NOT NULL, v2 VARCHAR(32) NOT NULL, v3 VARCHAR(32) NOT NULL)'
