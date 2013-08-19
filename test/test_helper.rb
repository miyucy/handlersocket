require 'minitest/autorun'
Bundler.require :default
require 'handlersocket'
require 'yaml'
require 'securerandom'

$conf  = YAML.load_file File.expand_path('../database.yml', __FILE__)

$mysql = Mysql2::Client.new($conf['my'])
$mysql.query "DROP DATABASE IF EXISTS #{$conf['db']}"
$mysql.query "CREATE DATABASE #{$conf['db']}"
$mysql.query "USE #{$conf['db']}"
