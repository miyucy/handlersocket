require 'test_helper'

describe HandlerSocket do
  it 'loaded' do
    HandlerSocket::VERSION.wont_be_nil
  end

  let(:hs) { HandlerSocket.new :host => $conf['hs']['host'], :port => $conf['hs']['port'] }

  it 'work' do
    $mysql.query <<-DDL
      CREATE TABLE table1 (k VARCHAR(32) KEY, v VARCHAR(32))
    DDL

    key = SecureRandom.hex 16
    val = SecureRandom.hex 16
    $mysql.query "INSERT INTO table1 VALUES ('#{$mysql.escape key}', '#{$mysql.escape val}')"

    res = hs.open_index 1, $conf['db'], 'table1', 'PRIMARY', 'k,v'
    res.must_equal 0

    res = hs.execute_single 1, '=', [key], 1, 0
    res[0].must_equal 0
    res[1].must_equal [[key, val]]
  end
end
