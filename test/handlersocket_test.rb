require 'test_helper'

describe HandlerSocket do
  it 'loaded' do
    HandlerSocket::VERSION.wont_be_nil
  end

  let(:mysql) { $mysql }
  let(:hs) { HandlerSocket.new :host => $conf['hs']['host'], :port => $conf['hs']['port'] }
  before { mysql.query %{DELETE FROM table1} }
  after  { hs.close }

  describe '#open_index' do
    it 'return opened index number' do
      res = hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      res.must_equal 0
    end

    it 'increment index number' do
      hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      res = hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      res.must_equal 2
    end

    it 'can not obtain new index number when closed' do
      hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      hs.close
      -> {
        hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      }.must_raise HandlerSocket::IOError
    end

    it 'can not execute after connection closed' do
      idx = hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v'
      hs.close
      -> {
        hs.execute_single idx, '=', ['foo'], 1, 0
      }.must_raise HandlerSocket::IOError
    end
  end

  describe '#execute_single' do
    let(:key) { SecureRandom.hex 16 }
    let(:val) { SecureRandom.hex 16 }
    let(:idx) { hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v' }

    before do
      mysql.query %{INSERT INTO table1 VALUES ('#{mysql.escape key}', '#{mysql.escape val}')}
    end

    it 'return value' do
      res = hs.execute_single idx, '=', [key], 1, 0
      res.must_equal [0, [[key, val]]]
    end

    it 'return value' do
      res = hs.execute_single idx, '=', [key]
      res.must_equal [0, [[key, val]]]
    end

    it 'return value' do
      res = hs.execute_single idx, '=', [key.reverse]
      res.must_equal [0, []]
    end
  end

  describe '#execute_multi' do
    let(:keys) { 10.times.map { SecureRandom.hex 16 } }
    let(:vals) { 10.times.map { SecureRandom.hex 16 } }
    let(:idx) { hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v' }

    before do
      keys.zip(vals).each do |key, val|
        mysql.query %{INSERT INTO table1 VALUES ('#{mysql.escape key}', '#{mysql.escape val}')}
      end
    end

    it 'return value' do
      res = hs.execute_multi [[idx, '=', [keys[0]], 1, 0],
                              [idx, '=', [keys[1]], 1, 0]]
      res.must_equal [[0, [[keys[0], vals[0]]]],
                      [0, [[keys[1], vals[1]]]]]
    end

    it 'return value' do
      res = hs.execute_multi [[idx, '=', [keys[2]]],
                              [idx, '=', [keys[3]]]]
      res.must_equal [[0, [[keys[2], vals[2]]]],
                      [0, [[keys[3], vals[3]]]]]
    end

    it 'return value' do
      res = hs.execute_multi idx, '=', [keys[4]], 1, 0
      res.must_equal [[0, [[keys[4], vals[4]]]]]
    end

    it 'return value' do
      res = hs.execute_multi idx, '=', [keys[5]]
      res.must_equal [[0, [[keys[5], vals[5]]]]]
    end
  end

  describe '#execute_insert' do
    let(:hs) { HandlerSocket.new :host => $conf['hs']['host'], :port => $conf['hs']['port_wr'] }
    let(:idx) { hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v' }
    let(:nulval) { nil }

    it 'return value' do
      key = SecureRandom.hex
      val = SecureRandom.hex
      res = hs.execute_insert idx, [key, val]
      res.must_equal [0, []]
    end

    it 'return value' do
      key = SecureRandom.hex
      val = SecureRandom.hex
      hs.execute_insert idx, [key, val]
      row = mysql.query(%{SELECT COUNT(*) FROM table1 WHERE k = '#{mysql.escape key}' AND v = '#{mysql.escape val}'}).first
      row['COUNT(*)'].must_equal 1
    end

    it 'return value' do
      key = SecureRandom.hex
      val = SecureRandom.hex
      hs.execute_insert idx, [key, val]
      -> {
        hs.execute_insert idx, [key, SecureRandom.hex]
      }.must_raise HandlerSocket::Error
    end

    it 'return value' do
      key = SecureRandom.hex
      hs.execute_insert idx, [key, nulval]
      row = mysql.query(%{SELECT COUNT(*) FROM table1 WHERE v IS NULL}).first
      row['COUNT(*)'].must_equal 1
    end
  end

  describe '#execute_delete' do
    let(:hs) { HandlerSocket.new :host => $conf['hs']['host'], :port => $conf['hs']['port_wr'] }
    let(:key) { SecureRandom.hex 16 }
    let(:val) { SecureRandom.hex 16 }
    let(:idx) { hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v' }

    before do
      mysql.query %{INSERT INTO table1 VALUES ('#{mysql.escape key}', '#{mysql.escape val}')}
    end

    it 'return value' do
      res = hs.execute_delete idx, '=', [key], 1, 0
      res.must_equal [0, [['1']]]
    end

    it 'return value' do
      hs.execute_delete idx, '=', [key], 1, 0
      row = mysql.query(%{SELECT COUNT(*) FROM table1}).first
      row['COUNT(*)'].must_equal 0
    end

    it 'return value' do
      res = hs.execute_delete idx, '=', [key], 1, 1
      res.must_equal [0, [['0']]]
    end

    it 'return value' do
      hs.execute_delete idx, '=', [key], 1, 1
      row = mysql.query(%{SELECT COUNT(*) FROM table1}).first
      row['COUNT(*)'].must_equal 1
    end

    it 'return value' do
      res = hs.execute_delete idx, '=', [SecureRandom.hex], 1, 0
      res.must_equal [0, [['0']]]
    end
  end

  describe '#execute_update' do
    let(:hs) { HandlerSocket.new :host => $conf['hs']['host'], :port => $conf['hs']['port_wr'] }
    let(:key) { SecureRandom.hex 16 }
    let(:val) { SecureRandom.hex 16 }
    let(:newval) { SecureRandom.hex 16 }
    let(:newkey) { SecureRandom.hex 16 }
    let(:nulval) { nil }
    let(:idx) { hs.open_index $conf['db'], 'table1', 'PRIMARY', 'k,v' }

    before do
      mysql.query %{INSERT INTO table1 VALUES ('#{mysql.escape key}', '#{mysql.escape val}')}
    end

    it 'return value' do
      res = hs.execute_update idx, '=', [key], 1, 0, [key, newval]
      res.must_equal [0, [['1']]]
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [key, newval]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape key}'}).first
      row['v'].must_equal newval
    end

    it 'return value' do
      res = hs.execute_update idx, '=', [key], 1, 1, [key, newval]
      res.must_equal [0, [['0']]]
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 1, [key, newval]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape key}'}).first
      row['v'].must_equal val
    end

    it 'return value' do
      res = hs.execute_update idx, '=', [key], 1, 0, [key, val]
      res.must_equal [0, [['1']]]
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [key, val]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape key}'}).first
      row['v'].must_equal val
    end

    it 'return value' do
      res = hs.execute_update idx, '=', [key], 1, 0, [newkey, val]
      res.must_equal [0, [['1']]]
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [newkey, val]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape key}'}).first
      row.must_equal nil
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [newkey, val]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape newkey}'}).first
      row['v'].must_equal val
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [newkey, newval]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape newkey}'}).first
      row['v'].must_equal newval
    end

    it 'return value' do
      res = hs.execute_update idx, '=', [newkey], 1, 0, [newkey, newval]
      res.must_equal [0, [['0']]]
    end

    it 'return value' do
      hs.execute_update idx, '=', [key], 1, 0, [newkey, nulval]
      row = mysql.query(%{SELECT * FROM table1 WHERE k = '#{mysql.escape newkey}'}).first
      row['v'].must_equal nulval
      row = mysql.query(%{SELECT * FROM table1 WHERE v IS NULL}).first
      row['k'].must_equal newkey
    end
  end
end
