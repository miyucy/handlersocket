require 'test_helper'

describe HandlerSocket do
  Record = Struct.new(:k, :v1, :v2, :v3, :used, :deleted)

  class Repository
    attr_reader :records

    def initialize
      @records = (1..1000).map { |n|
        Record.new(n.to_s, "v1_#{n}", "v2_#{n}", "v3_#{n}", false, false)
      }
      @mutex = Mutex.new
    end

    def use
      rec = pop
      yield rec
    ensure
      push rec
    end

    def pop
      @mutex.synchronize {
        record = @records.reject(&:used).sample
        record.used = true if record
        record
      }
    end

    def push(record)
      @mutex.synchronize {
        record.used = false
      }
    end
  end

  class Runner
    REPEAT = 1000
    @rw = false

    attr_reader :idx
    def initialize(repo)
      opt = {
        host: $conf['hs']['host'],
        port: $conf['hs'][self.class.instance_variable_get(:@rw) ? 'port_wr' : 'port'],
      }
      @hs = HandlerSocket.new opt
      @idx = @hs.open_index $conf['db'], 'table2', 'PRIMARY', 'k,v1,v2,v3'
      @repo = repo
      @runner = runner
    end

    def perform
      @runner.join
    end

    def footprint(f)
      # print f
    end
  end

  class DeleteRunner < Runner
    @rw = true

    def runner
      Thread.new do
        REPEAT.times do
          @repo.use do |rec|
            res = @hs.execute_delete idx, '=', [rec.k], 1, 0
            if rec.deleted
              raise unless res == [0, [['0']]]
            else
              raise unless res == [0, [['1']]]
            end
            rec.deleted = true
            footprint 'D'
          end
          Thread.pass
        end
      end
    end
  end

  class InsertRunner < Runner
    @rw = true

    def runner
      Thread.new do
        REPEAT.times do
          @repo.use do |rec|
            v1 = "iv1_#{rec.k}_#{SecureRandom.hex 4}"
            v2 = "iv2_#{rec.k}_#{SecureRandom.hex 4}"
            v3 = "iv3_#{rec.k}_#{SecureRandom.hex 4}"
            begin
              res = @hs.execute_insert idx, [rec.k, v1, v2, v3]
              raise unless res == [0, []]
              rec.deleted = false
              rec.v1, rec.v2, rec.v3 = v1, v2, v3
            rescue HandlerSocket::Error
              raise if rec.deleted
            end
            footprint 'I'
          end
          Thread.pass
        end
      end
    end
  end

  class UpdateRunner < Runner
    @rw = true

    def runner
      Thread.new do
        REPEAT.times do
          @repo.use do |rec|
            v1 = "wv1_#{rec.k}_#{SecureRandom.hex 4}"
            v2 = "wv2_#{rec.k}_#{SecureRandom.hex 4}"
            v3 = "wv3_#{rec.k}_#{SecureRandom.hex 4}"
            res = @hs.execute_update idx, '=', [rec.k], 1, 0, [rec.k, v1, v2, v3]
            if rec.deleted
              raise unless res == [0, [['0']]]
            else
              raise unless res == [0, [['1']]]
              rec.v1, rec.v2, rec.v3 = v1, v2, v3
            end
            footprint 'U'
          end
          Thread.pass
        end
      end
    end
  end

  class ReadRunner < Runner
    def runner
      Thread.new do
        REPEAT.times do
          @repo.use do |rec|
            res = @hs.execute_single idx, '=', [rec.k], 1, 0
            if rec.deleted
              raise unless res == [0, []]
            else
              raise unless res == [0, [[rec.k, rec.v1, rec.v2, rec.v3]]]
            end
            footprint 'R'
          end
          Thread.pass
        end
      end
    end
  end

  before do
    @repo = Repository.new
    $mysql.query "BEGIN"
    @repo.records.each do |record|
      $mysql.query "INSERT INTO table2 (k, v1, v2, v3) VALUES (#{record.k}, '#{$mysql.escape record.v1}', '#{$mysql.escape record.v2}', '#{$mysql.escape record.v3}')"
    end
    $mysql.query "COMMIT"
  end

  it 'works' do
    klasses = [
               DeleteRunner,
               InsertRunner,
               ReadRunner,
               UpdateRunner,
              ]
    runners = klasses.shuffle.map { |e| e.new @repo }
    runners.each(&:perform)

    records = {}
    $mysql.query("SELECT * FROM table2").each do |record|
      records[record['k'].to_s] = record.values_at(*%w(v1 v2 v3))
    end
    @repo.records.each do |record|
      if record.deleted
        records[record.k].must_equal nil
      else
        records[record.k].must_equal [record.v1, record.v2, record.v3]
      end
    end
  end
end
