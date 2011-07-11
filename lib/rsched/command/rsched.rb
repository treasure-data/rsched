require 'thread'

module RSched


class Lock
  def initialize(hostname, timeout)
  end

  # acquired=token, locked=false, finished=nil
  def aquire(ident, time)
  end

  def release(token)
  end

  def finish(token)
  end

  def delete_before(ident, time)
  end
end


class DBLock < Lock
  def initialize(hostname, timeout, uri, user, pass)
    require 'dbi'
    @hostname = hostname
    @timeout = timeout
    @db = DBI.connect(uri, user, pass)
    init_db
  end

  def init_db
    sql = ''
    sql << 'CREATE TABLE IF NOT EXISTS rsched ('
    sql << '  ident VARCHAR(256) NOT NULL,'
    sql << '  time INT NOT NULL,'
    sql << '  host VARCHAR(256),'
    sql << '  timeout INT,'
    sql << '  finish INT,'
    sql << '  PRIMARY KEY (ident, time));'
    @db.execute(sql)
  end

  def aquire(ident, time)
    now = Time.now.to_i
    if try_insert(ident, time, now) || try_update(ident, time, now)
      return [ident, time]
    elsif check_finished(ident, time)
      return nil
    else
      return false
    end
  end

  def release(token)
    ident, time = *token
    n = @db.do('UPDATE rsched SET timeout=? WHERE ident = ? AND time = ? AND host = ?;',
           0, ident, time, @hostname)
    return n > 0
  end

  def finish(token)
    ident, time = *token
    now = Time.now.to_i
    n = @db.do('UPDATE rsched SET finish=? WHERE ident = ? AND time = ? AND host = ?;',
           now, ident, time, @hostname)
    return n > 0
  end

  def delete_before(ident, time)
    @db.do('DELETE FROM rsched WHERE ident = ? AND time < ? AND finish IS NOT NULL;', ident, time)
  end

  private
  def try_insert(ident, time, now)
    n = @db.do('INSERT INTO rsched (ident, time, host, timeout) VALUES (?, ?, ?, ?);',
           ident, time, @hostname, now+@timeout)
    return n > 0
  rescue # TODO unique error
    return false
  end

  def try_update(ident, time, now)
    n = @db.do('UPDATE rsched SET host=?, timeout=? WHERE ident = ? AND time = ? AND finish IS NULL AND (timeout < ? OR host = ?);',
            @hostname, now+@timeout, ident, time, now, @hostname)
    return n > 0
  end

  def check_finished(ident, time)
    x = @db.select_one('SELECT finish FROM rsched WHERE ident = ? AND time = ? AND finish IS NOT NULL;',
                ident, time)
    return x != nil
  end
end


class Engine
  class Sched
    def initialize(cron, action, sched_start, from=Time.now.to_i, to=Time.now.to_i)
      @tab = CronSpec::CronSpecification.new(cron)
      @action = action
      @sched_start = sched_start
      @queue = []
      @last_time = from
      sched(to)
    end

    attr_reader :queue, :action

    def sched(now)
      while @last_time <= now
        t = Time.at(@last_time).utc
        if @tab.is_specification_in_effect?(t)
          time = create_time_key(t)
          @queue << time if time >= @sched_start
        end
        @last_time += 60
      end
      @queue.uniq!
    end

    private
    require 'time'
    if Time.respond_to?(:strptime)
      def create_time_key(t)
        Time.strptime(t.strftime('%Y%m%d%H%M00UTC'), '%Y%m%d%H%M%S%Z').to_i
      end
    else
      require 'date'
      def create_time_key(t)
        Time.parse(DateTime.strptime(t.strftime('%Y%m%d%H%M00UTC'), '%Y%m%d%H%M%S').to_s).to_i
      end
    end
  end

  def initialize(lock, conf)
    require 'cron-spec'
    @lock = lock
    @resume = conf[:resume]
    @delay = conf[:delay]
    @interval = conf[:interval]
    @delete = conf[:delete]
    @sched_start = conf[:from] || 0
    @finished = false
    @ss = {}

    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  # {cron => (ident,action)}
  def set_sched(ident, action, cron)
    now = Time.now.to_i
    @ss[ident] = Sched.new(cron, action, @sched_start, now-@resume, now-@delay)
  end

  def run(run_proc)
    until @finished
      one = false

      now = Time.now.to_i - @delay
      @ss.each_pair {|ident,s|

        s.sched(now)
        s.queue.delete_if {|time|
          x = @lock.aquire(ident, time)
          case x
          when nil
            # already finished
            true

          when false
            # not finished but already locked
            false

          else
            one = true
            if process(ident, time, s.action, run_proc)
              # success
              @lock.finish(x)
              try_delete(ident)
              true
            else
              # fail
              @lock.release(x)
              false
            end
          end
        }

      }

      unless one
        cond_wait(@interval)
      end

    end
  end

  def shutdown
    @finished = true
    @mutex.synchronize {
      @cond.broadcast
    }
  end

  private
  if ConditionVariable.new.method(:wait).arity == 1
    require 'timeout'
    def cond_wait(sec)
      @mutex.synchronize {
        Timeout.timeout(sec) {
          @cond.wait(@mutex)
        }
      }
    rescue Timeout::Error
    end
  else
    def cond_wait(sec)
      @mutex.synchronize {
        @cond.wait(@mutex, sec)
      }
    end
  end

  def process(ident, time, action, run_proc)
    begin
      run_proc.call(ident, time, action)
      return true
    rescue
      puts "failed ident=#{ident} time=#{time}: #{$!}"
      $!.backtrace.each {|bt|
        puts "  #{bt}"
      }
      return false
    end
  end

  def try_delete(ident)
    @lock.delete_before(ident, Time.now.to_i-@delete)
  end
end


class ExecRunner
  def initialize(cmd)
    @cmd = cmd + ' ' + ARGV.map {|a| Shellwords.escape(a) }.join(' ')
    @iobuf = ''
  end

  def call(ident, time, action)
    message = [ident, time, action].join("\t")
    IO.popen(@cmd, "r+") {|io|
      io.write(message) rescue nil
      io.close_write
      begin
        while true
          io.sysread(1024, @iobuf)
          print @iobuf
        end
      rescue EOFError
      end
    }
    if $?.to_i != 0
      raise "Command failed"
    end
  end
end


end  # module RSched


require 'optparse'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"

confout = nil
schedule = []

defaults = {
  :timeout => 600,
  :resume => 3600,
  :delete => 2592000,
  :delay => 0,
  :interval => 10,
  :type => 'mysql',
  :name => "#{Process.pid}.#{`hostname`.strip}",
}

conf = { }

op.on('--configure PATH.yaml', 'Write configuration file') {|s|
  confout = s
}

op.on('--exec COMMAND', 'Execute command') {|s|
  conf[:exec] = s
}

op.on('--run SCRIPT.rb', 'Run method named \'run\' defined in the script') {|s|
  conf[:run] = s
}

op.on('-a', '--add EXPR', 'Add an execution schedule') {|s|
  schedule << s
}

op.on('-t', '--timeout SEC', 'Retry timeout (default: 600)', Integer) {|i|
  conf[:timeout] = i
}

op.on('-r', '--resume SEC', 'Limit time to resume tasks (default: 3600)', Integer) {|i|
  conf[:resume] = i
}

op.on('-E', '--delete SEC', 'Limit time to delete tasks (default: 2592000)', Integer) {|i|
  conf[:delete] = i
}

op.on('-n', '--name NAME', 'Unique name of this node (default: PID.HOSTNAME)') {|s|
  conf[:name] = s
}

op.on('-w', '--delay SEC', 'Delay time before running a task (default: 0)', Integer) {|i|
  conf[:delay] = i
}

op.on('-F', '--from UNIX_TIME_OR_now', 'Time to start scheduling') {|s|
  if s == "now"
    conf[:from] = Time.now.to_i
  else
    conf[:from] = s.to_i
  end
}

#op.on('-x', '--kill-timeout SEC', 'Threashold time before killing process (default: timeout * 5)', Integer) {|i|
#  conf[:kill_timeout] = i
#}

op.on('-i', '--interval SEC', 'Scheduling interval (default: 10)', Integer) {|i|
  conf[:interval] = i
}

op.on('-T', '--type TYPE', 'Lock database type (default: mysql)') {|s|
  conf[:type] = s
}

op.on('-D', '--database DB', 'Database name') {|s|
  conf[:db_database] = s
}

op.on('-H', '--host HOST[:PORT]', 'Database host') {|s|
  conf[:db_host] = s
}

op.on('-u', '--user NAME', 'Database user name') {|s|
  conf[:db_user] = s
}

op.on('-p', '--password PASSWORD', 'Database password') {|s|
  conf[:db_password] = s
}

op.on('-d', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}

op.on('-f', '--file PATH.yaml', 'Read configuration file') {|s|
  conf[:file] = s
}


(class<<self;self;end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end


begin
  if eqeq = ARGV.index('--')
    argv = ARGV.slice!(0, eqeq)
    ARGV.slice!(0)
  else
    argv = ARGV.slice!(0..-1)
  end
  op.parse!(argv)

  if argv.length != 0
    usage nil
  end

  if conf[:file]
    require 'yaml'
    yaml = YAML.load File.read(conf[:file])
    y = {}
    yaml.each_pair {|k,v| y[k.to_sym] = v }

    conf = defaults.merge(y).merge(conf)

    if conf[:schedule]
      schedule = conf[:schedule] + schedule
    end

    if ARGV.empty? && conf[:args]
      ARGV.clear
      ARGV.concat conf[:args]
    end

  else
    conf = defaults.merge(conf)
  end

  if conf[:run]
    type = :run
  elsif conf[:exec]
    type = :exec
  else
    raise "--exec, --run or --configure is required"
  end

  if conf[:resume] <= conf[:timeout]
    raise "resume time (-r) must be larger than timeout (-t)"
  end

  if conf[:delete] <= conf[:resume]
    raise "delete time (-E) must be larger than resume time (-r)"
  end

  case conf[:type]
  when 'mysql'
    if !conf[:db_database] || !conf[:db_host] || !conf[:db_user]
      raise "--database, --host and --user are required for mysql"
    end
    dbi = "DBI:Mysql:#{conf[:db_database]}:#{conf[:db_host]}"

  when 'sqlite3'
    if !conf[:db_database]
      raise "--database is required for sqlite3"
    end
    dbi = "DBI:SQLite3:#{conf[:db_database]}"

  else
    raise "Unknown lock server type '#{conf[:type]}'"
  end

rescue
  usage $!.to_s
end


if confout
  require 'yaml'

  conf.delete(:file)
  conf[:schedule] = schedule
  conf[:args] = ARGV

  y = {}
  conf.each_pair {|k,v| y[k.to_s] = v }

  File.open(confout, "w") {|f|
    f.write y.to_yaml
  }
  exit 0
end


if schedule.empty?
  usage "At least one -a is required"
end


if conf[:daemon]
  exit!(0) if fork
  Process.setsid
  exit!(0) if fork
  File.umask(0)
  STDIN.reopen("/dev/null")
  STDOUT.reopen("/dev/null", "w")
  STDERR.reopen("/dev/null", "w")
  File.open(conf[:daemon], "w") {|f|
    f.write Process.pid.to_s
  }
end


lock = RSched::DBLock.new(conf[:name], conf[:timeout], dbi, conf[:db_user].to_s, conf[:db_password].to_s)
worker = RSched::Engine.new(lock, conf)

schedule.each {|e|
  tabs = e.split(/\s+/, 7)
  ident = tabs.shift
  action = tabs.pop
  time = tabs.join(' ')
  puts "adding schedule ident='#{ident}' time='#{time}' action='#{action}'"
  worker.set_sched(ident, action, time)
}


trap :INT do
  puts "shutting down..."
  worker.shutdown
end

trap :TERM do
  puts "shutting down..."
  worker.shutdown
end


if type == :run
  load File.expand_path(conf[:run])
  run_proc = method(:run)
else
  run_proc = RSched::ExecRunner.new(conf[:exec])
end

worker.run(run_proc)

# dbi
# dbd-sqlite3

