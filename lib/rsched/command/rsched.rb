require 'optparse'
require 'rsched/engine'
require 'rsched/version'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"
op.version = RSched::VERSION

confout = nil
schedule = []

defaults = {
  :timeout => 300,
  :resume => 3600,
  :delete => 2592000,
  :delay => 0,
  :interval => 10,
  :type => 'mysql',
  :node_name => "#{Process.pid}.#{`hostname`.strip}",
  :kill_retry => 60,
  :release_on_fail => false,
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

op.on('-t', '--timeout SEC', 'Retry timeout (default: 30)', Integer) {|i|
  conf[:timeout] = i
}

op.on('-r', '--resume SEC', 'Limit time to resume tasks (default: 3600)', Integer) {|i|
  conf[:resume] = i
}

op.on('-E', '--delete SEC', 'Limit time to delete tasks (default: 2592000)', Integer) {|i|
  conf[:delete] = i
}

op.on('-n', '--name NAME', 'Unique name of this node (default: PID.HOSTNAME)') {|s|
  conf[:node_name] = s
}

op.on('-w', '--delay SEC', 'Delay time before running a task (default: 0)', Integer) {|i|
  conf[:delay] = i
}

op.on('-F', '--from YYYY-mm-dd_OR_now', 'Time to start scheduling') {|s|
  if s == "now"
    conf[:from] = Time.now.to_i
  else
    conf[:from] = Time.parse(s).to_i
  end
}

op.on('-e', '--extend-timeout SEC', 'Threashold time before extending visibility timeout (default: timeout * 3/4)', Integer) {|i|
  conf[:extend_timeout] = i
}

op.on('-x', '--kill-timeout SEC', 'Threashold time before killing process (default: timeout * 10)', Integer) {|i|
  conf[:kill_timeout] = i
}

op.on('-X', '--kill-retry SEC', 'Threashold time before retrying killing process (default: 60)', Integer) {|i|
  conf[:kill_retry] = i
}

op.on('-i', '--interval SEC', 'Scheduling interval (default: 10)', Integer) {|i|
  conf[:interval] = i
}

op.on('-U', '--release-on-fail', 'Releases lock if task failed so that other node can retry immediately', TrueClass) {|b|
  conf[:release_on_fail] = b
}

op.on('-T', '--type TYPE', 'Lock database type (default: mysql)') {|s|
  conf[:db_type] = s
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

op.on('--env K=V', 'Set environment variable') {|s|
  k, v = s.split('=',2)
  (conf[:env] ||= {})[k] = v
}

op.on('-d', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}

op.on('-f', '--file PATH.yaml', 'Read configuration file') {|s|
  (conf[:files] ||= []) << s
}

op.on('-o', '--log PATH', 'Path to log file (default: stdout)') {|s|
  conf[:log] = s
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

  if conf[:files]
    require 'yaml'
    docs = ''
    conf[:files].each {|file|
      docs << File.read(file)
    }
    y = {}
    YAML.load_documents(docs) {|yaml|
      yaml.each_pair {|k,v| y[k.to_sym] = v }
    }

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

  case conf[:db_type]
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
    raise "Unknown lock server type '#{conf[:db_type]}'"
  end

  unless conf[:extend_timeout]
    conf[:extend_timeout] = conf[:timeout] / 4 * 3
  end

  unless conf[:kill_timeout]
    conf[:kill_timeout] = conf[:timeout] * 10
  end

rescue
  usage $!.to_s
end


if confout
  require 'yaml'

  conf.delete(:files)
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


require 'logger'
if log_path = conf[:log]
  log_io = File.open(log_path, 'a')
  $log = Logger.new(log_io)
else
  $log = Logger.new(STDOUT)
end
$log.level = Logger::DEBUG

$log.info "Using node name #{conf[:node_name]}"


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


begin
  lock = RSched::DBLock.new(conf[:node_name], conf[:timeout], dbi, conf[:db_user].to_s, conf[:db_password].to_s)
rescue DBI::InterfaceError
  STDERR.puts "Can't initialize DBI interface: #{$!}"
  STDERR.puts "You may have to install database driver first:"
  STDERR.puts ""
  STDERR.puts "  $ gem install dbd-mysql"
  STDERR.puts "  $ gem install dbd-sqlite3"
  STDERR.puts ""
  exit 1
end
worker = RSched::Engine.new(lock, conf)

schedule.each {|e|
  tabs = e.split(/\s+/, 7)
  ident = tabs.shift
  action = tabs.pop
  time = tabs.join(' ')
  $log.info "Adding schedule ident='#{ident}' time='#{time}' action='#{action}'"
  worker.set_sched(ident, action, time)
}


trap :INT do
  $log.info "shutting down..."
  worker.shutdown
end

trap :TERM do
  $log.info "shutting down..."
  worker.shutdown
end

trap :HUP do
  if log_io
    log_io.reopen(log_path, 'a')
  end
end


if type == :run
  load File.expand_path(conf[:run])
  run_proc = method(:run)
  if defined? terminate
    kill_proc = method(:terminate)
  else
    kill_proc = Proc.new { }
  end
else
  run_proc = RSched::ExecRunner.new(conf[:exec])
  kill_proc = run_proc.method(:terminate)
end

worker.init_proc(run_proc, kill_proc)
worker.run

