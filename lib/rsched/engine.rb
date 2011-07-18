require 'thread'
require 'monitor'
require 'time'
require 'rsched/lock'

module RSched


class Engine
  class Sched
    def initialize(cron, action, sched_start, from=Time.now.to_i, to=Time.now.to_i)
      require 'cron-spec'
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
    @lock = lock
    @resume = conf[:resume]
    @delay = conf[:delay]
    @interval = conf[:interval]
    @delete = conf[:delete]
    @extend_timeout = conf[:extend_timeout]
    @kill_timeout = conf[:kill_timeout]
    @kill_retry = conf[:kill_retry]
    @sched_start = conf[:from] || 0
    @finished = false
    @ss = {}

    @extender = TimerThread.new(@lock, @extend_timeout, @kill_timeout, @kill_retry)

    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  # {cron => (ident,action)}
  def set_sched(ident, action, cron)
    now = Time.now.to_i
    @ss[ident] = Sched.new(cron, action, @sched_start, now-@resume, now-@delay)
  end

  def init_proc(run_proc, kill_proc)
    @run_proc = run_proc
    @extender.init_proc(kill_proc)
  end

  def run
    @extender.start
    until @finished
      one = false

      now = Time.now.to_i - @delay
      @ss.each_pair {|ident,s|

        s.sched(now)
        s.queue.delete_if {|time|
          next if @finished

          token = @lock.acquire(ident, time)
          case token
          when nil
            # already finished
            true

          when false
            # not finished but already locked
            false

          else
            one = true
            process(token, ident, time, s.action)
          end
        }

        break if @finished
      }

      return if @finished

      unless one
        cond_wait(@interval)
      end

    end
  end

  def shutdown
    @finished = true
    @extender.shutdown
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

  def process(token, ident, time, action)
    puts "started token=#{token.inspect} time=#{time}"

    @extender.set_token(token)

    success = false
    begin
      @run_proc.call(ident, time, action)
      success = true
    rescue
      puts "failed token=#{token.inspect} time=#{time}: #{$!}"
      $!.backtrace.each {|bt|
        puts "  #{bt}"
      }
    end

    @extender.reset_token

    if success
      @lock.finish(token)
      cleanup_old_entries(ident)
      true
    else
      @lock.release(token)
      false
    end
  end

  def cleanup_old_entries(ident)
    @lock.delete_before(ident, Time.now.to_i-@delete)
  end

  class TimerThread
    include MonitorMixin

    def initialize(lock, extend_timeout, kill_timeout, kill_retry)
      super()
      @lock = lock
      @extend_timeout = extend_timeout
      @kill_timeout = kill_timeout
      @kill_retry = kill_retry
      @kill_time = nil
      @kill_proc = nil
      @extend_time = nil
      @token = nil
      @finished = false
    end

    def init_proc(kill_proc)
      @kill_proc = kill_proc
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def join
      @thread.join
    end

    def set_token(token)
      synchronize do
        now = Time.now.to_i
        @extend_time = now + @extend_timeout
        @kill_time = now + @kill_timeout
        @token = token
      end
    end

    def reset_token
      synchronize do
        @token = nil
      end
    end

    def shutdown
      @finished = true
    end

    private
    def run
      until @finished
        sleep 1
        synchronize do
          if @token
            now = Time.now.to_i
            try_kill(now, @token)
            try_extend(now, @token)
          end
        end
      end
    end

    def try_extend(now, token)
      if now > @extend_time
        puts "extending token=#{token.inspect}"
        @lock.extend_timeout(token, now)
        @extend_time = now + @extend_timeout
      end
    end

    def try_kill(now, token)
      if now > @kill_time
        if @kill_proc
          puts "killing #{token.inspect}..."
          @kill_proc.call rescue nil
        end
        @kill_time = now + @kill_retry
      end
    end
  end
end


class ExecRunner
  def initialize(cmd)
    @cmd = cmd + ' ' + ARGV.map {|a| Shellwords.escape(a) }.join(' ')
    @iobuf = ''
    @pid = nil
    @next_kill = :TERM
  end

  def call(ident, time, action)
    message = [ident, time, action].join("\t")
    IO.popen(@cmd, "r+") {|io|
      @pid = io.pid
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

  def terminate
    Process.kill(@next_kill, @pid)
    @next_kill = :KILL
  end
end


end  # module RSched
