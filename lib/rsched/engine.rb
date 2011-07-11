require 'thread'
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
          next if @finished

          x = @lock.acquire(ident, time)
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
