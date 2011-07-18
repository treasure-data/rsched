
module RSched


class Lock
  def initialize(hostname, timeout)
    @hostname = hostname
    @timeout = timeout
  end

  attr_reader :hostname, :timeout

  # acquired=token, locked=false, finished=nil
  def acquire(ident, time, now=Time.now.to_i)
  end

  def release(token, next_timeout=Time.now.to_i)
  end

  def finish(token, now=Time.now.to_i)
  end

  def extend_timeout(token, timeout=Time.now.to_i+@timeout)
  end

  def delete_before(ident, time)
  end
end


end

require 'rsched/dblock'

