
module RSched


class Lock
  def initialize(hostname, timeout)
  end

  # acquired=token, locked=false, finished=nil
  def acquire(ident, time)
  end

  def release(token)
  end

  def finish(token)
  end

  def delete_before(ident, time)
  end
end


end

require 'rsched/dblock'

