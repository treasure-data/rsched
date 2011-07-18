
module RSched


class DBLock < Lock
  def initialize(hostname, timeout, uri, user, pass)
    super(hostname, timeout)
    require 'dbi'
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

  def acquire(ident, time, now=Time.now.to_i)
    if try_insert(ident, time, now) || try_update(ident, time, now)
      return [ident, time]
    elsif check_finished(ident, time)
      return nil
    else
      return false
    end
  end

  def release(token, next_timeout=Time.now.to_i)
    ident, time = *token
    n = @db.do('UPDATE rsched SET timeout=? WHERE ident = ? AND time = ? AND host = ?;',
           next_timeout, ident, time, @hostname)
    return n > 0
  end

  def finish(token, now=Time.now.to_i)
    ident, time = *token
    n = @db.do('UPDATE rsched SET finish=? WHERE ident = ? AND time = ? AND host = ?;',
           now, ident, time, @hostname)
    return n > 0
  end

  def extend_timeout(token, timeout=Time.now.to_i+@timeout)
    ident, time = *token
    n = @db.do('UPDATE rsched SET timeout=? WHERE ident = ? AND time = ? AND host = ?;',
           timeout, ident, time, @hostname)
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


end

