
module RSched


class DBLock < Lock
  def initialize(hostname, timeout, uri, user, pass)
    super(hostname, timeout)
    require 'dbi'
    @uri = uri
    @user = user
    @pass = pass
    init_db(uri.split(':',3)[1])
  end

  private
  def connect!
    if @conn
      @conn.close
      @conn = nil
    end
    @conn = DBI.connect(@uri, @user, @pass)
  end

  def init_db(type)
    sql = ''
    case type
    when /mysql/i
      sql << 'CREATE TABLE IF NOT EXISTS rsched ('
      sql << '  ident VARCHAR(256) CHARACTER SET ASCII NOT NULL,'
      sql << '  time INT NOT NULL,'
      sql << '  host VARCHAR(256) CHARACTER SET ASCII,'
      sql << '  timeout INT,'
      sql << '  finish INT,'
      sql << '  PRIMARY KEY (ident, time)'
      sql << ') ENGINE=INNODB;'
    else
      sql << 'CREATE TABLE IF NOT EXISTS rsched ('
      sql << '  ident VARCHAR(256) NOT NULL,'
      sql << '  time INT NOT NULL,'
      sql << '  host VARCHAR(256),'
      sql << '  timeout INT,'
      sql << '  finish INT,'
      sql << '  PRIMARY KEY (ident, time)'
      sql << ');'
    end
    connect!
    @conn.execute(sql)
  end

  public
  def acquire(ident, time, now=Time.now.to_i)
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
    n = @conn.do('UPDATE rsched SET timeout=? WHERE ident = ? AND time = ? AND host = ?;',
           0, ident, time, @hostname)
    return n > 0
  end

  def finish(token, now=Time.now.to_i)
    ident, time = *token
    n = @conn.do('UPDATE rsched SET finish=? WHERE ident = ? AND time = ? AND host = ?;',
           now, ident, time, @hostname)
    return n > 0
  end

  def extend_timeout(token, timeout=Time.now.to_i+@timeout)
    ident, time = *token
    n = @conn.do('UPDATE rsched SET timeout=? WHERE ident = ? AND time = ? AND host = ?;',
           timeout, ident, time, @hostname)
    return n > 0
  end

  def delete_before(ident, time)
    @conn.do('DELETE FROM rsched WHERE ident = ? AND time < ? AND finish IS NOT NULL;', ident, time)
  end

  private
  def try_insert(ident, time, now)
    n = @conn.do('INSERT INTO rsched (ident, time, host, timeout) VALUES (?, ?, ?, ?);',
           ident, time, @hostname, now+@timeout)
    return n > 0
  rescue # TODO unique error
    return false
  end

  def try_update(ident, time, now)
    n = @conn.do('UPDATE rsched SET host=?, timeout=? WHERE ident = ? AND time = ? AND finish IS NULL AND timeout < ?;',
            @hostname, now+@timeout, ident, time, now)
    return n > 0
  end

  def check_finished(ident, time)
    x = @conn.select_one('SELECT finish FROM rsched WHERE ident = ? AND time = ? AND finish IS NOT NULL;',
                ident, time)
    return x != nil
  end
end


end

