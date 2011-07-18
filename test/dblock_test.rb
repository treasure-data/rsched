require File.dirname(__FILE__)+'/test_helper'

class DBLockTest < Test::Unit::TestCase
  TIMEOUT = 10
  DB_PATH = File.dirname(__FILE__)+'/test.db'
  DB_URI = "DBI:SQLite3:#{DB_PATH}"

  def clean
    FileUtils.rm_f DB_PATH
  end

  def test1_db
    RSched::DBLock.new('test1', TIMEOUT, DB_URI, '', '')
  end

  def test2_db
    RSched::DBLock.new('test2', TIMEOUT, DB_URI, '', '')
  end

  it 'acquire' do
    clean

    now = Time.now.to_i
    time = Time.now.to_i / 60 * 60

    db1 = test1_db
    db2 = test2_db

    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    # different host can't lock
    token = db2.acquire('ident1', time, now)
    assert_equal false, token

    # same host can relock
    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    # different identifier
    token = db2.acquire('ident2', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    # different time
    token = db2.acquire('ident1', time+60, now)
    assert_not_equal nil, token
    assert_not_equal false, token
  end

  it 'release' do
    clean

    now = Time.now.to_i
    time = Time.now.to_i / 60 * 60

    db1 = test1_db
    db2 = test2_db

    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    db1.release(token, now-1)

    # released
    token = db2.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token
  end

  it 'finish' do
    clean

    now = Time.now.to_i
    time = Time.now.to_i / 60 * 60

    db1 = test1_db
    db2 = test2_db

    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    db1.finish(token, now)

    # finished
    token_ = db1.acquire('ident1', time, now)
    assert_equal nil, token_

    # finished
    token_ = db2.acquire('ident1', time, now)
    assert_equal nil, token_
  end

  it 'timeout' do
    clean

    now = Time.now.to_i
    time = Time.now.to_i / 60 * 60

    db1 = test1_db
    db2 = test2_db

    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    # not timed out
    token_ = db2.acquire('ident1', time, now+TIMEOUT)
    assert_equal false, token_

    # timed out
    token = db2.acquire('ident1', time, now+TIMEOUT+1)
    assert_not_equal nil, token
    assert_not_equal false, token

    # taken
    token = db1.acquire('ident1', time, now+TIMEOUT+1)
    assert_equal false, token
  end

  it 'extend' do
    clean

    now = Time.now.to_i
    time = Time.now.to_i / 60 * 60

    db1 = test1_db
    db2 = test2_db

    token = db1.acquire('ident1', time, now)
    assert_not_equal nil, token
    assert_not_equal false, token

    # different host can't extend (even if same token)
    ok = db2.extend_timeout(token, now+TIMEOUT*2)
    assert_equal false, ok

    # same host can extend timeout
    ok = db1.extend_timeout(token, now+TIMEOUT*2)
    assert_equal true, ok

    # timeout is extended; different host can't lock
    token_ = db2.acquire('ident1', time, now+TIMEOUT+1)
    assert_equal false, token_

    # extended timeout is expired; different host can lock
    token = db2.acquire('ident1', time, now+TIMEOUT*2+1)
    assert_not_equal nil, token
    assert_not_equal false, token
  end
end

