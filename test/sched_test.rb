require File.dirname(__FILE__)+'/test_helper'

class SchedTest < Test::Unit::TestCase
  MINUTE = 60
  HOUR = 60*60
  DAY = 60*60*24

  it 'sched minutes' do
    now = Time.parse("2010-02-02 00:00:00 UTC").to_i
    sched_start = now

    sched = RSched::Engine::Sched.new("* * * * *", "act", sched_start, now, now)
    assert_equal [now], sched.queue

    sched = RSched::Engine::Sched.new("* * * * *", "act", sched_start, now, now+MINUTE*2)
    assert_equal [now, now+MINUTE, now+MINUTE*2], sched.queue

    sched = RSched::Engine::Sched.new("*/2 * * * *", "act", sched_start, now, now+MINUTE*2)
    assert_equal [now, now+MINUTE*2], sched.queue

    sched = RSched::Engine::Sched.new("0,1,3 * * * *", "act", sched_start, now, now+MINUTE*4)
    assert_equal [now, now+MINUTE, now+MINUTE*3], sched.queue
  end

  it 'sched hours' do
    now = Time.parse("2010-02-02 00:00:00 UTC").to_i
    sched_start = now

    sched = RSched::Engine::Sched.new("0 * * * *", "act", sched_start, now, now)
    assert_equal [now], sched.queue

    sched = RSched::Engine::Sched.new("0 * * * *", "act", sched_start, now, now+HOUR*2)
    assert_equal [now, now+HOUR, now+HOUR*2], sched.queue

    sched = RSched::Engine::Sched.new("0 */2 * * *", "act", sched_start, now, now+HOUR*2)
    assert_equal [now, now+HOUR*2], sched.queue

    sched = RSched::Engine::Sched.new("0 0,1,3 * * *", "act", sched_start, now, now+HOUR*4)
    assert_equal [now, now+HOUR, now+HOUR*3], sched.queue
  end

  it 'sched hours' do
    now = Time.parse("2010-02-02 00:00:00 UTC").to_i
    sched_start = now

    sched = RSched::Engine::Sched.new("0 0 * * *", "act", sched_start, now, now)
    assert_equal [now], sched.queue

    sched = RSched::Engine::Sched.new("0 0 * * *", "act", sched_start, now, now+DAY*2)
    assert_equal [now, now+DAY, now+DAY*2], sched.queue

    sched = RSched::Engine::Sched.new("0 0 */2 * *", "act", sched_start, now, now+DAY*2)
    assert_equal [now, now+DAY*2], sched.queue

    sched = RSched::Engine::Sched.new("0 0 2,3,5 * *", "act", sched_start, now, now+DAY*4)
    assert_equal [now, now+DAY, now+DAY*3], sched.queue
  end
end

