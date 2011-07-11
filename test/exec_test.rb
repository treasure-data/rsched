require File.dirname(__FILE__)+'/test_helper'

class ExecTest < Test::Unit::TestCase
  it 'success' do
    success_sh  = File.expand_path File.dirname(__FILE__)+"/success.sh"
    e = RSched::ExecRunner.new(success_sh)

    ident = 'ident'
    time = Time.parse("2010-02-02 00:00:00 UTC").to_i
    action = 'act'

    assert_nothing_raised do
      e.call(ident, time, action)
    end
  end

  it 'fail' do
    fail_sh  = File.expand_path File.dirname(__FILE__)+"/fail.sh"
    e = RSched::ExecRunner.new(fail_sh)

    ident = 'ident'
    time = Time.parse("2010-02-02 00:00:00 UTC").to_i
    action = 'act'

    assert_raise(RuntimeError) do
      e.call(ident, time, action)
    end
  end

  it 'stdin' do
    cat_sh  = File.expand_path File.dirname(__FILE__)+"/cat.sh"
    out_tmp = File.expand_path File.dirname(__FILE__)+"/cat.sh.tmp"
    e = RSched::ExecRunner.new("#{cat_sh} #{out_tmp}")

    ident = 'ident'
    time = Time.parse("2010-02-02 00:00:00 UTC").to_i
    action = 'act'

    e.call(ident, time, action)

    assert_equal [ident, time, action].join("\t"), File.read(out_tmp)
  end

  it 'huge' do
    huge_sh  = File.expand_path File.dirname(__FILE__)+"/huge.sh"
    e = RSched::ExecRunner.new("#{huge_sh}")

    ident = 'ident'
    time = Time.parse("2010-02-02 00:00:00 UTC").to_i
    action = 'act'

    e.call(ident, time, action)

    # should finish
  end
end

