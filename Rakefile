require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "rsched"
    gemspec.summary = "Generic Reliable Scheduler"
    gemspec.author = "Sadayuki Furuhashi"
    gemspec.email = "frsyuki@gmail.com"
    #gemspec.homepage = "http://.../"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "cron-spec", "= 0.1.2"
    gemspec.add_dependency "dbi", "~> 0.4.5"
    #gemspec.add_dependency "dbd-sqlite3", "~> 1.2.5"
    #gemspec.add_dependency "dbd-mysql", "~> 0.4.4"
    #gemspec.test_files = Dir["test/**/*.rb"]
    gemspec.files = Dir["bin/**/*", "lib/**/*", "test/**/*.rb"]
    gemspec.executables = ['rsched']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

VERSION_FILE = "lib/rsched/version.rb"

#file VERSION_FILE => ["VERSION"] do |t|
#  version = File.read("VERSION").strip
#  File.open(VERSION_FILE, "w") {|f|
#    f.write <<EOF
#module RSched
#
#VERSION = '#{version}'
#
#end
#EOF
#  }
#end
#
#task :default => [VERSION_FILE, :build]
task :default => :build

