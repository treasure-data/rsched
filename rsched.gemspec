# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run the gemspec command
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{rsched}
  s.version = "0.1.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Sadayuki Furuhashi"]
  s.date = %q{2011-07-11}
  s.default_executable = %q{rsched}
  s.email = %q{frsyuki@gmail.com}
  s.executables = ["rsched"]
  s.extra_rdoc_files = [
    "README.rdoc"
  ]
  s.files = [
    "bin/rsched",
     "lib/rsched/command/rsched.rb"
  ]
  s.rdoc_options = ["--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{Generic Reliable Scheduler}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<cron-spec>, ["= 0.1.2"])
      s.add_runtime_dependency(%q<dbi>, ["~> 0.4.5"])
    else
      s.add_dependency(%q<cron-spec>, ["= 0.1.2"])
      s.add_dependency(%q<dbi>, ["~> 0.4.5"])
    end
  else
    s.add_dependency(%q<cron-spec>, ["= 0.1.2"])
    s.add_dependency(%q<dbi>, ["~> 0.4.5"])
  end
end

