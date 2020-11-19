require 'git-version-bump'

Gem::Specification.new do |s|
	s.name = "em-pg-client-helper"

	s.version = GVB.version
	s.date    = GVB.date

	s.platform = Gem::Platform::RUBY

	s.homepage = "http://github.com/mpalmer/em-pg-client-helper"
	s.summary = "Simplify common operations using em-pg-client"
	s.authors = ["Matt Palmer"]

	s.extra_rdoc_files = ["README.md"]
	s.files = `git ls-files`.split("\n")

	s.add_runtime_dependency "em-pg-client",     "~> 0.3"
	s.add_runtime_dependency "git-version-bump", "~> 0.10"
	s.add_runtime_dependency "sequel",           ">= 3.31.0", "< 6"

	s.add_development_dependency 'bundler'
	s.add_development_dependency 'eventmachine'
	s.add_development_dependency 'github-release'
	s.add_development_dependency 'guard-spork'
	s.add_development_dependency 'guard-rspec'
	# Needed for guard
	s.add_development_dependency 'rb-inotify', '~> 0.9'
	if RUBY_VERSION =~ /^1\./
		s.add_development_dependency 'pry-debugger'
	else
		s.add_development_dependency 'pry-byebug'
	end
	s.add_development_dependency 'rake', '~> 10.4', '>= 1.0.4.2'
	s.add_development_dependency 'redcarpet'
	s.add_development_dependency 'rspec'
	s.add_development_dependency 'yard'
end
