require 'spork'

Spork.prefork do
	require 'bundler'
	Bundler.setup(:default, :development, :test)
	require 'rspec/core'
	require 'rspec/mocks'

	require 'pry'

	RSpec.configure do |config|
		config.fail_fast = true
#		config.full_backtrace = true

		config.expect_with :rspec do |c|
			c.syntax = :expect
		end
	end
end

Spork.each_run do
	require 'em-pg-client-helper'
	
	RSpec.configure do |config|
		config.include ::PG::EM::Client::Helper
	end
end
