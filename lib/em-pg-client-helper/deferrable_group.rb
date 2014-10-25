# Fire a callback/errback when all of a group of deferrables is finished.
#
# Essentially a "barrier" for deferrables; you define the set of
# deferrables you want to group together, and when they're all finished,
# only *then* does the callback(s) or errback(s) on this deferrable fire.
#
class PG::EM::Client::Helper::DeferrableGroup
	include ::EventMachine::Deferrable

	def initialize
		@failed = false
		@first_failure = nil
		@outstanding = []
		yield(self) if block_given?
	end

	def add(df)
		@outstanding << df
		df.callback { completed(df) }.errback { |ex| failed(df, ex) }
	end

	def completed(df)
		@outstanding.delete(df)
		maybe_done
	end

	def failed(df, ex)
		@first_failure ||= ex
		@failed = true
		completed(df)
	end

	private
	def maybe_done
		if @outstanding.empty?
			if @failed
				fail(@first_failure)
			else
				succeed
			end
		end
	end
end
