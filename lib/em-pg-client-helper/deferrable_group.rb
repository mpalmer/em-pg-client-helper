# Fire a callback/errback when all of a group of deferrables is finished.
#
# Essentially a "barrier" for deferrables; you define the set of
# deferrables you want to group together, and when they're all finished,
# only *then* does the callback(s) or errback(s) on this deferrable fire.
#
class PG::EM::Client::Helper::DeferrableGroup
	include ::EventMachine::Deferrable

	# Create a new deferrable group.
	#
	def initialize
		@failed = false
		@finished = false
		@first_failure = nil
		@outstanding = []
		yield(self) if block_given?
	end

	# Add a new deferrable to this group.
	#
	# @param df [EM::Deferrable] the deferrable to wait on.
	#
	# @return [EM::Deferrable] the same deferrable.
	#
	# @raise [RuntimeError] if you attempt to add a deferrable after the
	#   group has already "completed" (that is, all deferrables that were
	#   previously added to the group have finished).
	#
	def add(df)
		if @finished
			raise RuntimeError,
			      "This deferrable group has already completed."
		end

		@outstanding << df
		df.callback { completed(df) }.errback { |ex| failed(df, ex) }
	end

	# Mark a deferrable as having been completed.
	#
	# If this is the last deferrable in the group, then the callback/errback
	# will be triggered.
	#
	# @param df [EM::Deferrable]
	#
	def completed(df)
		@outstanding.delete(df)
		maybe_done
	end

	# Register that a given deferrable completed, but has failed.
	#
	# As soon as `failed` has been called, the deferrable group is guaranteed
	# to fail, no matter how many of the deferrables in the group succeed.
	# If this is the first deferrable in the group to have failed, then `ex`
	# will be the exception passed to the `errback`, otherwise the exception
	# will unfortunately be eaten by a grue.
	#
	def failed(df, ex)
		@first_failure ||= ex
		@failed = true
		completed(df)
	end

	private
	# Called every time a deferrable finishes, just in case we're ready to
	# trigger our callbacks.
	def maybe_done
		if @outstanding.empty?
			@finished = true
			if @failed
				fail(@first_failure)
			else
				succeed
			end
		end
	end
end
