# Fire a callback/errback when all of a group of deferrables is finished.
#
# Essentially a "barrier" for deferrables; you define the set of
# deferrables you want to group together, and when they're all finished,
# only *then* does the callback(s) or errback(s) on this deferrable fire.
#
class PG::EM::Client::Helper::DeferrableGroup
	include ::EventMachine::Deferrable

	class ClosedError < StandardError; end

	# Create a new deferrable group.
	#
	def initialize
		@closed      = false
		@failure     = nil
		@outstanding = []

		yield(self) if block_given?
	end

	# Add a new deferrable to this group.
	#
	# @param df [EM::Deferrable] the deferrable to wait on.
	#
	# @return [EM::Deferrable] the same deferrable.
	#
	# @raise [PG::EM::Client::Helper::DeferrableGroup::ClosedError] if you
	#   attempt to add a deferrable after the group has been closed (that is,
	#   the `#close` method has been called), indicating that the deferrable
	#   group doesn't have any more deferrables to add.
	#
	def add(df)
		if @closed
			raise ClosedError,
			      "This deferrable group is closed"
		end

		@outstanding << df
		df.callback { completed(df) }.errback { |ex| failed(df, ex) }
	end

	# Tell the group that no further deferrables are to be added
	#
	# If all the deferrables in a group are complete, the group can't be sure
	# whether further deferrables may be added in the future.  By requiring
	# an explicit `#close` call before the group completes, this ambiguity is
	# avoided.  It does, however, mean that if you forget to close the
	# deferrable group, your code is going to hang.  Such is the risk of
	# async programming.
	#
	def close
		@closed = true
		maybe_done
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
		@failure ||= ex
		completed(df)
	end

	private
	# Called every time a deferrable finishes, just in case we're ready to
	# trigger our callbacks.
	def maybe_done
		if @closed and @outstanding.empty?
			if @failure
				fail(@failure)
			else
				succeed
			end
		end
	end
end
