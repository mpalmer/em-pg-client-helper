module TxnHelper
	def expect_query_failure(q, args=[], err=nil, exec_time = 0.001)
		err ||= RuntimeError.new("A mock exec_defer failure")
		expect_query(q, args, exec_time, :fail, err)
	end

	def expect_query(q, args=[], exec_time = 0.001, disposition = :succeed, *disp_opts)
		df = EM::DefaultDeferrable.new

		expect(mock_conn)
		  .to receive(:exec_defer)
		  .with(*[q, args].compact)
		  .and_return(df)
		  .ordered

		EM.add_timer(exec_time) do
			df.__send__(disposition, *disp_opts)
		end
	end

	def in_transaction(*args, &blk)
		db_transaction(mock_conn, *args, &blk).callback { EM.stop }.errback { EM.stop }
	end

	def in_em
		EM.run do
			EM.add_timer(0.5) { EM.stop; raise "test timeout" }
			yield
		end
	end
end
