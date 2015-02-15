While [`em-pg-client`](https://github.com/royaltm/ruby-em-pg-client) is a
nice solution to the problem of accessing a PostgreSQL database from within
[EventMachine](http://rubyeventmachine.com/), it is somewhat... spartan. 
ORMs have spoiled us somewhat, and so the appeal of hand-writing SQL and
dealing with the finer details of error handling has faded.  Hence the
creation of the {PG::EM::Client::Helper} module.  It contains a collection
of useful helper methods that make it somewhat easier to perform common
operations against PgSQL databases.


# Installation

It's a gem:

    gem install em-pg-client-helper

If you're the sturdy type that likes to run from git:

    rake build; gem install pkg/em-pg-client-helper-<whatever>.gem

Or, if you've eschewed the convenience of Rubygems, then you presumably know
what to do already.


# Usage

To use any of these methods, you will want to add the following require:

    require 'em-pg-client-helper'

Then add this line in any classes you wish to use the helper methods in:

    include PG::EM::Client::Helper

The module documentation for {PG::EM::Client::Helper} has more information on
the available methods and how to use them.


# Contributing

Bug reports should be sent to the [Github issue
tracker](https://github.com/mpalmer/em-pg-client-helper/issues), or
[e-mailed](mailto:theshed+em-pg-client-helper@hezmatt.org).  Patches can be
sent as a Github pull request, or
[e-mailed](mailto:theshed+em-pg-client-helper@hezmatt.org).
