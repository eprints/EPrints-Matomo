
=head1 NAME

B<EPrints::Plugin::Event::MatomoEvent> - Event task to ping OpenAIRE

=head1 SYNOPSIS

Adapted from the official EPrints support package,
L<OAPiwik|https://github.com/openaire/EPrints-OAPiwik>.

Relies on the following configuration settings:

	# cfg.d/matomo_config.pl
	$c->{matomo}->{idsite} - site identifier
	$c->{matomo}->{token_auth} - authorization token
	$c->{matomo}->{tracker} - tracker URL
	$c->{matomo}->{max_payload} - limit of pings in one bulk request
	$c->{matomo}->{verbosity} - 0 or 1
	

=head1 DESCRIPTION

See L<EPrints::Plugin::Event> for standard methods.

There are two different jobs provided, corresponding to two phases of
operation.

With C<legacy_notify>, the job goes progresses through the entire table of
access events. A fake-but-likely request URL is generated for each one. With
each run of the job, a batch of up to $c->{matomo}->{max_payload} access events is sent to the tracker,
progress is recorded in a log for safety, and the job respawns to send the next
batch in 60 seconds. In the event of an error, the current batch is saved (stashed) to a
folder, and the job respawns in 60 minutes. On the next run, the saved batch is
reloaded and and the request is retried. If the request does not succeed for 24
hours, the job is rescheduled in 24 hours time; however, after a successful retry, normal
operation resumes with the next batch. When spawning a new job, the status of
the last run is included as a parameter for the sake of visibility, but is not
otherwise used.

The C<notify> job is intended to be triggered each time a new access event is
added to the database. Normally a HEAD request is sent to the tracker with the
tracking information provided in the query string; on error, the access event is
saved to a folder and a retry event scheduled in 5 minutes.


=cut

package EPrints::Plugin::Event::MatomoEvent;

use strict;

our $VERSION = v1.1.0;
our @ISA     = qw( EPrints::Plugin::Event );

use File::Copy;
use File::Path qw(make_path);
use JSON;
use POSIX qw(strftime);
use URI;
use LWP::ConnCache;
use Date::Calc qw(Add_Delta_Days);

use EPrints::Const
  qw(HTTP_OK HTTP_RESET_CONTENT HTTP_NOT_FOUND HTTP_LOCKED HTTP_INTERNAL_SERVER_ERROR);

=head1 CONSTANTS

=over

=item LEGACY_LOG

Relative path to record of last legacy ping.

=item SETUP_DONE

Relative path to file, if this file exists then intial setup has been performed

=item RETRY_DIR

Relative path to folder of failed pings.

=item RETRY_LOG

Relative path to record of retry attempts.

=item VIEW

Activity name for COUNTER Investigations.

=item DOWNLOAD

Activity name for COUNTER Requests.

=back

=cut

use constant {
	LEGACY_BATCH_NAME => 'legacy_access',
	LOG_FILES_DIR => '/matomo/',
	ERROR_LOG  => '/matomo-error.yaml',
	VIEW    => 'View',
	DOWNLOAD    => 'Download',
	# INTERNAL_TABLE => 'matomo_keyvalues'
};

=head1 METHODS

=head2 Constructor method

=over

=item $event = EPrints::Plugin::Event::MatomoEvent->new( %params )

The constructor has no special parameters of its own: all are passed to
the parent EPrints::Plugin::Event constructor.

=cut

sub new
{
	my ( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{actions} = [qw( enable disable )];

	$self->{package_name} = "MatomoEvent";

	$self->{json} = JSON->new->allow_nonref(1)->canonical(1)->pretty(1);

	if( defined $self->{session} )
	{
		$self->{dbh} = $self->{session}->get_database;
	}	

	return $self;
}

=back

=head2 High level public methods, corresponding to available job events

=over

=item $self->trigger_bulk_upload( $batch_name, $from_access_id, $to_access_id )

This commences a series of uploads for all the accesses from $from_access_id to $to_access_id in maximum chunks of max_payload
This must not be run from an Apache thread, only from cron or indexer.
This will create a log_file for the batch (so $batch_name must be unix file friendly) which tracks progress.
Indexer tasks will be scheduled until this bulk upload has finished.

returns status of the scheduled task
=cut

sub trigger_bulk_upload
{
	my ( $self, $batch_name, $from_access_id, $to_access_id ) = @_;
	my $repo = $self->{repository};
	# Maximum number of events in one upload:
	my $max_payload = $repo->config( 'matomo', 'max_payload' ) // 100;

	my $log_file = $repo->config('variables_path') . LOG_FILES_DIR . $batch_name . ".log";
	
	my $log = { tries_since_last_success => 0 };
	$log->{last_accessid} = $from_access_id;
	$log->{tries_since_last_success} = 0;
	$log->{maximum_access_id} = $to_access_id;

	# Save log file
	open( my $fh, ">", $log_file ) or do
	{
		$self->_log("initiate_historic_upload: Could not open $log_file: $!");
		return HTTP_INTERNAL_SERVER_ERROR;
	};
	print $fh $self->{json}->encode($log);
	close($fh) or warn "Failed to close $log_file: $!";

	# kick off the indexer 
	my $event = EPrints::DataObj::EventQueue->create_unique( $repo, {
		pluginid => 'Event::MatomoEvent',
		action => 'bulk_upload_batch',
		params => [ "First run of $batch_name access", $batch_name, EPrints::Time::iso_datetime() ],
	});

	return $event->get_value( "status" )

}

=item $status = $self->bulk_upload_batch( $self, [ $message, $batch_name, $unique_parameter ] )

Notifies the tracker of a batch of legacy access records, starting at the access id of C<$last_accessid> from the log file.
The C<$message> should be indicative of how the last run went;
this is a bit of a cheat, in order to make it clear in the Indexer task screen when the legacy upload has finished.

EventQueue->create_unique will return the same event if it has exactly the same parameters so C<$unique_parameter> exists purely so we definitely schedule another task.

=cut

sub bulk_upload_batch
{
	my ( $self, $message, $batch_name, $ignoreme ) = @_;
	my $repo = $self->{repository};
	# Maximum number of events in one upload:
	my $max_payload = $repo->config( 'matomo', 'max_payload' ) // 100;

	my $log_file = $repo->config('variables_path') . LOG_FILES_DIR . $batch_name . ".log";

	# Read log if exists (which it should as trigger_bulk_upload should have created it)
	my $log = { tries_since_last_success => 0 };
	if ( -f $log_file )
	{
		open( my $fh, "<", $log_file ) or do
		{
			$self->_log("legacy_notify: Could not open $log_file: $!");
			return HTTP_INTERNAL_SERVER_ERROR;
		};
		my $err = read( $fh, my $logcontent, -s $fh );
		if ( !defined $err )
		{
			$self->_log("legacy_notify: Could not read $log_file: $!");
			return HTTP_INTERNAL_SERVER_ERROR;
		}
		close($fh) or warn "Failed to close $log_file: $!";
		$log = eval { $self->{json}->decode($logcontent) } or do
		{
			$self->_log("legacy_notify: Could not parse $log_file: $@");
			return HTTP_INTERNAL_SERVER_ERROR;
		};
		if ( !exists $log->{maximum_access_id} ){
			$self->_log( "legacy_notify: maximum_access_id not set " );
			return HTTP_INTERNAL_SERVER_ERROR;
		}

	}else{
		$self->_log( "log file $log_file does not exist. Should have be created at first run." );
		return HTTP_INTERNAL_SERVER_ERROR;
	}

	$log->{last_run} = EPrints::Time::iso_datetime();
	$log->{last_accessid} //= 0;

	my $success = 0;
	my $finished = 0;

	my $max_access_id_to_search = $log->{last_accessid} + $max_payload;
	if ($max_access_id_to_search >= $log->{maximum_access_id}){
		$max_access_id_to_search = $log->{maximum_access_id} - 1;
	}
	my $accesses = $self->_accesses_between( $log->{last_accessid}, $max_access_id_to_search );
	my $accesses_count = scalar(@{$accesses});
	if ( $accesses_count > 0 )
	{
		$self->_log( "bulk_upload_batch: bulk pinging ".$accesses_count." accesses from ".$log->{last_accessid}." to $max_access_id_to_search" );
		
		$success = $self->_bulk_ping( $accesses );
		if($success){
			$log->{last_accessid} = $max_access_id_to_search;
			$log->{message} = "Successfully uploaded $accesses_count access entries";
		}else{
			$log->{message} = "Failed to upload access entries";
		}
	}
	else
	{
		$self->_log( "bulk_upload_batch: No more accesses found, no more bulk_upload_batch events will be scheduled for $batch_name." );
		$log->{last_accessid} = $max_access_id_to_search;
		$log->{message} = "Up to date";
		$finished = 1;
	}
	

	# Save log file
	open( my $fh, ">", $log_file ) or do
	{
		$self->_log("bulk_upload_batch: Could not open $log_file: $!");
		return HTTP_INTERNAL_SERVER_ERROR;
	};
	print $fh $self->{json}->encode($log);
	close($fh) or warn "bulk_upload_batch: Failed to close $log_file: $!";

	# How did it go?
	my $start_stamp = time();
	if ( $success )
	{
		# Do next batch in one minute (by default):
		$log->{tries_since_last_success} = 0;
		$start_stamp += $repo->config( 'matomo', 'bulk_upload_period_s' ) // 60;
		$log->{message} .= ", Uploaded up to ID ".$log->{last_accessid}.". Will stop when reached: ". ($log->{maximum_access_id}-1);
	}
	elsif ( $finished )
	{
		# All done!
		return HTTP_OK;
	}
	else
	{
		# Failed. Retry in an hour:
		$log->{tries_since_last_success}++;
		$start_stamp += ( 60 * 60 );
	}

	if ( $log->{tries_since_last_success} > 24 )
	{
		# Hasn't worked for a day, increase time by a day:
		$start_stamp += ( 60 * 60 * 24 );
	}
	

	$self->_log( "bulk_upload_batch: scheduling next run with message: ".$log->{message} );

	# Spawn new job
	my $task = EPrints::DataObj::EventQueue->create_unique(
		$self->{repository},
		{
			start_time => EPrints::Time::iso_datetime($start_stamp),
			pluginid   => $self->get_id,
			action     => "bulk_upload_batch",
			params     => [ $log->{message}, $batch_name, EPrints::Time::iso_datetime() ]
		}
	);
	$self->_log("bulk_upload_batch: new task status: " . $task->get_value( "status" ));
	return HTTP_OK;
}


=back

=head2 Low level supporting methods used in the above

=over

=item @accesses = $self->_accesses_between( $last_accessid, $current_accessid )

Loads all relevant access records I<after> the C<$last_accessid> and I<including>
the C<$current_accessid> and returns them as an array of arrayrefs, where the
only element is an C<$access> object (compatible with L<_unstash>).

=cut

sub _accesses_between
{
	my ( $self, $last_accessid, $current_accessid ) = @_;

	my $range_start = $last_accessid + 1;
	my $range_stop  = $current_accessid;

	my %params = (
		session       => $self->{repository},
		dataset       => $self->_access_dataset,
		search_fields => [
			{
				meta_fields => ['accessid'],
				value       => "$range_start-$range_stop",
				match       => 'EQ',
				merge       => 'ANY'
			},
			{ meta_fields => ['datestamp'],   match => 'SET' },
			{ meta_fields => ['referent_id'], match => 'SET' },
		],
		custom_order => "datestamp/accessid",
		allow_blank  => 1
	);
	my $search  = EPrints::Search->new(%params);
	my $results = $search->perform_search;
	my $total   = $results->count;
	$self->_log("_accesses_between: Loading $total access records.");

	my @accesses;
	foreach my $access ( $results->slice() )
	{
		# print STDERR "Pushing to accesses\n";
		# use Data::Dumper;
		# print STDERR Dumper($access);
		push @accesses, [$access];
	}
	return \@accesses;
}


=item $yesterday = $self->yesterdays_date( )

Get the date string for yesterday

=cut
sub yesterdays_date
{
	my ( $self ) = @_;
	

	my ($year,$mon,$day,$hour,$min,$sec) = EPrints::Time::utc_datetime();
	my ($y2, $m2, $d2) = Add_Delta_Days($year, $mon, $day, -1);

	my $yesterday = sprintf("%04d-%02d-%02d", $y2, $m2, $d2);
	return $yesterday;
}

=item $access_id = $self->_get_day_access_id( $day, $last )

Get the last/first access_id for a specific day where $day is %Y-%m-%d
$last: 1 for the last id in this date. 0 for the first

=cut

sub _get_day_access_id
{
	my ( $self, $day, $last ) = @_;

	my $order = "-datestamp";
	if (!$last){
		$order = "datestamp";
	}

	my %params = (
		session       => $self->{repository},
		dataset       => $self->_access_dataset,
		search_fields => [
			{
				meta_fields => ['accessid'],
				match => 'SET'
			},
			{ 
				meta_fields => ['datestamp'],
				value       => "$day",
				match => 'EQ'
			},
			{ meta_fields => ['referent_id'], match => 'SET' },
		],
		custom_order => $order,
		allow_blank  => 0,
		limit        => 1
	);
	my $search  = EPrints::Search->new(%params);
	my $results = $search->perform_search;
	my $total   = $results->count;

	if ($total != 1){
		$self->_log("Search returned $total results for accesses yesterday");
		return undef;
	}

	return @{$results->ids}[0];
	# use Data::Dumper;
	# # $Data::Dumper::Maxrecurse =2;
	# $Data::Dumper::Maxdepth = 2;
	# print STDERR Dumper($results);
	# my @accesses;
	# foreach my $access ( $results->slice() ){
	# 	push @accesses, [$access];
	# }
	# return @accesses;

	# return $results->slice();

}

=item ($id1, $id2) | $id2 = $self->_archive_id ( $any )

Takes Boolean. If true, returns an array containing both the v1 and v2
OAI identifiers for the current archive. Otherwise just returns the v2
one.

Reproduced from C<EPrints::OpenArchives::archive_id>.

=cut

sub _archive_id
{
	my ( $self, $any ) = @_;
	my $repo = $self->{repository};

	my $v1 = $repo->config( 'oai', 'archive_id' );
	my $v2 = $repo->config( 'oai', 'v2', 'archive_id' );

	$v1 ||= $repo->config('host');
	$v1 ||= $repo->config('securehost');
	$v2 ||= $v1;

	return $any ? ( $v1, $v2 ) : $v2;
}

=item %qf_params = $self->_to_form( $access, [ $request_url ] )

Convert an C<$access> object into a
L<Matomo Tracking HTTP API|https://developer.matomo.org/api-reference/tracking-api>
query form.

A request URL will be calculated if blank or undefined.

Does not insert the C<token_auth> as this is handled differently depending on
whether a singular or bulk call is made.

Returns empty if the C<$access> does not have reportable data.

=cut

sub _as_form
{
	my ( $self, $access, $request_url ) = @_;
	my $repo = $self->{repository};

	# Required parameters:
	my %qf_params = (
		idsite => $repo->config( 'matomo', 'idsite' ),
		rec    => '1',
	);

	# COUNTER classification
	my $is_request =
		 $access->is_set('service_type_id')
	  && $access->value('service_type_id') eq '?fulltext=yes'
	  ? 1
	  : 0;

	# Recommended parameters:
	# - action_name ("The title of the action being tracked.")
	my $action_name = $is_request ? DOWNLOAD : VIEW;
	$qf_params{action_name} = $action_name;

	# - url
	if ( !$request_url )
	{
		if ( $is_request && $access->is_set('referent_docid') )
		{
			my $doc =
			  $self->_document_dataset->get_object( $repo,
				$access->value('referent_docid') );
			if ( defined $doc )
			{
				$request_url = $doc->get_url();
			}
			elsif ( $access->is_set('referent_id') )
			{
				# Should only get here if the document has since been deleted.
				# So long as the first bit of the URL identifies the dataset,
				# the last bit doesn't have to be accurate, so long as it's
				# consistent for a given file
				$request_url =
					$repo->config('base_url') . '/'
				  . $access->value('referent_id') . '/'
				  . $access->value('referent_docid');
			}
			else
			{
				# Should never get here.
				$request_url =
					$repo->config('base_url')
				  . '/id/document/'
				  . $access->value('referent_docid');
			}
		}
		elsif ( $access->is_set('referent_id') )
		{
			my $eprint =
			  $self->_eprint_dataset->get_object( $repo, $access->value('referent_id') );
			$request_url =
			  defined $eprint
			  ? $eprint->get_url()
			  : $repo->config('base_url') . '/' . $access->value('referent_id');
		}
		else
		{
			# If both eprint and document IDs are null, ignore.
			return;
		}
	}
	$qf_params{url} = $request_url;

	# - apiv (API version)
	$qf_params{apiv} = '1';

	# Optional User info:
	# - urlref (The full HTTP Referrer URL)
	if ( $access->is_set('referring_entity_id') )
	{
		my $referer = $access->value('referring_entity_id');
		$referer =~ s/^(.{1024}).*$/$1/s;    # stop it getting too long
		$qf_params{urlref} = $referer;
	}

	# User Agent
	if ( $access->is_set('requester_user_agent') )
	{
		$qf_params{ua} = $access->value('requester_user_agent');
	}

	# Optional Action info:
	# - cvar (custom variable, stringified JSON containing OAI PMH ID)
	if ( $access->is_set('referent_id') )
	{
		my $oai_id =
		  EPrints::OpenArchives::to_oai_identifier( $self->_archive_id(),
			$access->value('referent_id'),
		  );
		$qf_params{cvar} = '{"1":["oaipmhID","' . $oai_id . '"]}';
	}

	# - download (URL of a file the user has downloaded).
	if ($is_request)
	{
		$qf_params{download} = $request_url;
	}

	# Parameters requiring authentication:
	# - cip (Override value for the visitor IP)
	if ( $access->is_set('requester_id') )
	{
		$qf_params{cip} = $access->value('requester_id');
	}

	# - cdt (Override for the datetime of the request)
	if ( $access->is_set('datestamp') )
	{
		$qf_params{cdt} = $access->value('datestamp');
	}

	return %qf_params;
}

=item initiate_historic_upload( $repository )

Schedule the first indexer task which will slowly upload all historic access data from {matomo}->{legacy_start_access_id} to the end of yesterday (UTC)

=cut

sub initiate_historic_upload
{
	my ( $self, $repo, $from_access_id ) = @_;

	$from_access_id //= $repo->config( 'matomo', 'legacy_start_access_id' );
	$self->_ensure_path_exists($repo->config('variables_path') . LOG_FILES_DIR);

	my $log_file = $repo->config('variables_path') . LOG_FILES_DIR . LEGACY_BATCH_NAME . ".log";

	if(-e $log_file){
		# log file exists, so the legacy batch is already in progress
		$self->_log("Legacy access data is already being uploaded. Do you want to restart batch ". LEGACY_BATCH_NAME. " instead?");
		return 0;
	}
	my $today = EPrints::Time::iso_date();
	#trigger up to but not including the first result today
	#the cron job will run in the early hours tomorrow morning to send off today's (it's yesterday) accesses
	my $to_access_id = $self->_get_day_access_id($today, 0);

	if(! defined $to_access_id){
		$self->_log("Unable to find access id from the beginning of today. Unable to initiate historic data upload");
		return 0;
	}

	$self->_log("initiate_historic_upload: Will commence " . LEGACY_BATCH_NAME ." from access id $from_access_id to $to_access_id");
	
	my $status = $self->trigger_bulk_upload(LEGACY_BATCH_NAME, $from_access_id , $to_access_id );
	
	return $status;

}

=item initiate_yesterdays_upload( $repository )

Schedule the first indexer task which will slowly upload all access data from yesterday (UTC)

=cut

sub initiate_yesterdays_upload
{
	my ( $self, $repo ) = @_;

	my $yesterday = $self->yesterdays_date();
	my $batch_name = "daily_".$yesterday;

	$self->_ensure_path_exists($repo->config('variables_path') . LOG_FILES_DIR);

	my $log_file = $repo->config('variables_path') . LOG_FILES_DIR . "$batch_name.log";

	if(-e $log_file){
		# log file exists, so the batch is already in progress
		$self->_log("Yesterday's access data is already being uploaded. Do you want to restart batch $batch_name instead?");
		return 0;
	}
	my $today = EPrints::Time::iso_date();
	my $from_access_id = $self->_get_day_access_id($yesterday, 0);
	my $to_access_id = $self->_get_day_access_id($yesterday, 1);

	if(! defined $to_access_id || ! defined $from_access_id){
		$self->_log("Unable to find access id from the beginning or end of yesterday. Unable to initiate data upload");
		return 0;
	}

	$self->_log("initiate_yesterdays_upload: Will commence $batch_name from access id $from_access_id to $to_access_id");
	
	my $status = $self->trigger_bulk_upload($batch_name,  $from_access_id, $to_access_id );
	
	return $status;
}

=item restart_batch( $repository, $batch_name )

Reschedule an indexer task for a specific batch

=cut

sub restart_batch
{
	my ( $self, $repo, $batch_name ) = @_;
	$self->_ensure_path_exists($repo->config('variables_path') . LOG_FILES_DIR);

	my $log_file = $repo->config('variables_path') . LOG_FILES_DIR . "$batch_name.log";

	if(-e $log_file){
		# log file exists, so the batch has run before
		my $event = EPrints::DataObj::EventQueue->create_unique( $repo, {
			pluginid => 'Event::MatomoEvent',
			action => 'bulk_upload_batch',
			params => [ "Restart batch $batch_name", $batch_name, EPrints::Time::iso_datetime() ],
		});
	
		return $event->get_value( "status" );
	}else{
		$self->_log("No log file found for $batch_name. Cannot restart.")
	}
	return 0;
}

=item $message = $self->_bulk_ping( $accesses, [ $is_recovery ] )

Sends a POST request to the configured Matomo Tracking HTTP API, registering
multiple access events at once. The C<$accesses> argument must be an arrayref
containing arrayrefs, where the first item is an C<$access> object and the
optional second item is a request URL.

Bulk tracking is handled by the Matomo
L<BulkTracking|https://github.com/matomo-org/matomo/blob/5.x-dev/plugins/BulkTracking/>
plugin.

If C<$is_recovery> evaluates true, successfully sent access events will be
logged, so they can be compared against the lists of previously stashed ones.

Any events that failed to send are stashed.

Returns a 1 for success or 0 for failure.

=cut

sub _bulk_ping
{
	my ( $self, $accesses) = @_;
	my $repo = $self->{repository};

	# my $tracker_url = URI->new( $repo->config( 'matomo', 'tracker' ) );
	
	my $token_auth  = $repo->config( 'matomo', 'token_auth' );

	# Filter out unusable events:
	my @events;
	foreach my $tuple ( @{$accesses} )
	{
		my ( $access, $request_url ) = @{$tuple};
		my %qf_params = $self->_as_form( $access, $request_url );
		if (%qf_params)
		{
			push @events,
			  {
				a => $access,
				u => $qf_params{url},
				p => \%qf_params,
			  };
		}
	}

	# Can't continue if that leaves us with nothing:
	if  (!@events){
		self->log("No usable events to upload");
		return 0;
	};

	# Can't continue if can't authenticate, so stash:
	if ( !defined $token_auth )
	{
		self->log('Missing authorization token');
		return 0;
	}

	# According to BulkTracking/Tracker/Requests.php, each member of
	# the requests array can be either be a URL string (in which case
	# the URL is parsed, then the query part is parsed again for
	# parameters), or a hash (in which case it is used directly): so
	# we may as well avoid the round trip and deliver hashes.
	my $payload = {
		requests   => [],
		token_auth => $token_auth,
	};
	foreach my $event (@events)
	{
		push @{ $payload->{requests} }, $event->{p};
	}

	my $tracker_url = HTTP::Request->new('POST', $repo->config( 'matomo', 'tracker' ));

	$tracker_url->header( 'Content-Type' => 'application/json' );
	
	# Turn into payload JSON string.
	my $content = $self->{json}->encode($payload);

	$tracker_url->content( $content );

	my $response = $self->_user_agent->request($tracker_url);
	# if we submmit json with the correct content-type header, we should get json back
	my $returned_json = eval { $self->{json}->decode($response->decoded_content) } or do
	{
		$self->_log("Could not parse expected json response from Matomo ", 1);
		return 0;
	};
	if (   $response->header('Client-Warning')
		&& $response->header('Client-Warning') eq 'Internal response' )
	{
		$self->_log('Failed to send request', 1);
		return 0;
	}
	elsif ( $response->code > 399 )
	{
		$self->_log('Tracker responded ' . $response->code . ' ' . $response->message. " Content: " . $response->decoded_content, 1);
		return 0;
	}
	elsif(!defined $returned_json->{invalid} || !defined $returned_json->{tracked} || !defined $returned_json->{status})
	{
		$self->_log('Tracker did not respond with expected JSON format', 1);
		return 0;
	}elsif($returned_json->{status} ne "success")
	{
		$self->_log('Tracker responded with status :' . $returned_json->{status}, 1);
		return 0;
	}
	elsif ($returned_json->{tracked} == 0) 
	{
		$self->_log('Tracker responded with no tracked events', 1);
		return 0;
	}
	elsif (int($returned_json->{invalid}) > 0 || $returned_json->{tracked} != scalar(@events)) 
	{
		$self->_log('Tracker responded with fewer tracked events that submitted. Tracked events: ' . $returned_json->{tracked} . " expected: " .scalar(@events), 1);
		# assumption here - if we had ANY successfully tracked events then we should just continue. Otherwise
		# there's a risk we get stuck forever trying to upload a broken event.
		# we just above got more general failures and zero tracked events, so hopefully this only occurs with the odd broken event?
		return 1;
	}
	else
	{
		$self->_log("_bulk_ping successfully submitted " . scalar(@events) . " access events");
		return 1;
	}
}

=item $ds = $self->_access_dataset

Returns access dataset.

=cut

sub _access_dataset
{
	my ($self) = @_;

	if ( !defined $self->{access_ds} )
	{
		$self->{access_ds} = $self->{repository}->dataset('access');
	}

	return $self->{access_ds};
}

=item $ds = $self->_document_dataset

Returns document dataset.

=cut

sub _document_dataset
{
	my ($self) = @_;

	if ( !defined $self->{document_ds} )
	{
		$self->{document_ds} = $self->{repository}->dataset('document');
	}

	return $self->{document_ds};
}

=item $ds = $self->_eprint_dataset

Returns eprints dataset.

=cut

sub _eprint_dataset
{
	my ($self) = @_;

	if ( !defined $self->{eprint_ds} )
	{
		$self->{eprint_ds} = $self->{repository}->dataset('eprint');
	}

	return $self->{eprint_ds};
}

=item $bool = $self->_ensure_path_exists( $path )

Returns 1 if path already exists or has been successfully created.
Returns 0 if path still does not exist after best efforts.

=cut

sub _ensure_path_exists
{
	my ( $self, $path ) = @_;

	if ( -d $path )
	{
		return 1;
	}

	my @created = make_path( $path, { error => \my $err } );
	if ( $err && @{$err} )
	{
		for my $diag ( @{$err} )
		{
			my ( $f, $msg ) = %{$diag};
			if ( $f eq '' )
			{
				$msg = ": $msg";
			}
			else
			{
				$msg = " $f: $msg";
			}
			$self->_log("_ensure_path_exists: Error creating directory$msg");
		}
		return 0;
	}

	return 1;
}

=item $ok = $self->_err_log( $msg, %params )

Appends a message to the dedicated error log. This could either be an error or
the correction of an earlier error.

The output format is YAML flavoured but may not be entirely valid YAML.

Recognised keys for the C<%params>:

=over

=item sent

Access events that have been sent in bulk. Typically, they will have previously
been stashed. Value should be an arrayref of arrayrefs, where the first element
is an Access DataObj and the second (if present) is a request URL.

=item stashed

Access events that have been stashed instead of being sent. Value should be as
for B<sent>.

=item response

Body of the error response sent back by Matomo.

=back

=cut

sub _err_log
{
	my ( $self, $msg, %params ) = @_;
	my $repo = $self->{repository};

	my @lines;

	push @lines, "time: " . EPrints::Time::iso_datetime();
	push @lines, "message: $msg";
	if ( defined $params{response} )
	{
		push @lines, "response: >";
		my @response_lines = split( "\n", $params{response} );
		foreach my $response_line (@response_lines)
		{
			push @lines, "  $response_line";
		}
	}
	foreach my $key ( 'sent', 'stashed' )
	{
		if ( defined $params{$key} )
		{
			push @lines, "$key:";
			foreach my $tuple ( @{ $params{$key} } )
			{
				my ( $access, $request_url ) = @{$tuple};
				$request_url //= '???';
				push @lines, '- ' . $access->id . " > $request_url";
			}
		}
	}

	my $error = join( "\n  ", @lines );

	my $error_file = $repo->config('variables_path') . ERROR_LOG;
	open( my $fh, '>>', $error_file ) or do
	{
		$self->_log("_err_log: Could not log error:\n  $error\n  $!");
		return 0;
	};
	print $fh "- $error\n";
	close($fh) or warn "Failed to close $error_file: $!";
	return 1;
}

=item $self->_log( $msg )

Write message to Indexer log.

=cut

sub _log
{
	my ( $self, $msg, $always ) = @_;
	if ( $self->{repository}->config( 'matomo', 'verbosity' ) || (defined $always && $always ) )
	{
		$self->{repository}->log("MatomoEvent::$msg");
		select()->flush();
	}
	return;
}

=item $ua = $self->_user_agent

User agent for making calls to APIs.

=cut

sub _user_agent
{
	my ($self) = @_;

	if ( !defined $self->{ua} )
	{
		my $conn_cache = LWP::ConnCache->new();
		$self->{ua} = LWP::UserAgent->new( conn_cache => $conn_cache );
		$self->{ua}->env_proxy;
	}

	return $self->{ua};
}

# =item $self->create_internal_tables()

# Duplicated code taken from irstats, this provides us with a thread safe store of key-value pairs.

# There is a feature request for EPrints 3.5 to have a generic mechanism to do this, but until then we'll create our own table

# =cut

# sub create_internal_tables
# {
# 	my( $self ) = @_;
	
# 	my $dbh = $self->{dbh};
# 	if (!defined $dbh){
# 		return 0;
# 	}

# 	my $rc = 1;	
# 	if( $dbh->has_table( $INTERNAL_TABLE ) )
# 	{
# 		return 1;
# 	}
	
# 	my $session = $self->{session};

# 	my @fields;

# 	push @fields, EPrints::MetaField->new(
# 				repository => $session->get_repository,
# 				name => "key",
# 				type => "text",
# 				maxlength => 255,
# 				sql_index => 0
# 	);

# 	push @fields, EPrints::MetaField->new(
# 				repository => $session->get_repository,
# 				name => "value",
# 				type => "text",
# 				maxlength => 255,
# 				sql_index => 0
# 	);

# 	$rc &= $self->_create_table( $INTERNAL_TABLE, 1, @fields );

# 	return $rc;
# }

# # Retrieves the stored value '$key'. The main use of this is for locking (see above) and also to keep track of the accessid when doing
# # incremental updates of the stats (so to know where to restart from).
# sub get_internal_value
# {
# 	my( $self, $key ) = @_;
	
# 	my $dbh = $self->{dbh};
# 	return undef unless( defined $dbh && $dbh->has_table( $INTERNAL_TABLE ) );

# 	my $Q_value = $dbh->quote_identifier( 'value' );
# 	my $sql = "SELECT $Q_value FROM ".$dbh->quote_identifier( $INTERNAL_TABLE )." WHERE ".$dbh->quote_identifier("key")."=".$dbh->quote_value($key);
# 	my $sth = $dbh->prepare( $sql );
#         $dbh->execute( $sth, $sql );
# 	my @r = $sth->fetchrow_array;
# 	return $r[0];
# }

# # sets the internal value '$key'
# # $replace is an optional boolean which asks to replace the current value (if any), otherwise the current value will be +=
# # Default behaviour is to replace the existing value
# sub set_internal_value
# {
# 	my( $self, $key, $newval, $replace, $insert ) = @_;
	
# 	$replace = 1 unless( defined $replace );
# 	$insert ||= 0;

# 	my $dbh = $self->{dbh};
# 	return undef unless( defined $dbh );

# 	my $curval = $self->get_internal_value( $key );
# 	if( defined $curval && !$insert )
# 	{
# 		$newval += $curval unless( defined $replace );
# 		return $dbh->_update( $INTERNAL_TABLE, ['key'], [$key], ['value'], [$newval] );
# 	}
# 	else
# 	{
#                 my @v;
#                 push @v, [$key, $newval];
#                 return $dbh->insert( $INTERNAL_TABLE, ['key', 'value'], @v );
# 	}

# 	return 0;
# }

# # Removes a previously stored internal value.
# sub reset_internal_value
# {
# 	my( $self, $key ) = @_;

# 	return 1 unless( defined $key );

# 	my $Q_tablename = $self->{dbh}->quote_identifier( $INTERNAL_TABLE );
# 	my $Q_key = $self->{dbh}->quote_identifier( "key" );
# 	my $Q_value = $self->{dbh}->quote_value( $key );

# 	return $self->{dbh}->do( "DELETE FROM $Q_tablename WHERE $Q_key = $Q_value" );
# }


=back

=cut

1;
