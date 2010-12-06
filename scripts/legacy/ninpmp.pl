#!perl

=head1 NAME

ninpmp.pl - NRT Input Monitor for Pre-Processing

=head1 SYNOPSIS

I<ninpmp.pl> - Run from within B<OPUS>.  This is a monitoring process
within the Input pipeline.  

=head1 DESCRIPTION

This process monitors Pre-Processing and sends alerts in the case of problems.

The process runs a shell script B<spvspp.opus> on the anaB6 machine which 
checks that preproc is in the process list as running.  If it is, the shell
script returns a status of 0 and the monitor quits happily.  If not, the
shell script returns a status of 1 and the monitor decides whether or not 
to send an alert.  

The monitor runs every minute,
and an alert every minute until the problem is fixed is overkill.  Therefore,
the monitor keeps track of the last time it sent an alert simply by placing
a file in the workspace whose name is the time of the last alert sent.  The
time between alerts is the DELTA in the resource file, currently 10 minutes.
If fewer than ten minutes have gone by since an alert was sent, it will simply
log the fact that the problem remains.  If more than 10 minutes has gone by,
it will send another alert.  The next time it then finds preproc running
correctly, it removes this dated file.  

If it cannot send an alert, it will log the fact but quit with a status of
zero so that the process continues to try repeatedly.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<LOG_FILES>

This is where it logs the alerts sent, set to /isdc/log/nrtinput.  

=item B<ALERTS>

This is where to write alerts, set to the "rttm_alerts" entry in the path file.

=item B<DELTA>

This is the time in minutes between multiple alerts, currently 10 minutes.  

=item B<DELTA_TIME>

This is the OPUS trigger frequency in DDD:HH:MM:SS, currently set to 30 
seconds, not to be confused with the DELTA between alert generations above.

=back

=cut

#
# This is the input monitor for preprocessing process.  
# The intention is to have it trigger every so often (say once per min?) 
# and have it run.
#
# spsvrpp - monitors to see if the pp process is still running *NOT WRITTEN*

use strict;
use ISDCPipeline;
use UnixLIB;
use TimeLIB;
use ISDCLIB;

my $delta = $ENV{DELTA}; # delta between multiple alerts in minutes
&ISDCPipeline::EnvStretch ( "LOG_FILES", "ALERTS", "PARFILES" );

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{LOG_FILES}" ) unless ( -d "$ENV{LOG_FILES}" );

$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/nrt_pp_monitor.log";

#
# simply searches for Preproc in process table and errors if not found
#
#  (can't use ssh anymore, prompts for password)
my ( $retval, @result ) = &ISDCPipeline::RunProgram (
	"ssh anaB6 $ENV{ISDC_ENV}/opus/nrtinput/spvspp.opus","quiet" );

my $time  = &TimeLIB::MyTime ();
my $times = &TimeLIB::MyTimeSec ();
my ( $retval2, @result2 );
#
#  with new ops setup, exit value not passed anymore, always 0 if 
#   the rsh itself succeeds.   
#
#  So have to look at result string

if ( ! ($retval) && ( $result[0] =~ /running/ ) ) {
	print "preproc is running at $time.\n";
	# remove previous "lock" file if there was one
	my ( $lock ) = sort ( glob ( "$ENV{OPUS_WORK}/nrtinput/scratch/pp_alert_lock*" ) );
	unlink "$lock" if ( $lock );
	exit 0;
}
#
# in this case, either script failed or preproc not running, 
#  only send if previous alert is $ENV{DELTA} old 
#
else { # in case of error
	if ( $retval ) {
		# this not 0 means rsh failed
		print "ERROR:  cannot ssh to machine anaB6:\n@result";
	}
	else {
		print "\n********\npreproc not present or unverifiable at $time\n";
	}
	# look for "lock" file indicating that an alert has already been sent;
	my $lock = glob ( "$ENV{OPUS_WORK}/nrtinput/scratch/pp_alert_lock*" );
	
	if ($lock) { # a lock file exists
		my $lasttime = $lock;
		$lasttime =~ s/.*pp_alert_lock_(.*)$/$1/;
		$delta = $delta*60;
		my $dtime = $times;
		if ($dtime < $lasttime + $delta) { # > $delta min since last alert
			print "Alert already sent less than $ENV{DELTA} minutes ago at $time;  logging\n";
			($retval2,@result2) = &ISDCPipeline::RunProgram(
				"ril_write LogType=Warning_0 Message=\"Preproc not present or unverifiable at "
					."$time\" Task= ProcedureNumber=0000", "quiet");
			die "*******     ERROR:  Cannot run command \'ril_write LogType=Warning_0 Message=\"Preproc not "
				."present or unverifiable at $time\" Task= ProcedureNumber=\':\n@result2" if ($retval2);
			exit 0;
		}
		else { # less than $delta min since last alert
			# remove current lock file (will be replaced below)
			unlink $lock;
			# send new alert since it's at least 10 minutes since last
			print "Sending alert at $time\n";
		}
	} # end of if ($lock)
	
	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ALERTS}" ) unless ( -d "$ENV{ALERTS}" );
	
	&ISDCPipeline::WriteAlert (
		"step"    => "NRT Input Monitor PP - ALERT",
		"level"   => 3,
		"message" => "Preproc not present or unverifiable at $time",
		"subdir"  => $ENV{ALERTS},
		"id"      => "300",
		);
	
	print "Alert sent\n\n";
	# create new alert lock file
	open LOCK, ">$ENV{OPUS_WORK}/nrtinput/scratch/pp_alert_lock_$times";
	close LOCK;
	exit 0;
} # end of else (from if ($retval) )


######################################################################



__END__ 


=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the Input Pipeline, please see the Input 
Pipeline ADD.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

