#!perl

=head1 NAME

ninpmd.pl - NRT Input Monitor for Data Receipt

=head1 SYNOPSIS

I<ninpmd.pl> - Run from within B<OPUS>.  This is a monitoring process
within the Input pipeline.  

=head1 DESCRIPTION

This process monitors Data Receipt, restarts if it if is not running, and
sends alerts in the case of problems.

The process runs a shell script B<check_remote_rttm> which checks the status 
of RTTM on the nrtdr machine.  Beginning with a ping test to verify that the
machine is up and then attempting to log on through rsh, it will return a 
a status of 20 or 21 repsectively if one of these fails.  
Once it has logged in, if it finds that the process is not running, it will 
attempt to restart it.  After 10 attempts, if it cannot restart rttm, it
will exit with a status of 22.  If the process is already running or has
been successfully restarted, it will log the status information.

The pipeline process will take the exit status of the B<check_remote_rttm> 
run and decide what to do based on the exact value of that status.  If the
status is zero, then RTTM is running and the monitor quits.  If the status 
is 20, it sends an alert saying that nrtdr did not respond to a ping.  If
the status is 21, it sends an alert that the log on to nrtdr failed.  If
the status is 22, it sends an alert that the rttm process could not be 
restarted.  Another nonzero status will result in a generic unknown alert.

The monitor will in fact not send an alert every time.  It runs every minute,
and an alert every minute until the problem is fixed is overkill.  THerefore,
the monitor keeps track of the last time it sent an alert simply by placing
a file in the workspace whose name is the time of the last alert sent.  The
time between alerts is the DELTA in the resource file, currently 10 minutes.
If fewer than ten minutes have gone by since an alert was sent, it will simply
log the fact that the problem remains.  If more than 10 minutes has gone by,
it will send another alert.  The first time it then finds rttm running
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

=back

=cut

#
# This is the input monitor for data receipt process.  
# The intention is to have it trigger every so often (say once per min?) 
# and have it run.
#

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use TimeLIB;
use ISDCLIB;

&ISDCPipeline::EnvStretch ( "LOG_FILES", "ALERTS", "PARFILES" );

my $delta = $ENV{DELTA}; # delta between multiple alerts in minutes
$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/nrt_dr_monitor.log";

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{LOG_FILES}" ) unless ( -d "$ENV{LOG_FILES}" );

my $time  = &TimeLIB::MyTime ();
my $times = &TimeLIB::MyTimeSec ();
#  call shell script to ping, rsh, check, and restart rttmd
my ( $retval, @result ) = &ISDCPipeline::RunProgram ( "check_remote_rttm" );
print "checking Data Receipt at $time yields:\n@result";

# zero status means running fine or restarted successfully
if ( ! ($retval) ) {
	# remove previous "lock" file if there was one
	my ( $lock ) = sort ( glob ( "$ENV{OPUS_WORK}/nrtinput/scratch/rttm_alert_lock*" ) );
	unlink "$lock" if ($lock);
	exit 0;
}

# nonzero status means couldn't check or couldn't restart
my $alert;
my $alertid;
# possible exit status values from check_remote_rttm (*256)
if ( $retval == 5120 ) {
	$alert = "Ping test failed for nrtdr";
	$alertid = "210";
}
elsif ( $retval == 5376 ) {
	$alert = "RSH test failed for nrtdr";
	$alertid = "211";
}
elsif ( $retval == 5632 ) {
	$alert = "restart of rttm on ntrdr failed after 10 retries";
	$alertid = "200";
}
# unknown
else {
	$alert = "rttm check failed (?)";
	$alertid = "209";
}
# look for "lock" file indicating that an alert has already been sent;
my $lock = glob( "$ENV{OPUS_WORK}/nrtinput/scratch/rttm_alert_lock*" );
if ( $lock ) {
	my $lasttime = $lock;
	$lasttime =~ s/.*rttm_alert_lock_(.*)$/$1/;
	$delta = $delta*60;
	my $dtime = $times;
	if ( $dtime < $lasttime + $delta ) {
		print "Alert already sent less than $ENV{DELTA} minutes ago at $time;  logging only\n\n";
		my ( $retval2, @result2 ) = &ISDCPipeline::RunProgram ( "ril_write LogType=Warning_0 "
			."Message=\"$alert\" Task= ProcedureNumber=0000","quiet" );
		
		exit 0;
	}
	else {
		# remove current lock file (will be replaced below)
		unlink $lock;
		# send new alert since it's at least 10 minutes since last
		print "Sending another alert at $time\n";
	}
}

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ALERTS}" ) unless ( -d "$ENV{ALERTS}" );

&ISDCPipeline::WriteAlert (
	"step"    => "NRT Input Monitor DR - ALERT",
	"level"   => 3,
	"message" => $alert,
	"subdir"  => $ENV{ALERTS},
	"id"      => $alertid,
	);

print "********\nAlert \"$alert\" sent\n********\n"; 
# create new alert lock file
open LOCK, ">$ENV{OPUS_WORK}/nrtinput/scratch/rttm_alert_lock_$times";
close LOCK;

exit 0;


######################################################################

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

