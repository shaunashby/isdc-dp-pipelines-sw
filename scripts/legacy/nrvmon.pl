#!perl

=head1 NAME

nrvmon.pl - NRT Revolution Pipeline Monitor

=head1 SYNOPSIS

I<nrvmon.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

=item

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use OPUSLIB;
use TimeLIB;
use lib "$ENV{ISDC_OPUS}/nrtrev/";
use Archiving;
use File::Basename;

sub ILTcheck;
sub PSDcheck;
sub OSFcheck;
sub ARCcheck;

my @result;
my $retval;
my @output;
my $time = &TimeLIB::MyTime();


print "\n========================================================================\n";

##########################################################################
# machinations to get correct environment variables through path file
##

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","LOG_FILES","REV_INPUT","SCWDIR","SCW_INPUT","ARC_TRIG","ARC_TRIG_INGESTING","ARC_TRIG_DONE");

&ILTcheck;

&PSDcheck;

&OSFcheck;

&ARCcheck;


exit 0;

##########################################################################
#                                DONE
##########################################################################




##########################################################################

=item B<ILTcheck> ( )

=cut

sub ILTcheck {
	print "\n\n\n"
		."#################################################################################\n"
		."##############     CHECKING ILT OSFS\n"
		."#################################################################################\n";
	
	
	###################
	#
	# SPR 3036:  it's not this simple.  Can't use the OSF creation as a reliable
	#   way to "date" the dump.  Instead, must use the ERT stamp of the dump
	#   itself.  (Too bad.  This was nice and simple.)  
	#
	#  #  ILT_WAIT is in minutes, agelimit should be in days:
	#  my $agelimit = $ENV{ILT_WAIT} / ( 60 * 24);
	#
	#  ISDCPipeline::BBUpdate(
	#			 "agelimit" => "$agelimit",
	#			 "type" => "ilt",
	#			 "curstat" => "h",
	#			 "column" => "RV",
	#			 "newstat" => "w",
	#			);
	###################
	#  
	
	#  ILT_WAIT is in minutes.  We need a delta of seconds.
	#   We're going to want to add it to the time stamp on the dump, and with 
	#   UTCops, the default is subtraction, so here we make it negative:
	my $delta = $ENV{ILT_WAIT} * -60;
	print "*******     Delta is $delta\n";
	#  So take the ERT (UTC) time stamp of the dump, add the delta, and then
	#   compare that to now.
	
	
	#  Get a list of all ILTs on BB with hold status:
	my @ilt_osfs = &ISDCPipeline::BBUpdate(
		"return"  => 1,
		"type"    => "ilt",
		"curstat" => "h",
		"column"  => "RV"
		);
	
	#  this is non-leap seconds since Jan1, 1970, UTC, converted into localtime in format YYYYMMDDHHMMSS
	my $now_utc = &TimeLIB::UTCops();  
	print "*******    ".&TimeLIB::MyTime()." - Now is:  $now_utc UTC\n";
	
	my $one_ilt;
	my $one_ilt_utc;
	my $one_ready_utc;
	my ($retval,@result);
	
	#  For each, add delta to it's ERT and see if that's less than now:
	foreach $one_ilt (@ilt_osfs) {
		print "*******     Considering ILT $one_ilt\n";
		$one_ilt =~ /^\d{4}_(\d{14})_\d{2}_ilt$/;
		$one_ilt_utc = $1;
		print "*******     Time stamp $one_ilt_utc UTC\n";
		$one_ready_utc = &TimeLIB::UTCops("utc" => "$one_ilt_utc","delta"=>$delta);
		print "*******     ...  will be ready to process at $one_ready_utc "
			."(which should equal $one_ilt_utc minus delta $delta\n";
		if ($one_ready_utc < $now_utc) {
			
			print "*******     Since $one_ready_utc less than $now_utc, this ILT is ready to process!\n";
			
			($retval,@result) = &ISDCPipeline::RunProgram("osf_update -p nrtrev -f $one_ilt "
				."-s $osf_stati{REV_ST_C}");
			die "*******     ERROR:  couldn't run \'osf_update -p nrtrev -f $one_ilt "
				."-s $osf_stati{REV_ST_C}\':  @result" if ($retval);
			
		} else {
			print "*******     Since $one_ready_utc not less than $now_utc, "
				."this ILT is not ready to process.\n";
		}
		
	} # foreach hold ILT
	
} #  end sub ILTcheck


##########################################################################

=item B<PSDcheck> ( )

=cut

sub PSDcheck {
	
	print "\n\n\n"
		."######################################################################\n"
		."##############     CHECKING SPI PSD OSFS\n"
		."######################################################################\n";
	
	my $psdtype;
	my ($retval,@result);
	my $stamp = &TimeLIB::MyTime();
	$stamp =~ s/-|:|T//g;
	my @osfs;
	my $lastosf;
	my $last_dectime;
	my $last_hextime;
	my $last_status;
	
	# Determine what revolution we're in;  last written good enough:
	# FIXME: This is really lame and adding the 0 to make sure we don't pick up
	# stray files is also a hack. Eventually fix by actually checking that
	# we get directories returned....
	my @revs = `$myls $ENV{REP_BASE_PROD}/scw/0* 2> /dev/null`;
	my $revno = pop @revs;  
	chomp $revno;
	$revno = &File::Basename::basename ( $revno );
	print "*******     Current revolution is $revno\n";
	
	foreach $psdtype ("spa","spp","spe","sps") {  
		print "\n"
			."**********************************************************************\n"
			."*******     Checking SPI PSD OSFs of type $psdtype\n"
			."**********************************************************************\n";
		
		#  Get last PSD dataset 
		#  This gives a hash of dataset and their status
		my %osfs = &ISDCPipeline::BBUpdate(
			"type"   => "$psdtype",
			"return" => 2,
			);
		#  Fixes SPR 2728.  Why doesn't sorting when writing the hash
		#   do it?
		@osfs = sort keys %osfs;
		if (%osfs) {
			#      print "*******     DEBUG:  got:\n".join('', @osfs)."\n";
			$lastosf = pop @osfs;  
			print "*******     Last OSF is $lastosf with status $osfs{$lastosf}.\n";
			#  Get time of last PSD OSF
			$last_hextime = `osf_test -p nrtrev.path -f $lastosf`;
			#	051129 - Jake - SPR 4378 - osf_test does not give a return value ($?)
			#	Its 0 unless syntax is completely wrong (if $lastosf is "")
			die "*******     ERROR:  cannot \'osf_test -p nrtrev.path -f $lastosf\'" if ($?);
			chomp $last_hextime;
			$last_hextime =~ s/.*(\w{8})-(\w{6}).*\.(\w{23}).*/$1/;
			
			die "*******     ERROR:  cannot parse last $psdtype type PSD $last_hextime!" 
				unless ($last_hextime =~ /^\w{8}$/);
			
			print "*******     Last $psdtype type PSD OSF was at hex time $last_hextime\n";
			$last_dectime = hex($last_hextime);
			print "*******     Last $psdtype type PSD OSF was at decimal time $last_dectime\n";
		} # if previous SPI PSD triggers of this type
		else {
			print "*******     WARNING:  no previous PSD OSFs found;  checking for unprocessed triggers.\n";
			@osfs = sort(glob("$ENV{REV_INPUT}/*_$psdtype.trigger*"));
			# double-check since glob may return one empty string
			if ((@osfs) && (-e $osfs[$#osfs])) {
				print "*******     Found $osfs[$#osfs]\n";
				print "*******     OSF not created, but trigger there;  not triggering new analysis.\n";
				next;
			}
			else {
				print "*******     WARNING:  no triggers found at $ENV{REV_INPUT}/*_$psdtype.trigger*\n";
			}
			print "*******     WARNING:  no previous PSD analysis found for type $psdtype;  setting last time to 0\n";
			$last_dectime = 0;
			$lastosf = "";
			
		} # end if no OSFs exist
		
		
		#  Now, see if it's time to trigger another one:
		
		my $now = time;    
		# Use the type to get the right DELTA:
		my $var = $psdtype;
		if ($var =~ s/spa/SPI_PSD_ADC_DELTA/) {}
		elsif ($var  =~ s/spp/SPI_PSD_PERF_DELTA/) {}
		elsif ($var  =~ s/spe/SPI_PSD_EFFI_DELTA/) {}
		elsif ($var  =~ s/sps/SPI_PSD_SI_DELTA/) {}
		else {
			die "*******     ERROR:  don't recognize SPI PSD type $var!";
		}
		
		print "*******     Comparing $psdtype type OSF dec time $last_dectime to now ($now)\n";
		
		my $diff = $now - $last_dectime;  
		if ($diff >= $ENV{$var}) {
			print "******     Elapsed time $diff greater than $var==$ENV{$var};  triggering analysis\n";
			
			if (scalar keys %osfs) {
				#  Get last PSD dataset status and see if we really need a new trigger:
				$last_status = $osfs{$lastosf};
				if ($last_status =~ /h/) {
					print "*******     Last OSF $lastosf is on hold;  resetting to wait.\n";
					
					($retval,@result) = &ISDCPipeline::RunProgram("osf_update -p nrtrev.path "
						."-f $lastosf -s $osf_stati{REV_ST_C}");
					
					die "*******     ERROR:  cannot \'osf_update -p nrtrev.path -f $lastosf "
						."-s $osf_stati{REV_ST_C}\':  @result" if ($retval);
					next;
				}
				elsif ( ($last_status =~ /x/) && ($lastosf =~ /^$revno/) ) {
					#  If there's an error, then it depends what revolution we are.
					#   No gain to try again the same rev, but next one may work.
					print "*******     Last OSF status was $last_status;  not creating new OSF.\n";
					next;
				}  # if error
				elsif ($last_status =~ /x/) {
					print "*******     Last OSF status was $last_status but from different revolution;  still creating new OSF.\n";
				}  # if error
				elsif ($last_status =~ /cw/) { # Covers cww or ccw
					print "*******     Last OSF status was $last_status;  not creating new OSF.\n";
					next;	
				} # if any part still waiting
				elsif ($last_status =~ /c$/) {
					print "*******     Last OSF status was $last_status;  creating new OSF.\n";
					#  Only here, contine within this loop and create trigger.
				}
				else {
					die "*******     Don't recognize status $last_status!";
				}
			} # end if previous PSD OSFs found somewhere
			
			#  Lastly, check that revolution hasn't already been write protected:
			if (-w "$ENV{REP_BASE_PROD}/scw/$revno") {
				print "*******     Revolution $revno writeable.\n"; 
			}
			else {
				print "*******     Revolution $revno NOT writeable;  no triggering.\n"; 
				next;
			}
			
			($retval,@result) = &ISDCPipeline::RunProgram("$mytouch $ENV{REV_INPUT}/${revno}_${stamp}_00_$psdtype.trigger"); 
			
			die "*******     ERROR:  cannot \'$mytouch $ENV{REV_INPUT}/${revno}_${stamp}_00_$psdtype.trigger\':  @result" if ($retval); 
			
		} 
		else {
			print  "******     Elapsed time $diff less than $var==$ENV{$var};  no analysis yet.  Quitting.\n";
			next;
		}
		
	} # foreach type 
	
	return;
	
}  # end subPSDcheck


##########################################################################

=item B<OSFcheck> ( )

These functions are designed to do this by default.  Only must specify
different behavior for errors and completed datasets.  Can define
more, obviously.  If you do,  don't forget to restrict the existing
ones.  
	
=cut

sub OSFcheck {
	
	print "\n\n\n"
		."#################################################################################\n"
		."###########       Checking for old OSFs to be cleaned\n"
		."#################################################################################\n";
	
	#  OSF_AGELIMIT's are in days
	&ISDCPipeline::BBUpdate(
		"agelimit"  => "$ENV{OSF_AGELIMIT_DEFAULT}",
		"matchstat" => "$osf_stati{REV_COMPLETE}",
		"exclude"   => "arc_prep",
		);
	
	&ISDCPipeline::BBUpdate(
		"agelimit" => "$ENV{OSF_AGELIMIT_ERRORS}",
		"errors"   => 1,
		);
	
	&ISDCPipeline::BBUpdate(
		"agelimit"  => "$ENV{OSF_AGELIMIT_ARCHIVED}",
		"matchstat" => "$osf_stati{REV_COMPLETE}",
		"match"     => "arc_prep",
		);
	
}  # end sub OSFcheck


##########################################################################

=item B<ARCcheck> ( )

=cut

sub ARCcheck {
	
	my @revnos;
	
	#  Get a hash of all OSFs on the blackboard and their status:
	my %osf_status = &ISDCPipeline::BBUpdate("return" => 2);
	my %revnos;
	my @archived;
	my ($revno,$root);
	my $arc_dir;
	
	#  Go through OSFs and get list of revnos for which there is no arc_prep
	#   trigger:
	foreach (keys %osf_status) {
		#    print ">>>>>>>     DEBUG:  got OSF $_\n";
		/^(\d{4})_(.*)$/;
		$revno = $1;
		$root = $2;
		$revnos{$revno}++;
	} # end foreach keys in osf_status
	#  Now, go through revnos and delete those for which an arc_prep
	#   trigger already exists:
	foreach (keys %revnos) {
		if (defined ($osf_status{"${_}_arc_prep"}) ) {
			print ">>>>>>>     DEBUG:  got revno $_ arc_prep trigger;  deleting from list.\n";
			delete $revnos{$_};
		}
		else {
			print ">>>>>>>     DEBUG:  got revno $_ to check\n";
		}
	}
	
	#  Now, for each revno which hasn't yet been triggered for archiving,
	#   see if it's ready:
	REVNO:
	foreach $revno (keys %revnos) {
		
		# Double check that there are no triggers in the Arc system, which
		#  could happen if the arc_prep trigger is deleted before the errors
		
		foreach $arc_dir ("$ENV{ARC_TRIG}","$ENV{ARC_TRIG_INGESTING}","$ENV{ARC_TRIG_DONE}") {
			
			if (`$myls $arc_dir/scw_${revno}rev0000.trigger* 2> /dev/null`) {
				print ">>>>>>>     WARNING:  found rev $revno trigger under $arc_dir;  "
					."strange that the ${revno}_arc_prep trigger is gone already, but this "
					."could be because there are still errors on the BB.  Skipping.\n";
				next REVNO;
			}
			else {
				print ">>>>>>>     DEBUG:  Didn't find rev $revno trigger under $arc_dir;  continuing.\n";
			}
		} # foreach arc_dir
		
		&Archiving::CheckRev("$revno");
		
	} # end foreach keys revnos
	
} # end sub ARCcheck
##########################################################################






__END__ 

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level
Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

