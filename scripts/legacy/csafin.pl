#!perl -w

=head1 NAME

I<csafin.pl> - conssa FIN step script

=head1 SYNOPSIS

I<csafin.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

Handles the write-protection, gzipping, indexing, trigger modification, etc.

=cut

use strict;
use ISDCPipeline;
use ISDCLIB;
use OPUSLIB;
use UnixLIB;

use lib "$ENV{ISDC_OPUS}/conssa/";
use SATools;

print "\n========================================================================\n";

&ISDCPipeline::EnvStretch ( "LOG_FILES", "OUTPATH", "WORKDIR", "OBSDIR", "PARFILES" );

$ENV{OSF_DATASET} =~ /(\S+)_(IBIS|JMX\d|OMC|SPI)/;
my $ogid = $1;
my $inst = $2;
my $ins  = $ENV{OSF_DCF_NUM};
#my $proc = &ISDCLIB::Initialize()." $inst";
my $proc = &ProcStep()." $inst";
my $return = 0;

######################################################################
##
##   Two possibilities:  got OG and just finish it,
##    or got SwG and must check status of others and maybe reset others.
##
######################################################################

#########
#
#  Observation group
#
#########
if ( $ENV{OSF_DATA_ID} =~ /obs/ ) {
	
	&ISDCPipeline::PipelineFinish ();
	
	#  Clean Blackboard for this Obs:
	&ISDCPipeline::BBUpdate (
		"match" => "^$ENV{OSF_DATASET}_",
		"type"  => "scw",
		);
	
	#  Check that other Instruments for this OG are done:
	my ($retval) = &SATools::ObsCheck (
		"ogid" => "$ogid", 
		"proc" => "$proc"
		);
	
	if ($retval) {
		print "*******     OG $ogid not yet finished;  there remain $retval OSFs to process.\n"
			."*******     No write protection yet.\n"
	}
	else {
		
		print "*******     OG $ogid looks done;  now compress, write protect, and index.\n";
		
		chdir "$ENV{REP_BASE_PROD}/obs/$ogid.000/" 
			or die ">>>>>>>     ERROR:  cannot chdir to $ENV{REP_BASE_PROD}/obs/$ogid.000/";
		
	#	It is simpler to gzip everything and then unzip the main files.
	#	Added a cd before the find to shorten the argument given to gzip.
	#	It seemed quite long and a potential problem if too long.
#	my $fitslist = `$myfind . -name \\\*fits`;
#	$fitslist =~ s/\n/ /g;
#	&UnixLIB::Gzip ( "$fitslist" );

		&UnixLIB::Gzip ( "*.fits", "scw/*/*.fits" );

		&UnixLIB::Gunzip ( "og_*.fits.gz", "swg_idx_*.fits.gz", "scw/*/swg_*.fits.gz" );

		&ISDCPipeline::MakeIndex (
			"root"      => "GNRL-OBSG-GRP-IDX",
			"subdir"    => "$ENV{OUTPATH}/idx/obs",
			"add"       => "1",
			"osfname"   => "$ENV{OSF_DATASET}",
			"filematch" => "../../obs/$ogid.000/og_*.fits",
			"ext"       => "[GROUPING]",
			"template"  => "GNRL-OBSG-GRP-IDX.tpl",
			);
		
		&ISDCPipeline::LinkUpdate (
			"root"    => "GNRL-OBSG-GRP-IDX",
			"ext"     => ".fits",
			"subdir"  => "$ENV{REP_BASE_PROD}/idx/obs",
			"logfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log",
			);
		
		my $core = `$myls $ENV{REP_BASE_PROD}/obs/$ogid.000/core 2> /dev/null`;
		if ( $core ) {
			print "\n*******     WARNING:  removing core file\n*******  $core\n*******\n";
			`$myrm -f $core`;   # unlink "$core"; # unlink wasn't working ???
		}
		
		&Message ( "$proc - write protect" );
		
		##  Now recursively write protect, and hereafter log  only to process log
		my  ($retval,@result) = &ISDCPipeline::RunProgram ( "$mychmod -R -w $ENV{OBSDIR}/$ogid.000/" );
		
		die "*******  ERROR:  cannot write protect $ENV{OBSDIR}/$ogid.000/:\n@result" if ($retval);
		
		
		( $retval, @result ) = &ISDCPipeline::RunProgram (
			"$mymv $ENV{INPUT}/$ogid.trigger_processing $ENV{INPUT}/$ogid.trigger_done"
			) if (-e "$ENV{INPUT}/$ogid.trigger_processing"); 
		die "******     ERROR:  Cannot move trigger file $ENV{INPUT}/$ogid.trigger_processing to done:\n@result" 
			if ($retval);
		
		# if it had an error during processing, was fixed and reset by hand,
		#  then this needs to find the "_bad" trigger file instead.  
		( $retval, @result ) = &ISDCPipeline::RunProgram (
			"$mymv $ENV{INPUT}/$ogid.trigger_bad $ENV{INPUT}/$ogid.trigger_done"
			) if (-e "$ENV{INPUT}/$ogid.trigger_bad"); 
		die "******     ERROR:  Cannot move trigger file $ENV{INPUT}/$ogid.trigger_bad to done:\n@result" 
			if ($retval);
		
		print "*******     DONE\n";
	} # if not retval from ObsCheck
	
}  # if obs type



#########
#
#  Science Window group:
#
#########
elsif ($ENV{OSF_DATA_ID} =~ /scw/) {
	
	##  Case two:  SwG, so check all SwGs associated
	$ENV{OSF_DATASET} =~ /_(\d{12})$/;
	my $scwid = $1;
	print "*******     Current science window is $scwid\n";
	my $obs_wait;
	my $obs_current_state = "-";
	
	#  Get current status of current scwid;  this is what we then check for.
	my $osf = `$myls $ENV{OPUS_WORK}/conssa/obs/*$ENV{OSF_DATASET}* 2> /dev/null`;
	die ">>>>>>>     ERROR:  can't find my own OSF $ENV{OPUS_WORK}/conssa/obs/*$ENV{OSF_DATASET}*!  "
		."Something's wrong.\n" if ($?);
	chomp $osf;
	my ($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF ( "$osf" );
	my $good_status = $osfstatus;
	# Want status of middle column:
	$good_status =~ s/\w(\w)\w/$1/;
	print "*******     Current status of SA is $good_status\n";
	#  Two possible status values:  
	#     -  c means complete (loop II for IBIS, only one loop others); 
	#     -  o means complete for loop I for IBIS only.
	
	if ($good_status =~ /c/) {
		# In this case, check others and maybe set OG to waiting:
		#  OG waiting is w for all but IBIS;  for IBIS, if the ScW is c, then
		#  the second loop just finished, and it's the second wait for IBIS,i.e. v
		$obs_wait = "w" unless ($inst =~ /IBIS/);
		if ($inst =~ /IBIS/) {
			$obs_wait = "v";
			$obs_current_state = "s";
		}
	}
	elsif ($good_status =~ /g/) {
		#  In this case, it's only IBIS;  if the scw's were "g", then the OG hasn't
		#   run once yet and is ready for its first loop, i.e. w.
		$obs_wait = "w";
		$return = 5; # to set this scw FI step to "o" also.
	}
	else {
		die "*******     ERROR:  good_status of $good_status not among expected values c or o!";
	}
	
	# Checks science windows;  if all scw_complete, sets obs to obs_wait.
	#   also sets any other science windows whose finish hasn't run yet
	#   to scw_complete as well.
	&SATools::ScwCheck(
		"ogid"               => "${ogid}_${inst}",
		"dcf"                => "$ENV{OSF_DCF_NUM}",
		"scwid"              => "$scwid",
		"proc"               => "$proc",
		"scw_complete"       => "$good_status",
		"obs_wait"           => "$obs_wait",
		"ogid_current_state" => "$obs_current_state",
		);
	
} #  if scw type
else {
	
	die "*******     ERROR:  don't recognize OSF_DATA_ID $ENV{OSF_DATA_ID}!";
}


exit $return;




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

