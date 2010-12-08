#!/usr/bin/perl

=head1 NAME

I<cssfin.pl> - consssa FIN step script

=head1 SYNOPSIS

I<cssfin.pl> - Run from within B<OPUS>.  This is the third, and obviously final, step in the consssa pipeline.

=head1 DESCRIPTION

Basic cleanup, write-protection and the creation of the ingest trigger

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use OPUSLIB qw(:osf_stati);
use CorLIB;
use SSALIB;

my ($retval,@result);

print "\n========================================================================\n";
print "*******     Trigger $ENV{OSF_DATASET} received\n";

&ISDCPipeline::EnvStretch("LOG_FILES","OUTPATH","WORKDIR","ARC_TRIG","INPUT");

my ( $scwid, $revno, $og, $inst, $INST, $instdir, $OG_DATAID, $OBSDIR ) = &SSALIB::ParseOSF;
my $proc = &ProcStep." $INST";

&Message ( "$proc - STARTING" );

$ENV{PARFILES} = "$ENV{OPUS_WORK}/consssa/scratch/$ENV{OSF_DATASET}/pfiles";

&ISDCLIB::DoOrDie ( "mkdir -p $ENV{PARFILES}" ) unless ( -e $ENV{PARFILES} );

print "*******     Scw is $scwid;  Instrument is $INST;  group is ${og}.\n";
      
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#     
#              Check that this should be done.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

if ( ( $ENV{REDO_CORRECTION} ) && ( $INST=~/SPI|OMC|PICSIT/ ) ) {
	&Message ( "$proc - Not ReRunning Correction for $INST $ENV{OSF_DATASET}.\n" );
}

####################################################################################################
#
#	REDO_CORRECTION manipulation
#
####################################################################################################

if ( ( $ENV{REDO_CORRECTION} ) && ( $INST=~/ISGRI|JMX/ ) ) {

	$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";

	&CorLIB::Fin (
		"revno"     => "$revno",
		"scwid"     => "$scwid",
		"proc"      => "$proc",
		"OBSDIR"    => "$OBSDIR",
		"OG_DATAID" => "$OG_DATAID",
		"INST"      => "$INST",
		);
}



####################################################################################################
#
#	The rest of the normal fin step
#
####################################################################################################

unless ( $ENV{REDO_CORRECTION} ) {
	
	#	It is simpler to gzip everything and then unzip the main files.
	#	Added a cd before the find to shorten the argument given to gzip.
	#	It seemed quite long and a potential problem if too long.
	my $fitslist = `cd $OBSDIR/.; find . -name \\\*fits`;
	&Error ( "Nothing found in $OBSDIR!?!" ) unless ( $fitslist );
	
	$fitslist =~ s/\n/ /g;
	
	#	These next 3 lines make the gunzip unnecessary.
	$fitslist =~ s/(\.\/scw\/[\d\.]{16}\/swg_[\w\d]{3,6}.fits)//g;
	$fitslist =~ s/(\.\/og_[\w\d]{3,6}.fits)//;
	$fitslist =~ s/(\.\/swg_idx_[\w\d]{3,6}.fits)//;
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - compress all data",
		"program_name" => "$mygzip $fitslist",
		"subdir"       => "$OBSDIR",
		) if ( $fitslist );

	#  Move alerts into logs subdirectory
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - remove alerts",
		"program_name" => "$myrm *alert*",
		"subdir"       => "$OBSDIR",
		) if (`$myls $OBSDIR/*alert* 2> /dev/null`);

	&Error ( "*******  ERROR:  Alerts still exist!" )
		if (`$myls $OBSDIR/*alert* 2> /dev/null`);
}


####################################################################################################
#
#  Trigger cleanup
#
####################################################################################################

&Message ( "Looking for Trigger +$ENV{INPUT}/${scwid}_${inst}.trigger\*+" );
chomp ( my @trigger = `$myls $ENV{INPUT}/${scwid}_${inst}.trigger*` );
&Message ( "Found Trigger +$trigger[0]+" );

if ( @trigger ) {
	my $i;
	foreach ( @trigger ) {	#	multiple trigger files should never happen, but now it can deal with it if someone manually intervenes

		#	What if the trigger that does exist is trigger_done?
		#	perhaps should skip it here
		next if ( /_done/ );

		my ( $trigger_done ) = ( /^(.+${scwid}_${inst}\.trigger).*$/ );
		&Error ( "No trigger match of +$_+ in regular expression /${scwid}_${inst}.trigger/." ) unless ( "$trigger_done" );
		$trigger_done .= "_done"."$i";
		&Message ( "Done Trigger +$trigger_done+" );
		( $retval, @result ) = &ISDCPipeline::RunProgram ( "$mymv $_ $trigger_done" );
		&Error ( "Cannot move trigger file $_ to done:\n@result\n" ) if ($retval);
		$i++;
	}
} else {
	&Message ( "Cannot find trigger file +$ENV{INPUT}/${scwid}_${inst}.trigger\*+ as expected." );
}


unless ( $ENV{REDO_CORRECTION} ) {

	####################################################################################################
	#
	#  Write protection
	#
	####################################################################################################

	&Message ( "$proc - write protecting obs dir (Can no longer use ISDCPipeline::Log\*" );
	($retval,@result) = &ISDCPipeline::RunProgram("$mychmod -R -w $OBSDIR/");
	die "*******  ERROR:  cannot write protect $OBSDIR:\n@result" if ($retval);

	####################################################################################################
	#
	#	Begin Archive Triggering
	#
	####################################################################################################

	unless ( -d "$ENV{ARC_TRIG}" ) {
		&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ARC_TRIG}" );
		die "*******     ERROR:  didn't create archive trigger directory $ENV{ARC_TRIG}" 
			unless ( -d "$ENV{ARC_TRIG}" );
	}

	my $ingest_trigger = "$ENV{ARC_TRIG}/css_${OG_DATAID}0000.trigger";
	
	open(AIT,">${ingest_trigger}_temp") or 
		die "*******     ERROR:  cannot open trigger file ${ingest_trigger}_temp";
	print AIT "$ingest_trigger CSS $OBSDIR\n";
	close(AIT);
	
	($retval,@result) = &ISDCPipeline::RunProgram (
	        "$mymv ${ingest_trigger}_temp $ingest_trigger");
	die "******     ERROR:  Cannot make trigger $ingest_trigger:\n@result" if ($retval);
	die "******     ERROR:  Trigger $ingest_trigger does not exist!" 
		unless ( -e $ingest_trigger );

	####################################################################################################
	#
	#	End Archive Triggering
	#
	####################################################################################################

}

&ISDCLIB::DoOrDie ( "$myrm -rf $ENV{PARFILES}" ) if ( -e "$ENV{PARFILES}" );

exit;

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

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
