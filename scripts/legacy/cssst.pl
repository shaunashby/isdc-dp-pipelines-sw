#!perl

=head1 NAME

I<cssst.pl> - consssa ST step script

=head1 SYNOPSIS

I<cssst.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

=over

=cut

use strict;
use ISDCPipeline;
use UnixLIB;
use OPUSLIB;
use ISDCLIB;
use SSALIB;

print "\n========================================================================\n";
print "*******     Trigger $ENV{EVENT_NAME} received\n";


&ISDCPipeline::EnvStretch("LOG_FILES","OUTPATH","PARFILES","WORKDIR","SCWDIR");

#	Use the full $ENV{EVENT_NAME} because the function needs to determine if the file is zero size
my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $ENV{EVENT_NAME} );
print ( "osfname:$osfname\n" ) if ( "$ENV{DEBUGIN}" );

my $retval;

print "*******     Triggering CSS SA/ScW processing for $osfname;  creating OG and OSF.\n";

print "\n*******************************************************************\n";
print "*******     INSTRUMENT $INST:\n";
print "*******************************************************************\n";

$ENV{OSF_DATASET} = $osfname; #	THIS LINE MUST BE HERE as this environment variable is never set in the first step (before the OSF is created)
my $proc = &ProcStep()." $INST";


=item Create OSF

=cut


if ( ( $ENV{REDO_CORRECTION} ) && ( $INST=~/SPI|OMC|PICSIT/ ) ) {
	&Message ( "$proc - Not ReRunning Correction for $INST $ENV{OSF_DATASET}.\n" );
} else {

#	my ( $scwid, $revno, $og, $inst, $INST, $instdir, $OG_DATAID, $OBSDIR ) = &SSALIB::ParseOSF;
#	&ISDCLIB::DoOrDie ( "$mymkdir -p $OBSDIR" ) unless ( -d "$OBSDIR" );
#	&Error ( "Did not mkdir $OBSDIR" ) unless ( -d "$OBSDIR" );

	# Startup OSF for observation, with status cww 
	$retval = &ISDCPipeline::PipelineStart (
		"dataset"     => "$ENV{OSF_DATASET}", 
		"state"       => "$osf_stati{SSA_ST_C}",  
		"type"        => "scw", 
		"dcf"         => "$dcf", 
		"logfile"     => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log", 
		"reallogfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}_css.txt", 
		);
}
	
&Error ( "Cannot start pipeline for $ENV{OSF_DATASET}\n" ) if ($retval);
	
exit 0;

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

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut


#	last line
