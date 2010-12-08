#!/usr/bin/perl

=head1 NAME

I<csascw.pl> - conssa SCW (1 and 2) step script

=head1 SYNOPSIS

I<csascw.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

Simple cleanup and calls to the appropriate _science_analysis

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use OPUSLIB qw(:osf_stati);
use UnixLIB;
use SPILIB;
use OMCLIB;
use IBISLIB;
use JMXLIB;

print "\n========================================================================\n";

&ISDCPipeline::EnvStretch ( "LOG_FILES", "OUTPATH", "WORKDIR", "OBSDIR", "PARFILES", "IC_ALIAS" );

$ENV{OSF_DATASET} =~ /^(\w+)_(IBIS|JMX1|JMX2|SPI|OMC)_(\d{12})$/;
my $ogid  = $1;
my $INST  = $2;
my $scwid = $3;
my $loop  = ( $ENV{PROCESS_NAME} =~ /csasw1/ ) ? 1 : 2;
my $proc  = &ProcStep();

$proc    .= " $INST";
$proc    .= " loop $loop" if ( $INST =~ /IBIS/ );

#########              Start log of stage 
&Message ( "STARTING" );
$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
$ENV{PFILES} = "$ENV{PARFILES};$ENV{ISDC_ENV}/pfiles";

print "*******     Processing OGID $ogid and ScwID $scwid for $proc\n";

chdir( "$ENV{OBSDIR}/$ogid.000" ) or die "Cannot chdir to $ENV{OBSDIR}/$ogid.000";


if ( $ENV{PROCESS_NAME} =~ /csasw1/ ) {
	&Message ( "Checking science window for previous run." );
	#	SPR 4464 - cleanup in case of re-run
	my @list = glob ( "scw/$scwid.*/*" );
	my @needCleaned;
	foreach my $file ( @list ) {
		next if ( $file =~ /swg_.+\.fits/ );
		push @needCleaned, $file;
	}
	if ( @needCleaned ) {
		&Message ( "Found extra files.  Cleaning..." );
		&ISDCLIB::QuickClean    ( @needCleaned );
		&ISDCLIB::QuickDalClean ( glob ( "scw/$scwid.*/swg_*fits" ) );	#	PFILES isn't set yet, so this creates a dal_clean.par in the scw's dir.
	}
}

#########              Ititialize some defaults that only change for
#########               IBIS looping.  
my $return = 0;

SWITCH:  {
	
	#  IBIS does a figure eight:  process science windows, then obs group,
	#   then science windows again, then obs group again.  So need extra 
	#   status values for loops.
	if ( ( $INST =~ /IBIS/ ) && ( $loop == 1 ) ) {
		&IBISLIB::ISA (
			"proctype" => "mosaic",	#	I think that this is best
			"INST"     => "$INST",	#	just to avoid the warning message
			"scwid"    => "$scwid",  
			);
		#  When first loop done, don't set to c but to o.  
		$return = 5;
	}
	if ( ( $INST =~ /IBIS/ ) && ( $loop == 2 ) ) {

		&IBISLIB::ISA (
			"proctype" => "mosaic",	#	I think that this is best
			"INST"     => "$INST",	#	just to avoid the warning message
			"scwid"    => "$scwid",  
			);
		#  When second loop is done, can exit normally, setting science window
		#   to c.  So needn't change return value here.
	}
	
	if ( $INST =~ /JMX(\d)/ ) {
		my ( $jemxnum ) = $1;	#	( $INST =~ /JMX(\d)/ );
		&JMXLIB::JSA (
			"jemxnum"  => "$jemxnum",
			"proctype" => "mosaic",	#	I think that this is best
			"scwid"    => "$scwid",  
			);
	}
	if ( $INST =~ /OMC/ ) {
		&OMCLIB::OSA (
			"proctype" => "mosaic",	#	I think that this is best
			"INST"     => "$INST",	#	just to avoid the warning message
			"scwid"    => "$scwid",  
			);
	}
	
} # end of SWITCH

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

#	last line
