#!perl

=head1 NAME

csaobs.pl - conssa OBS (1 and 2) step script

=head1 SYNOPSIS

csaobs.pl - Run from within OPUS.  This script is called during the third (and fifth for IBIS) step(s) of processing in the conssa pipeline.

=head1 DESCRIPTION

Handles the cleanup and group _science_analysis

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use OPUSLIB;
use UnixLIB;
use SPILIB;
use OMCLIB;
use IBISLIB;
use JMXLIB;
use SATools;

print "\n========================================================================\n";

&ISDCPipeline::EnvStretch("LOG_FILES","OUTPATH","WORKDIR","OBSDIR","PARFILES","IC_ALIAS");

$ENV{OSF_DATASET} =~ /(\w+)_(IBIS|SPI|JMX1|JMX2|OMC)$/;
my $ogid = $1;
my $INST = $2;
my $return = 0;
my $loop = ( $ENV{PROCESS_NAME} =~ /csaob1/ ) ? 1 : 2;
my $proc = &ProcStep();

$proc   .= " $INST";
$proc   .= " loop $loop" if ($INST =~ /IBIS/);

print "*******     Processing Ogid $ogid for $proc\n";

chdir("$ENV{OBSDIR}/$ogid.000/") or die "Cannot chdir to $ENV{OBSDIR}/$ogid.000/";
print "*******     Current directory is $ENV{OBSDIR}/$ogid.000/\n";

&Message ( "STARTING" );
$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
$ENV{PFILES} = "$ENV{PARFILES};$ENV{ISDC_ENV}/pfiles";

if ( $ENV{PROCESS_NAME} =~ /csaob1/ ) {
	&Message ( "Checking og for previous run." );
	#  SPR 4464 - cleanup in case of re-run
	my @list = glob ( "*" );
	my @needCleaned;
	foreach my $file ( @list ) {
		next if ( ( $file =~ /og_.+\.fits/ )
			|| ( $file =~ /swg_idx_.+\.fits/ )
			|| ( $file =~ /rebinned_corr_ima.fits/ )
			|| ( $file =~ /energy_bands.fits.gz/ )
			|| ( $file =~ /scw/ )
			|| ( $file =~ /logs/ ) );
		push @needCleaned, $file;
	}
	if ( @needCleaned ) {
		&Message ( "Found extra files.  Cleaning..." );
		&ISDCLIB::QuickClean    ( @needCleaned );
		&ISDCLIB::QuickDalClean ( glob ( "og_*fits" ) );		#	PFILES aren't set yet, so this creates a dal_clean.par in the obs dir.
	}
}

SWITCH:  {
	
	if (($INST =~ /IBIS/) &&  ($loop == 1)) {
		&IBISLIB::ISA (
			"proctype" => "mosaic",		#	needed bc of scw count check
			"INST"     => "$INST",		#	not really used, but keeps a warning from appearing
			);

		$return = 5;
		&SATools::ScwSetWait();
	}
	
	if ($INST =~ /SPI/) {
		&SPILIB::SSA (
			"proctype" => "mosaic",
			);
	}
	if ($INST =~ /JMX(\d)/) {
		my ( $jemxnum ) = $1;	#	( $INST =~ /JMX(\d)/ );
		&JMXLIB::JSA (
			"proctype" => "mosaic",
			"jemxnum"  => "$jemxnum",
			);
	}
	if ($INST =~ /OMC/) {
		&OMCLIB::OSA(
			"proctype" => "mosaic",
			);
	}
	
} # end of SWITCH

exit $return;


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
