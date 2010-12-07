#!perl

=head1 NAME

I<nqlst.pl> - nrtqla pipeline ST step script

=head1 SYNOPSIS

I<nqlst.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

This script creates the appropriate OSFs and logs via PipelineStart.

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use OPUSLIB;
use QLALIB;

print "\n========================================================================\n";
print "*******     Trigger $ENV{EVENT_NAME} received\n";

#######################

&ISDCPipeline::EnvStretch ( "LOG_FILES","OUTPATH","OBSDIR","PARFILES","WORKDIR","SCWDIR" );

my ( $dataset, $path, $suffix ) = &File::Basename::fileparse ( $ENV{EVENT_NAME}, '\..*' );

if ( ! ( $ENV{RUN_SLEWS} ) && ( $dataset !~ /0$/ ) ) {
	print "*******     Dataset $dataset not a pointing and RUN_SLEWS not set.  Quitting.\n";
	exit 0;
}

#  Control which OSFs and OGs to create via nqlst.resource file entries:
my @insts;
push @insts, "ibis" if ( $ENV{RUN_IBIS_SCW} );
push @insts, "spi"  if ( $ENV{RUN_SPI_SCW}  );
push @insts, "jmx1" if ( $ENV{RUN_JMX1_SCW} );
push @insts, "jmx2" if ( $ENV{RUN_JMX2_SCW} );
push @insts, "omc"  if ( $ENV{RUN_OMC_SCW}  );

my $revno = &ISDCPipeline::RevNo ( $dataset );

print "*******     Triggering QLA ScW processing for $dataset;  creating OG and OSFs.\n";

foreach my $inst ( @insts ) {
	print "\n*******************************************************************\n";
	print "*******     INSTRUMENT $inst:\n";
	print "*******************************************************************\n";
	
	# The DCF is only three, and we need the JEMX number.  So the DCFs will be
	#   IBI, SPI, OMC, JX1, and JX2
	my $dcf = $inst;
	$dcf =~ tr/a-z/A-Z/; 
	$dcf =~ s/JM/J/; 
	
	my $in = &ISDCLIB::inst2in ( $inst );
	$ENV{OSF_DATASET} = "qs${in}_${dataset}"; 
	
	mkdir $ENV{OBSDIR},0755 unless ( -d $ENV{OBSDIR} );

	# Startup OSF for observation, with status cww 
	#
	my $retval = &ISDCPipeline::PipelineStart (
		"dataset"     => "$ENV{OSF_DATASET}", 
		"state"       => "$osf_stati{QLA_ST_C}",  
		"type"        => "scw", 
		"dcf"         => "$dcf", 
		"logfile"     => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log", 
		"reallogfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}_qla.txt", 
		);			       
	
	die "*******     ERROR:  cannot start pipeline for $ENV{OSF_DATASET}" if ( $retval );
	
} # foreach insts

exit 0;

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
