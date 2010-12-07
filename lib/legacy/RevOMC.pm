package RevOMC;

=head1 NAME

RevOMC.pm - NRT Revolution File Pipeline OMC Module

=head1 SYNOPSIS

use I<RevOMC.pm>;
Run from within B<OPUS>.  This module is called by the scripts of the NRT Revolution File Pipeline.   

=head1 DESCRIPTION

This Module contains functions for each of thee processing steps (DP, ICA, and ACA) for each of the different file types for this instrument which are written into the revolution file directory, i.e. REP_BASE_PROD/scw/RRRR/rev.000/raw.

All functions must be called with an array of paramters as follows:

=over 5

=item proc 

The name of the processing step calling the function, e.g. "NRT DP".

=item stamp

The time stamp of the dataset in format YYYYMMDDHHMMSS.  

=item workdir

The working directory, e.g. OPUS_WORK/nrtrev/scratch/RRRR_YYYYMMDDHHMMSS_irv/

=item osfname

The name of the OSF.  

=item dataset

The file name corresponding to the OSF.

=item type

The type of the dataset, e.g. "irv".

=item revno

The revolution number the dataset belongs to. 

=item prevrev

The previous revolution number.

=item nexrev

The next revolution number.

=back 

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;

$| = 1;

##########################################################################

=item B<DPomc> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for all omc cal 

OMC revolution files are either "bias", "dark", "flatfield", or "sky".  For 
all types, the executalbe B<o_prp_cal_obt> is called to calculate the
on board time and set it in the raw input file.  

=cut

sub DPomc {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $structure = $dataset;
	$structure =~ s/^.*omc_raw_(.*)_\d{14}_\d{2}\.fits$/$1/;
	$structure =~ tr/a-z/A-Z/;
	$structure =~ s/FLATFIELD/LEDF/;
	$structure =~ s/SKY/SKYF/;
	$structure = "OMC.-".$structure."-CRW";
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_cal_obt",
		"program_name" => "o_prp_cal_obt",
		"par_crw"      => "raw/$dataset"."[$structure]",
		"par_boundary" => "41",			
		"par_addFact"  => "-35",			
		"par_mulFact"  => "0.125",		
		);
	
	return;
} # end of DPomc

1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrvdp.pl>.

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

