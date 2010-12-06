package RevJMX;

=head1 NAME

RevJMX.pm - NRT Revolution File Pipeline JMX Module

=head1 SYNOPSIS

use I<RevJMX.pm>;
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
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

sub RevJMX::DPjm;
sub RevJMX::ACAjm;
sub RevJMX::DPjme;
sub RevJMX::ACAjme;

$| = 1;


##########################################################################

=item B<DPjm> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for jemxN_raw_frss files:

The pipeline first creates the output file in the aca subdirectory of the 
workdir (named the same as the input file but with "aca" instead of "raw".)
The executable B<j_prp_frss_obt> is then called to fill in the on board time
in the output file (which will be filled in the B<nrvaca> process.

=cut

sub DPjm {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
#	my $jmxno = $dataset;
#	$jmxno =~ s/.*mx(\d)_raw_frss.*/$1/;
	( my $jmxno = $dataset ) =~ s/.*mx(\d)_raw_frss.*/$1/;
#	my $newdataset = $dataset;
#	$newdataset =~ s/raw/aca/g;
	( my $newdataset = $dataset ) =~ s/raw/aca/g;
	# Remove version:
	$newdataset =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	my $ext = "[GROUPING]";
	my $tpl = "JMX".$jmxno."-GAIN-CAL-IDX";
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JemX create GAIN-CAL structure",
		"program_name" => "dal_create",
		"par_obj_name" => "$newdataset",
		"par_template" => "$tpl.tpl",
		"subdir"       => "$workdir/aca",
		);
	chdir("$workdir") or &Error ( "Cannot chdir back to $workdir" );
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JemX j_prp_frss_obt",
		"program_name" => "j_prp_frss_obt",
		"par_inDOL"    => "raw/$dataset"."$ext",
		"par_outDOL"   => "aca/$newdataset".$ext,
		"par_jemxNum"  => "$jmxno",
		"par_clobber"  => "yes",
		);
	
	return;
} # end of DPjm


##########################################################################

=item B<ACAjm> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for jemxN_raw_frss files:

The JemX ACA file created in B<nrvdp> is filled with the executable 
B<j_calib_gain_fitting>, which uses the B<jmxN_calb_cfg> and B<jmxN_calb_mod>
files found in the IC repository.  

=cut

sub ACAjm {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
#	my $jmxnum = $dataset;
#	$jmxnum =~ s/.*mx(\d)_raw_frss.*$/$1/;
	( my $jmxnum = $dataset ) =~ s/.*mx(\d)_raw_frss.*$/$1/;
#	my $newdataset = $dataset;
#	$newdataset =~ s/raw/aca/;
	( my $newdataset = $dataset ) =~ s/raw/aca/;
	# Remove version:
	$newdataset =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	
	my $imodgrp = &ISDCPipeline::GetICFile(
		"structure" => "JMX$jmxnum-IMOD-GRP",
		"filematch" => "aca/$newdataset"."[JMX$jmxnum-GAIN-CAL]",  
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JemX j_calib_gain_fitting",
		"program_name" => "j_calib_gain_fitting",
		"par_inCAL"    => "raw/$dataset"."[GROUPING]",
		"par_outCAL"   => "aca/$newdataset"."[GROUPING]",
		"par_instMod"  => "$imodgrp",
		"par_chatter"  => "1",
		"par_clobber"  => "y",
		"par_jemxNum"  => "$jmxnum",
		);
	
} # end of ACAjm


#############################################################################


=item B<DPjme> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

DP step for JEMX ECAL data

=cut

sub DPjme {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
#	my $jmxnum = $dataset;
#	$jmxnum =~ s/^.*mx(\d)_raw_ecal.*$/$1/;
	( my $jmxnum = $dataset ) =~ s/^.*mx(\d)_raw_ecal.*$/$1/;
	
#	my $newdataset = $dataset;
#	$newdataset =~ s/raw/prp/;
	( my $newdataset = $dataset ) =~ s/raw/prp/;
	$newdataset =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect raw file",
		"program_name" => "$mychmod -w raw/$dataset",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - create ECAL PRP group",
		"program_name" => "dal_create",
		"par_obj_name" => "prp/$newdataset",
		"par_template" => "JMX$jmxnum-ECAL-GRP.tpl",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - attach raw to group",
		"program_name" => "dal_attach",
		"par_Parent"   => "prp/$newdataset"."[GROUPING]",
		"par_Child1"   => "raw/$dataset"."[JMX$jmxnum-ECAL-CRW]",
		"par_Child2"   => "",
		"par_Child3"   => "",
		"par_Child4"   => "",
		"par_Child5"   => "",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - dal_attr_copy",
		"program_name" => "dal_attr_copy",
		"par_indol"    => "raw/$dataset"."[JMX$jmxnum-ECAL-CRW]",
		"par_outdol"   => "prp/$newdataset"."[GROUPING]",
		"par_keylist"  => "ERTFIRST,ERTLAST,REVOL",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - dal_attr_copy",
		"program_name" => "dal_attr_copy",
		"par_indol"    => "raw/$dataset"."[JMX$jmxnum-ECAL-CRW]",
		"par_outdol"   => "prp/$newdataset"."[JMX$jmxnum-ECAL-CPR]",
		"par_keylist"  => "ERTFIRST,ERTLAST,REVOL",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"                => "$proc - dp_obt_calc",
		"program_name"        => "dp_obt_calc",
		"par_InSWGroup"       => "prp/$newdataset"."[GROUPING]",
		"par_OutSWGroup"      => "",
		"par_RawData"         => "",
		"par_ConvertedData"   => "",
		"par_AttributeData"   => "",
		"par_TimeInfo"        => "",
		"par_IN_STRUCT_NAME"  => "JMX$jmxnum-ECAL-CRW",
		"par_OUT_STRUCT_NAME" => "JMX$jmxnum-ECAL-CPR",
		"par_ATT_STRUCT_NAME" => "",
		"par_LOBT_2X4_NAMES"  => "",
		"par_LOBT_1X8_NAMES"  => "LOBT_EVENT OB_TIME",
		"par_PKT_NAMES"       => "",
		"par_LOBT_ATTR"       => "",
		"par_PKT_ATTR"        => "",
		"par_OBT_TYPE"        => "JEMX",
		);
	
}


#############################################################################

=item B<ACAjme> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

DP step for JEMX ECAL data

=cut

sub ACAjme {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
#	my $jmxnum = $dataset;
#	$jmxnum =~ s/^.*mx(\d)_raw_ecal.*$/$1/;
	( my $jmxnum = $dataset ) =~ s/^.*mx(\d)_raw_ecal.*$/$1/;
	
#	my $newdataset = $dataset;
#	$newdataset =~ s/raw/prp/;
	( my $newdataset = $dataset ) =~ s/raw/prp/;
	$newdataset =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	
	my $imodgrp = &ISDCPipeline::GetICFile(
		"structure" => "JMX$jmxnum-IMOD-GRP",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"           => "$proc - j_calib_adc",
		"program_name"   => "j_calib_adc",
		"par_ecalRaw"    => "raw/$dataset"."[JMX$jmxnum-ECAL-CRW]",
		"par_ecalPrp"    => "prp/$newdataset"."[JMX$jmxnum-ECAL-CPR]",
		"par_instMod"    => "$imodgrp",
		"par_alrtOffset" => "$ENV{JMX_ECAL_ALRT_OFFSET}",
		"par_alrtSlope"  => "$ENV{JMX_ECAL_ALRT_SLOPE}",
		"par_alrtZero"   => "$ENV{JMX_ECAL_ALRT_ZERO}",
		"par_alrtBoth"   => "$ENV{JMX_ECAL_ALRT_BOTH}",
		"par_alrtOther"  => "$ENV{JMX_ECAL_ALRT_OTHER}",
		"par_jemxNum"    => "$jmxnum",
		"par_chatter"    => "1",
		);
	
}
#############################################################################

1;

__END__


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

