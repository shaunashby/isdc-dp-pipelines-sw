package QLALIB;

=head1 NAME

I<QLALIB.pm> - nrtqla pipeline library

=head1 SYNOPSIS

use I<QLALIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use ISDCPipeline;
use ISDCLIB;

sub QLALIB::QCheck;
sub QLALIB::ParseOSF;

$| = 1;

########################################################################

=item B<QCheck> ( %att )

Quick Look Analysis Flux Check;  called from nqlscw  

This subroutine acquires some information from the swg and some IC files
and then calls q_set_src_fluxes and q_match_src_fluxes.

=cut

sub QCheck {
	&Carp::croak ( "QLALIB::QCheck: Need even number of args" ) if @_ % 2;
	
	my %att = @_;
	my $retval;
	my @result;
	my $srclResDOL;
	my $srclCatDOL;
	my $proc = &ProcStep()." $att{INST}";
	my $ogDOL       = $att{"ogDOL"};
	my $SWGIndexDOL = &ISDCPipeline::FindDOL ( "$ogDOL", "GNRL-SCWG-GRP-IDX" );
	
	print "\n========================================================================\n";
	
	print "#######     DEBUG:  ISDC_REF_CAT is $ENV{ISDC_REF_CAT}\n";
	
	my $jemxgroup  = "";
	my $jemxarf    = "";
	my $jemxebound = "";
	my $jemxrmf    = "";

	my $isgrgroup  = "";
	my $isgrarf    = "";
	my $isgrebound = "";
	my $isgrrmf    = "";

	if ( $att{INST} =~ /IBIS/ ) {
		$srclResDOL  = &ISDCPipeline::FindDOL ( "$ogDOL", "ISGR-SRCL-RES", "don't stop on error" );	#	do not include a 'y' in the last parameter
		$srclCatDOL  = &ISDCPipeline::FindDOL ( "$ogDOL", "ISGR-SRCL-CAT", "don't stop on error" );	#	do not include a 'y' in the last parameter
		( $isgrarf ) = &ISDCPipeline::GetICFile(
			"structure" => "ISGR-ARF.-RSP",
			"filematch" => "$ogDOL",
			);
		( $isgrgroup ) = &ISDCPipeline::GetICFile(
			"structure" => "ISGR-RMF.-GRP",
			"filematch" => "$ogDOL",
			);
		$isgrebound = &ISDCPipeline::FindDOL ( "$isgrgroup", "ISGR-EBDS-MOD" );
		$isgrrmf    = &ISDCPipeline::FindDOL ( "$isgrgroup", "ISGR-RMF.-RSP" );

	} elsif ( $att{INST} =~ /JMX/ ) {
		#	my ($jemxnum) = ( $INST =~ /\w{3}(\d)/ );
		$srclResDOL  = &ISDCPipeline::FindDOL ( "$ogDOL", "$att{INST}-SRCL-RES", "don't stop on error" );
		$srclCatDOL  = &ISDCPipeline::FindDOL ( "$ogDOL", "$att{INST}-SRCL-CAT", "don't stop on error" );
		( $jemxgroup ) = &ISDCPipeline::GetICFile(
			"structure" => "$att{INST}-RMF.-GRP",		
			"filematch" => "$ogDOL",
			);
		$jemxarf    = &ISDCPipeline::FindDOL ( "$jemxgroup", "$att{INST}-AXIS-ARF" );
		$jemxebound = &ISDCPipeline::FindDOL ( "$jemxgroup", "$att{INST}-FBDS-MOD" );
		$jemxrmf    = &ISDCPipeline::FindDOL ( "$jemxgroup", "$att{INST}-RMF.-RSP" );
	}

	#  050819 - Jake - SPR 4298
	#	Be aware that $att{INST} is IBIS, its NOT ISGR
	&Message ( "Did not find SRCL-CAT for $att{INST}." ) unless ( $srclCatDOL );
	&Message ( "Did not find SRCL-RES for $att{INST}." ) unless ( $srclResDOL );
	unless ( ( $srclResDOL ) && ( $srclCatDOL ) ) {
		&Message ( "Skipping q_set_src_fluxes and q_match_src_fluxes." );
		return;
	}

	#	since there is only one SW in Index, could have just GetAttribute on the SW
	#	instead of doing a GetColumn on the index.
	chomp ( my $RA_center  = &ISDCLIB::GetColumn ( "$SWGIndexDOL", "RA_SCX" ) );
	chomp ( my $DEC_center = &ISDCLIB::GetColumn ( "$SWGIndexDOL", "DEC_SCX" ) );
	chomp ( my $StartTime  = &ISDCLIB::GetColumn ( "$SWGIndexDOL", "TSTART" ) );
	chomp ( my $ScWID      = &ISDCLIB::GetColumn ( "$SWGIndexDOL", "SWID" ) );
	chomp ( my $OBTStart   = &ISDCLIB::GetColumn ( "$SWGIndexDOL", "OBTSTART" ) );

	&ISDCPipeline::PipelineStep (
		"step"             => "$proc - q_set_src_fluxes",
		"program_name"     => "q_set_src_fluxes",
		"par_srcl_cat_dol" => "$srclCatDOL",
		"par_srcl_res_dol" => "$srclResDOL",
		"par_isgr_history" => "$ENV{OPUS_WORK}/nrtqla/scratch/isgr_qla_history.fits[1]", 
		"par_jemx_history" => "$ENV{OPUS_WORK}/nrtqla/scratch/jemx_qla_history.fits[1]", 
		"par_jemxrmf"      => "$jemxrmf",
		"par_jemxarf"      => "$jemxarf",
		"par_jemxebound"   => "$jemxebound",
		"par_isgrrmf"      => "$isgrrmf",
		"par_isgrarf"      => "$isgrarf",
		"par_isgrebound"   => "$isgrebound",
		"par_jemxrenorm"   => "0.5",
		"par_chat"         => "2",
		);

	&ISDCPipeline::PipelineStep (
		"step"             => "$proc - q_match_src_fluxes",
		"program_name"     => "q_match_src_fluxes",
		"par_srcl_cat_dol" => "$srclCatDOL",
		"par_srcl_res_dol" => "$srclResDOL",
		"par_obtstart"     => "$OBTStart",
		"par_TOO_list_dol" => "$ENV{OPUS_WORK}/nrtqla/scratch/TOO_triggers.fits[1]",#
		"par_RA_center"    => "$RA_center",
		"par_DEC_center"   => "$DEC_center",
		"par_radial_limit" => "21",
		"par_factor1"      => "1.0",		
		"par_factor2"      => "1.0",		
		"par_factor3"      => "1.0",		
		"par_factor4"      => "1.0",			#	061026 - Jake - SCREW 1940 - from 10.0 to 1.0
		"par_limit1"       => "1.0",		
		"par_limit2"       => "1.0",		
		"par_limit3"       => "1.0",		
		"par_limit4"       => "1.0",		
		"par_alert1"       => "4",
		"par_alert2"       => "4",
		"par_alert3"       => "4",
		"par_alert4"       => "1",
		"par_alertA"       => "4",
		"par_alertB"       => "4",
		"par_alertC"       => "4",
		"par_alertD"       => "4",
		"par_SigLimit1"    => "10.0",	
		"par_SigLimit2"    => "30.0",
		"par_alertNew1"    => "2",
		"par_alertNew2"    => "3",
		"par_TooCheck"     => "1",
		"par_alertTOO"     => "2",
		"par_Soft_column"  => "1",
		"par_Hard_column"  => "2",
		"par_CompareByID"  => "1",
		"par_distlimit"    => "0.1",
		"par_ScWID"        => "$ScWID",
		"par_FluxModel"    => "0",
		"par_jemxrenorm"   => "0.5",
		"par_chat"         => "2",
		"par_jemxrmf"      => "$jemxrmf",
		"par_jemxarf"      => "$jemxarf",
		"par_jemxebound"   => "$jemxebound",
		"par_isgrrmf"      => "$isgrrmf",
		"par_isgrarf"      => "$isgrarf",
		"par_isgrebound"   => "$isgrebound",
		);

	return;

} # end of QCheck

########################################################################

=item B<ParseOSF> ( $dataset )

This function simply parses the dataset.

Returns ( $scwid, $revno, $inst, $INST, $og );

=cut

sub ParseOSF {
	my ( $dataset ) = @_;

	if ( $dataset =~ /^qs/ ) { 
		#	qsj2_055600020010
		my ( $in, $scwid ) = ( $dataset =~ /^qs(\w{2})_(\d{12})$/ );
		&Error ( "No inst found in dataset $ENV{OSF_DATASET}" ) unless ( $in );
		&Error ( "No scwid found in dataset $ENV{OSF_DATASET}" ) unless ( $scwid );
		my ( $revno ) = ( $scwid =~ /^(\d{4}).*/ );
		&Error ( "No revno found in scwid $scwid" ) unless ( $revno );
		my $inst = &ISDCLIB::in2inst ( $in );
		( my $INST = $inst ) =~ tr/a-z/A-Z/;
		my $og = "og_$inst.fits";
		return ( $scwid, $revno, $inst, $INST, $og );
	} else {

		#	070503 - Jake - SCREW 1983 - added parsing for mosaics

		#	qmib_0555_04200520001_0010
		my ( $in, $revno, $obsid, $pdefv ) = ( $dataset =~ /^qm(\w{2})_(\d{4})_(\d{11})_(\d{4})$/ );
		&Error ( "No inst found in dataset $ENV{OSF_DATASET}" )  unless ( $in );
		&Error ( "No obsid found in dataset $ENV{OSF_DATASET}" ) unless ( $obsid );
		&Error ( "No revno found in dataset $ENV{OSF_DATASET}" ) unless ( $revno );
		my $inst = &ISDCLIB::in2inst ( $in );
		( my $INST = $inst ) =~ tr/a-z/A-Z/;
		my $og = "og_$inst.fits";
		return ( $obsid, $revno, $inst, $INST, $og, $pdefv );
	}

}	#	end of ParseOSF


########################################################################

1;

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

Tess Jaffe <theresa.jaffe@obs.unige.ch>

=cut

#	last line
