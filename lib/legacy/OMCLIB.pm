package OMCLIB;

=head1 NAME

I<OMCLIB.pm> - library used by nrtqla, conssa and consssa

=head1 SYNOPSIS

use I<OMCLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;

$| = 1;

=item B<OSA> ( %att )

=cut

sub OSA {
	&Carp::croak ( "OMCLIB::OSA: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	$att{proctype} = "scw"  unless $att{proctype} =~ /mosaic/;       #  either "scw" or "mosaic"
	$att{IC_Group} = "../../idx/ic/ic_master_file.fits[1]" unless ( $att{IC_Group} );
	
	my $proc = &ProcStep();
	
	print "\n========================================================================\n";
	
	&Message ( "Running OMCLIB::OSA for dataset:$ENV{OSF_DATASET}: process:$ENV{PROCESS_NAME}; proctype:$att{proctype}.\n" );



	my $ogDOL = "og_omc.fits[1]";

	#	default settings for consssa / scw
	my $startLevel = "COR";
	my $endLevel   = "IMA2";

	if ( $ENV{PROCESS_NAME} =~ /cssscw/ ) {
		#	use defaults
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csaob1/ ) {
		$startLevel = "IMA2";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csasw1/ ) {
		$ogDOL      = &ISDCLIB::FindDirVers ( "scw/$att{scwid}" )."/swg_omc.fits[GROUPING]";
		$endLevel   = "IMA";
	}
	else {
		&Error ( "No match found for PROCESS_NAME: $ENV{PROCESS_NAME}; proctype: $att{proctype}\n" );
	}

 	if ( $att{proctype} =~ /scw/ ) { 
		#       Check to ensure that only 1 child on OG
		my $numScwInOG = &ISDCLIB::ChildrenIn ( $ogDOL, "GNRL-SCWG-GRP-IDX" );
		&Error ( "Not 1 child in $ogDOL: $numScwInOG" ) unless ( $numScwInOG == 1 );
	}
	
	my ($retval,@result) = &ISDCPipeline::PipelineStep (
		"step"                   => "$proc - processing ogid/Scw $ENV{OSF_DATASET}",
		"program_name"           => "omc_science_analysis",
		"par_ogDOL"              => "$ogDOL",
		"par_startLevel"         => "$startLevel",
		"par_endLevel"           => "$endLevel",
		"par_chatter"            => "1",
		"par_IC_Alias"           => "$ENV{IC_ALIAS}",
		"par_IC_Group"           => "$att{IC_Group}",

		"par_IMA_inpCat"         => "",
		"par_IMA_maxWcsOff"      => "10.0",
		"par_IMA_wcsFlag"        => "n",

		"par_IMA_noiseHighLeft"  => "33.0",
		"par_IMA_noiseHighRight" => "35.0",
		"par_IMA_noiseLowLeft"   => "45.0",
		"par_IMA_noiseLowRight"  => "49.0",
		"par_COR_biastime"       => "630",
		"par_COR_darkCurrent"    => "",
		"par_COR_flatField"      => "",
		"par_COR_higain"         => "5.0",
		"par_COR_kscKappa"       => "3",
		"par_COR_lowgain"        => "30.0",
		"par_GTI_Accuracy"       => "any",
		"par_GTI_TimeFormat"     => "IJD",
		"par_GTI_attTolerance"   => "0.1",
		"par_GTI_gtiOmcNames"    => "",
		"par_GTI_gtiScNames"     => "",
		"par_GTI_gtiUser"        => "",
		"par_GTI_omcLimitTable"  => "",
		"par_GTI_scLimitTable"   => "",
		"par_IMA_badPixels"      => "",
		"par_IMA_magboxsize"     => "5",
		"par_IMA_maxCentOff"     => "2",
		"par_IMA_maxshottime"    => "300",
		"par_IMA_minBoxFrac"     => "0.90",
		"par_IMA_minSNR"         => "1.0",
		"par_IMA_minTimeFrac"    => "0.99",
		"par_IMA_minshottime"    => "0",
		"par_IMA_numSigma"       => "2",
		"par_IMA_photCal"        => "",
		"par_IMA_scienceImage"   => "n",
		"par_IMA_skyStdDev"      => "10.0",
		"par_IMA_timestep"       => "630",
		"par_IMA_triggerImage"   => "y",
		"par_IMA_usePrp"         => "y",
		"par_IMA_startshot"      => "1",
		"par_IMA_clobber"        => "y",
		"par_IMA_triggersize"    => "0",
		"par_IMA_outfitsname"    => "",
		"par_IMA_accurateWCS"    => "y",
		"par_IMA_attach"         => "y",
		"par_IMA_endshot"        => "0",
		"par_IMA_onlyImage"      => "n",
		"par_IMA_omc_id"         => "",
		"par_IMA_datalevel"      => "COR",
		"stoponerror"            => 0,
		);
        
	if ($retval) {
		if ($retval =~ /491099|35644/) {
			&Message ( "$proc - WARNING:  no data;  continuing." );
		} else {
			print "*******     ERROR:  return status of $retval from omc_science_analysis not allowed.\n";
			exit 1;      
		}
	} # if error

	
	return;
	
} # end of OSA

1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

