package SPILIB;

=head1 NAME

I<SPILIB.pm> - library used by nrtqla, conssa and consssa

=head1 SYNOPSIS

use I<SPILIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use ISDCLIB;

$| = 1;

=item B<SSA>

=cut

sub SSA {
	&Carp::croak ( "SPILIB::SSA: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	$att{proctype} = "scw"  unless ( $att{proctype} =~ /mosaic/ );                        #       either "scw" or "mosaic"
	$att{IC_Group} = "../../idx/ic/ic_master_file.fits[1]" unless ( $att{IC_Group} );
	$att{revno}    = "0000" unless ( $att{revno} );	#	for conssa

	my $proc = &ProcStep();

	print "\n========================================================================\n";
	my %parameters = (
		"par_obs_group"                  => "og_spi.fits[1]",										#	MAY HAVE TO BE FILENAME AND NOT DOL
		"par_IC_Alias"                   => "$ENV{IC_ALIAS}",
		"par_IC_Group"                   => "$att{IC_Group}",
		"par_coeff_DOL"                  => "",			#	050422 - Should I still specify
		"par_log_File"                   => "logs/spi_sa.log",

		"par_run_cat_extract"            => "YES",
		"par_spiros_source-cat-dol"      => "",
		"par_detectors"                  => "0-18",			#	shouldn't this be an IC file
		"par_spiros_nofsources"          => "2",
		"par_cat_extract_fluxMin"        => "0.0",
		"par_spibounds_nbins"            => "1,1,1,1,1",
		"par_spibounds_nregions"         => "5",
		"par_spibounds_regions"          => "20,40,80,150,300,1000",
		"par_spiros_srclocbins"          => "ALL",
		"par_spiros_image-proj"          => "TAN",
		"par_spiros_sigmathres"          => "5.0",
		"par_spiros_detector-subset"     => "",		#	"$spiros_detector_subset",
		"par_spiros_background-method"   => "5",
		"par_spiros_image-fov"           => "POINTING+ZCFOV",
		"par_spiros_optistat"            => "LIKEH",
		"par_spiros_source-timing-mode"  => "WINDOW",
		"par_spiros_source-timing-scale" => "0.5",
		"par_catalog"                    => "$ENV{ISDC_REF_CAT}"."[SPI_FLAG==1]",

		#	060615 - Jake - added for spi_scripts-4.0 (all default values)
		"par_run_gaincorrection"               => "no",
		"par_run_phase_analysis"               => "no",
		"par_spi_flatfield_ptsNbConstBack"     => "5",
		"par_spi_phase_hist_T90epoch"          => "5911.6",
		"par_spi_phase_hist_asini"             => "80.6",
		"par_spi_phase_hist_ecc"               => "0.514",
		"par_spi_phase_hist_ephemDOL"          => "Crab_ephemeris.fits",	#	where does this file come from???
		"par_spi_phase_hist_omega_d"           => "249",
		"par_spi_phase_hist_orbit"             => "no",
		"par_spi_phase_hist_phaseBinNum"       => "20",
		"par_spi_phase_hist_phaseBounds"       => "",
		"par_spi_phase_hist_phaseOffNum"       => "0",
		"par_spi_phase_hist_phaseSameWidthBin" => "yes",
		"par_spi_phase_hist_phaseSubtractOff"  => "no",
		"par_spi_phase_hist_porb"              => "34.29",
		"par_spi_phase_hist_pporb"             => "0.0",
		"par_use_pointing_filter"              => "yes",

		#	unchanged from spi_scripts 3.0 defaults
		"par_IRF_DOL"                    => "",
		"par_RMF_DOL"                    => "",
		"par_cat_extract_fluxMax"        => "1000",
		"par_clobber"                    => "yes",
		"par_coordinates"                => "RADEC",
		"par_run_background"             => "YES",
		"par_run_binning"                => "YES",
		"par_run_pointing"               => "YES",
		"par_run_simulation"             => "NO",
		"par_run_spiros"                 => "YES",
		"par_spi_add_sim_FluxScale"      => "0.01",
		"par_spi_add_sim_SrcLat"         => "19.0",
		"par_spi_add_sim_SrcLong"        => "80.0",
		"par_spi_obs_back_model01"       => "GEDSAT",
		"par_spi_obs_back_model02"       => "ADJACENT",
		"par_spi_obs_back_model03"       => "GEDSAT",
		"par_spi_obs_back_model04"       => "GEDSAT",
		"par_spi_obs_back_mpar01"        => "",
		"par_spi_obs_back_mpar02"        => "",
		"par_spi_obs_back_mpar03"        => "",
		"par_spi_obs_back_mpar04"        => "",
		"par_spi_obs_back_nmodel"        => "1",
		"par_spi_obs_back_norm01"        => "NO",
		"par_spi_obs_back_norm02"        => "OFFLINE",
		"par_spi_obs_back_norm03"        => "NO",
		"par_spi_obs_back_norm04"        => "NO",
		"par_spi_obs_back_npar01"        => "",
		"par_spi_obs_back_npar02"        => "1786-1802,1815-1828 keV",
		"par_spi_obs_back_npar03"        => "",
		"par_spi_obs_back_npar04"        => "",
		"par_spi_obs_back_scale01"       => "1.0",
		"par_spi_obs_back_scale02"       => "1.0",
		"par_spi_obs_back_scale03"       => "1.0",
		"par_spi_obs_back_scale04"       => "1.0",
		"par_spiros_energy-subset"       => "",
		"par_spiros_iteration-output"    => "NO",
		"par_spiros_mode"                => "IMAGING",
		"par_spiros_pointing-subset"     => "",

		"par_run_fullcheck"              => "no",			#	added for spi_scripts-4.5
		"par_spi_flatfield_single"       => "no",			#	added for spi_scripts-4.5
		"par_spi_templates_scaling"      => "1",			#	added for spi_scripts-4.5
		"par_spi_templates_type"         => "GEDSAT",	#	added for spi_scripts-4.5
		"par_use_background_flatfields"  => "yes",		#	added for spi_scripts-4.5
		"par_use_background_models"      => "no",			#	added for spi_scripts-4.5
		"par_use_background_templates"   => "no",			#	added for spi_scripts-4.5
	        "stoponerror"                    => 0,				#	071204 - SCREW 997
	    );

	if ( $att{proctype} =~ /scw/ ) {			#	SPI is never processed on a scw by scw basis
		#	Check to ensure that only 1 child on OG
		my $numScwInOG = &ISDCLIB::ChildrenIn ( $parameters{'par_obs_group'}, "GNRL-SCWG-GRP-IDX" );
		&Message ( "Not 1 child in $parameters{'par_obs_group'}: $numScwInOG" ) unless ( $numScwInOG == 1 );
	}

#	my $depth = "../..";
#	$depth .= "/.." unless ( $att{revno} =~ /9999/ );

	if ( $ENV{PROCESS_NAME} =~ /^csa/ ) {
		$parameters{'par_cat_extract_fluxMin'}    = "0.001";
		$parameters{'par_run_cat_extract'}        = "NO";

		#	NOTE that the following perl variable has an "_" whereas the spi_science_analysis variable has a "-"
#		$spiros_detector_subset = ( $ENV{SA_UNIT_TEST} =~ /TRUE/ ) ? "AUTO" : "0-18";   #  050808 - Jake - OSA 5 parameter update

		$parameters{'par_spiros_image-fov'}       = "POINTING+FCFOV";
		$parameters{'par_spiros_image-proj'}      = "CAR";
		$parameters{'par_spiros_nofsources'}      = "5";
		$parameters{'par_spiros_optistat'}        = "CHI2";
		$parameters{'par_spiros_source-timing-mode'}            = "QUICKLOOK";
		$parameters{'par_spiros_source-timing-scale'}           = "0.1";
		$parameters{'par_spiros_srclocbins'}      = "FIRST";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /^css/ ) {
		if ( ( $att{revno} <= "0140" ) || ( $att{revno} == "0000" ) ){
		}
		elsif ( ( $att{revno} >= "0141" ) && ( $att{revno} <= "0214" ) ){
			$parameters{'par_detectors'} = "0,1,3-19,21-24,26-29,34-60,63-66,68-70,74-84";
		} else {
			$parameters{'par_detectors'} = "0,1,3-16,18,19,21-24,26-29,34-44,46,48-58,63-66,68-70,74-79,81,83";
		}
#		$parameters{'par_coeff_DOL'} = &ISDCLIB::FindDirVers("$depth/scw/$att{revno}/rev")."/aca/spi_gain_coeff.fits.gz" 
		$parameters{'par_coeff_DOL'} = &ISDCLIB::FindDirVers("../../../scw/$att{revno}/rev")."/aca/spi_gain_coeff.fits.gz" 
			unless ( $att{revno} =~ /0000/ );
	}
	else {
		&Error ( "No match found for PROCESS_NAME: $ENV{PROCESS_NAME}\n" );
	}
	#	NO QLA!


	&Message ( "REVNO = $att{revno} : Using \$detectors = $parameters{'par_detectors'}" );

	my ($retval,@result) = &ISDCPipeline::PipelineStep(
		"step"                           => "CSS Obs SPI - processing ogid $ENV{OSF_DATASET}",
		"program_name"                   => "spi_science_analysis",
		%parameters
		);

	if ($retval) {		#	071204 - Jake - SCREW 997 added 35644
		if ($retval =~ /35644/) {
			&Message ( "$proc - WARNING:  no data;  continuing." );
		} else {
			print "*******     ERROR:  return status of $retval from spi_science_analysis not allowed.\n";
			exit 1;      
		}
	} # if error

	
	&ISDCPipeline::RunProgram("$mymv Parameters*par logs/")
		if ( `$myls Parameters*par 2> /dev/null` );

	&ISDCPipeline::RunProgram("$myrm GNRL-REFR-CAT.fits")
		if ( -e "GNRL-REFR-CAT.fits" );

	return;

} # end of SSA.

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
