package IBISLIB;

=head1 NAME

I<IBISLIB.pm> - library used by nrtqla, conssa and consssa

=head1 SYNOPSIS

use I<IBISLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;

sub IBISLIB::ISA;
sub IBISLIB::IPMosaic;

$| = 1;

=item B<ISA> ( %att )

Wrapper for ibis_science_analysis

=cut

sub ISA {
	&Carp::croak ( "IBISLIB::ISA: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	$att{proctype}     = "scw"  unless $att{proctype} =~ /mosaic/;			#	either "scw" or "mosaic"
	$att{IC_Group}     = "../../idx/ic/ic_master_file.fits[1]" unless ( $att{IC_Group} );
	$att{instdir}      = "obs" unless ( $att{instdir} );

	my $proc           = &ProcStep();
	&Message ( "Running IBISLIB::ISA for dataset:$ENV{OSF_DATASET}: process:$ENV{PROCESS_NAME}; proctype:$att{proctype}; INST:$att{INST}." );

	my %parameters = (
		"par_ogDOL"                   => "og_ibis.fits[1]",
		"par_startLevel"              => "GTI",
		"par_endLevel"                => "IMA2",
		"par_IC_Group"                => "$att{IC_Group}",
		"par_CAT_refCat"              => "$ENV{ISDC_REF_CAT}",
		"par_SWITCH_disableIsgri"     => "no",
		"par_SWITCH_disablePICsIT"    => "yes",
		"par_SCW1_BKG_I_isgrBkgDol"   => "-",
		"par_SCW1_BKG_I_isgrUnifDol"  => "-",
		"par_IBIS_II_ChanNum"         => "-1",
		"par_IBIS_II_E_band_min"      => "20 40",		#	only used when IBIS_II_ChanNum = 2 (nrtqla)
		"par_IBIS_II_E_band_max"      => "40 80",		#	only used when IBIS_II_ChanNum = 2 (nrtqla)
		"par_OBS1_SearchMode"         => "3",
		"par_OBS1_DoPart2"            => "0",
		"par_OBS1_ToSearch"           => "10",
		"par_OBS1_MapSize"            => "40.",
		"par_SCW1_ICOR_riseDOL"       => "",
		"par_rebinned_corrDol_ima"    => "",
		"par_rebinned_backDol_ima"    => "",
		"par_IC_Alias"                => "$ENV{IC_ALIAS}",
		"par_PICSIT_inCorVar"         => "1",
		"par_SCW1_GTI_PICsIT"         => "ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS",
		"par_GENERAL_levelList"       => "PRP,COR,GTI,DEAD,BIN_I,BKG_I,CAT_I,IMA,IMA2,BIN_S,SPE,LCR,COMP,CLEAN",
		"par_SCW1_GTI_attTolerance_X" => "0.05",
		"par_SCW1_GTI_attTolerance_Z" => "0.2",
		"par_OBS1_ExtenType"          => "0",
		"par_tolerance"               => "0.0001",
		"par_IBIS_IPS_ChanNum"        => "0",
		"par_IBIS_IPS_E_band_max_m"   => "600 1000 10000",        
		"par_IBIS_IPS_E_band_max_s"   => "600 1000 10000",        
		"par_IBIS_IPS_E_band_min_m"   => "170 600 1000",          
		"par_IBIS_IPS_E_band_min_s"   => "170 600 1000",          
		"par_IBIS_min_rise"           => "7",
		"par_IBIS_max_rise"           => "90",
		"par_IBIS_P_convFact"         => "7.0",
		"par_IBIS_SPS_ChanNum"        => "51",
		"par_SCW1_GTI_TimeFormat"     => "OBT",
		"par_SCW1_GTI_ISGRI"          => "ATTITUDE ISGRI_DATA_GAPS",
		"par_SCW1_ICOR_probShot"      => "0.01",
		"par_SCW1_BIN_cleanTrk"       => "0",# "1"
		"par_SCW1_BKG_I_method_cor"   => "0",
		"par_brSrcDOL"                => "",
		"par_SCW1_BKG_P_method"       => "0",
		"par_SCW1_BKG_picsSUnifDOL"   => "",
		"par_SCW1_BKG_picsMUnifDOL"   => "",
		"par_OBS1_MapAlpha"           => "0.0",
		"par_OBS1_MapDelta"           => "0.0",
		"par_OBS1_SouFit"             => "1",
		"par_SCW2_ISPE_MethodFit"     => "1",
		"par_SCW2_BKG_I_method_cor"   => "0",
		"par_SCW2_BKG_P_method"       => "0",
		"par_ILCR_num_e"              => "2",                                        # "4"
		"par_ILCR_e_min"              => "15 40",                                    # "20 40 60 100"
		"par_ILCR_e_max"              => "40 300",                                   # "40 60 100 200"
		"par_OBS2_projSel"            => "TAN",
		"par_SWITCH_disableCompton"   => "yes",		#	don't think that we ever use Compton
		"par_IBIS_II_inEnergyValues"  => "energy_bands.fits[1]",	#	not used unless IBIS_II_ChanNum = -1, so can hard code this
		"par_IBIS_NoisyDetMethod"     => "1",		#	060116 - SCREW 1783 denied so this stays at 1
		"par_OBS1_MinNewSouSnr"       => "5",                                        # "7."
		"par_OBS1_MinCatSouSnr"       => "1",
		"par_OBS2_imgSel"             => "EVT_TYPE==SINGLE && E_MIN==252 && E_MAX==336",
		"par_ModPixShad"              => "400",
		"par_brPifThreshold"          => "0.0001",
		"par_SCW1_BIN_P_PicsCxt"      => "",
		"par_SCW2_BIN_P_PicsCxt"      => "",
		"par_CAT_usrCat"              => "",
		"par_GENERAL_clobber"         => "yes",
		"par_IBIS_IPS_corrPDH"        => "0",                                                  
		"par_IBIS_SI_ChanNum"         => "-1",
		"par_IBIS_SI_E_band_max"      => "",
		"par_IBIS_SI_E_band_min"      => "",
		"par_IBIS_SI_inEnergyValues"  => "",
		"par_IBIS_SM_inEnergyValues"  => "",
		"par_IBIS_SPS_E_band_max_m"   => "",                                      
		"par_IBIS_SPS_E_band_max_s"   => "",                                      
		"par_IBIS_SPS_E_band_min_m"   => "",                                              
		"par_IBIS_SPS_E_band_min_s"   => "",                                              
		"par_IBIS_SPS_corrPDH"        => "0",                                                  
		"par_IBIS_SP_ChanNum"         => "51",
		"par_IBIS_SS_inEnergyValues"  => "",
		"par_IBIS_IP_ChanNum"         => "3",
		"par_IBIS_IP_E_band_max_m"    => "600 1000 13500",
		"par_IBIS_IP_E_band_max_s"    => "600 1000 10000",
		"par_IBIS_IP_E_band_min_m"    => "350 600 1000",
		"par_IBIS_IP_E_band_min_s"    => "175 600 1000",
		"par_IBIS_SP_E_band_max_m"    => "",
		"par_IBIS_SP_E_band_max_s"    => "",
		"par_IBIS_SP_E_band_min_m"    => "",
		"par_IBIS_SP_E_band_min_s"    => "",
		"par_ILCR_delta_t"            => "100",                                    
		"par_ILCR_select"             => "",                                                
		"par_ISGRI_mask"              => "",
		"par_OBS1_CAT_class"          => "",
		"par_OBS1_CAT_date"           => "-1.",
		"par_OBS1_CAT_fluxDef"        => "",
		"par_OBS1_CAT_fluxMax"        => "",
		"par_OBS1_CAT_fluxMin"        => "",
		"par_OBS1_CAT_radiusMax"      => "20",
		"par_OBS1_CAT_radiusMin"      => "0",
		"par_OBS1_CleanMode"          => "1",
		"par_OBS1_DataMode"           => "0",
		"par_OBS1_PixSpread"          => "1",
		"par_OBS1_ScwType"            => "POINTING",
		"par_OBS1_covrMod"            => "",
		"par_OBS1_deco"               => "",
		"par_OBS1_FastOpen"           => "1",
		"par_OBS1_NegModels"          => "0",
		"par_OBS2_detThr"             => "3.0",                                                     
		"par_PICSIT_detThr"           => "3.0",                           
		"par_PICSIT_outVarian"        => "0",
		"par_PICSIT_deco"             => "",
		"par_PICSIT_source_DEC"       => "",
		"par_PICSIT_source_RA"        => "",
		"par_PICSIT_source_name"      => "",
		"par_SCW1_BIN_I_idxLowThre"   => "",
		"par_SCW1_BIN_P_HepiLut"      => "",                                                 
		"par_SCW1_BIN_P_inDead"       => "",
		"par_SCW1_BIN_P_inGTI"        => "",
		"par_SCW1_BKG_I_method_int"   => "1",
		"par_SCW1_BKG_badpix"         => "yes",                 
		"par_SCW1_BKG_divide"         => "no",                  
		"par_SCW1_BKG_flatmodule"     => "no",              
		"par_SCW1_BKG_picsMBkgDOL"    => "",
		"par_SCW1_BKG_picsSBkgDOL"    => "",
		"par_SCW1_BIN_I_idxNoisy"     => "",
		"par_SCW1_GTI_Accuracy"       => "any",
		"par_SCW1_GTI_BTI_Dol"        => "",                           
		"par_SCW1_GTI_BTI_Names"      => "",                 
		"par_SCW1_GTI_LimitTable"     => "",
		"par_SCW1_GTI_SCI"            => "",
		"par_SCW1_GTI_SCP"            => "",
		"par_SCW1_GTI_gtiUserI"       => "",
		"par_SCW1_GTI_gtiUserP"       => "",
		"par_SCW1_ICOR_GODOL"         => "",
		"par_SCW1_ICOR_idxSwitch"     => "",
		"par_SCW1_ISGRI_event_select" => "",
		"par_SCW1_PCOR_enerDOL"       => "",
		"par_SCW1_veto_mod"           => "",
		"par_SCW2_BIN_I_idxLowThre"   => "",
		"par_SCW2_BIN_P_HepiLut"      => "",                                                 
		"par_SCW2_BIN_P_inDead"       => "",
		"par_SCW2_BIN_P_inGTI"        => "",
		"par_SCW2_BIN_cleanTrk"       => "0",
		"par_SCW2_BKG_I_isgrBkgDol"   => "",
		"par_SCW2_BKG_I_method_int"   => "1",
		"par_SCW2_BKG_badpix"         => "yes",                         
		"par_SCW2_BKG_divide"         => "no",                          
		"par_SCW2_BKG_flatmodule"     => "no",                      
		"par_SCW2_BKG_picsMBkgDOL"    => "-",
		"par_SCW2_BKG_picsMUnifDOL"   => "",
		"par_SCW2_BKG_picsSBkgDOL"    => "-",
		"par_SCW2_BKG_picsSUnifDOL"   => "",
		"par_SCW2_ISGRI_event_select" => "",
		"par_SCW2_ISPE_DataMode"      => "0",
		"par_SCW2_ISPE_MethodInt"     => "1",
		"par_SCW2_ISPE_idx_isgrResp"  => "",
		"par_SCW2_ISPE_isgrUnifDol"   => "",
		"par_SCW2_PIF_filter"         => "",                            
		"par_SCW2_cat_for_extract"    => "",               
		"par_SCW2_catalog"            => "",                                       
		"par_SCW2_BIN_I_idxNoisy"     => "",
		"par_SCW2_ISPE_isgrarf"       => "",
		"par_SWITCH_osimData"         => "NO",
		"par_chatter"                 => "2",
		"par_corrDol"                 => "",                                                                            
		"par_staring"                 => "no",
		"par_sum_spectra"             => "no",
		"par_rebin_arfDol"            => "",
		"par_rebin_slope"             => "-2",
		"par_rebinned_backDol_lc"     => "",
		"par_rebinned_backDol_spe"    => "",
		"par_rebinned_corrDol_lc"     => "",
		"par_rebinned_corrDol_spe"    => "",
		"par_aluAtt" => "",
		"par_leadAtt" => "",
		"par_tungAtt" => "",
		"par_SCW1_ICOR_supGDOL"   => "",
		"par_SCW1_ICOR_supODOL"   => "",
		"par_protonDOL"           => "",
		"stoponerror"             => 0,		#	071204 - Jake - SCREW 997
	);
	#	set defaults to consssa/scw, cause its easier programmatically

	#####################################################################################

	#	differentiate between conssa, consssa and nrtqla with $ENV{PROCESS_NAME}
	if ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $att{proctype} =~ /scw/ ) ) {
		#	use the defaults
	}
	elsif ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $att{proctype} =~ /mosaic/ ) ) {
		$parameters{'par_startLevel'}   = "CAT_I";
		$parameters{'par_endLevel'}     = "IMA";
		$parameters{'par_OBS1_SearchMode'}   = "2";
		$parameters{'par_OBS1_DoPart2'}      = "2";
		$parameters{'par_OBS1_MapSize'}      = "80";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /nqlobs/ ) { 
		$parameters{'par_startLevel'}   = "CAT_I";
		$parameters{'par_endLevel'}     = "IMA";
		$parameters{'par_OBS1_ToSearch'}     = "50";
		$parameters{'par_OBS1_DoPart2'}      = "2";
		$parameters{'par_IBIS_II_ChanNum'}   = "2";
		$parameters{'par_OBS1_SearchMode'}   = "2";
		$parameters{'par_OBS1_MinCatSouSnr'} = "4"; 
		$parameters{'par_SCW1_BKG_I_isgrBkgDol'}  = "";
		$parameters{'par_SCW1_BKG_I_isgrUnifDol'} = "";

#	071127 - testing new qla parameters
		$parameters{'par_tolerance'} = 0.1;
		$parameters{'par_IBIS_min_rise'} = 16;
		$parameters{'par_IBIS_max_rise'} = 116;
		$parameters{'par_IBIS_P_convFact'} = 7.1;
		$parameters{'par_SCW1_ICOR_probShot'} = 0.0001;
		$parameters{'par_SCW1_BIN_cleanTrk'} = 1;
		$parameters{'par_SCW1_BKG_I_method_cor'} = 1;

		$parameters{'par_SCW1_GTI_PICsIT'}="VETO ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS";
		$parameters{'par_SCW1_GTI_ISGRI'}="VETO ATTITUDE ISGRI_DATA_GAPS";
#BEO - 20071213 SCREW 2059
		#$parameters{'par_brSrcDOL'}= "$ENV{ISDC_REF_CAT}"."[ISGRI_FLAG==1 && ISGR_FLUX_1>100]",
		$parameters{'par_brSrcDOL'}= "",
		$parameters{'par_SCW1_BKG_P_method'}=1;
		$parameters{'par_SCW2_ISPE_MethodFit'}=6;
		$parameters{'par_SCW2_BKG_I_method_cor'}=1;
		$parameters{'par_SCW2_BKG_P_method'}=1 ;
				
        # Testing parameters for QLA with OSA9:
        $parameters{"par_startLevel"}       = "COR";
        $parameters{"par_endLevel"}         = "SPE";
        $parameters{"par_tolerance"}        = 0.1;
        $parameters{"par_IBIS_II_ChanNum"}      = 2;
        $parameters{"par_SCW1_ICOR_InterpLUT2"} = "no";
        $parameters{"par_SCW2_ISPE_idx_isgrResp"}   = "isgri_srcl_res_xte19_clean.fits";
        $parameters{"par_IBIS_SI_inEnergyValues"}   = "isgri_srcl_res_xte19_clean.fits[3]";
        $parameters{"par_OBS1_ToSearch"}        = 5;
        $parameters{"par_OBS1_MinCatSouSnr"}    = 5;
        $parameters{"par_OBS1_MinNewSouSnr"}    = 6;
        $parameters{"par_OBS1_NegModels"}       = 1;
        $parameters{"par_OBS1_ExtenType"}       = 2;
        $parameters{"par_ILCR_delta_t"}         = 500;
        $parameters{"par_ILCR_e_max"}           = "40 100";
        $parameters{"par_ILCR_e_min"}           = "20 40";

	}
	elsif ( $ENV{PROCESS_NAME} =~ /nqlscw/ ) {
		$parameters{'par_startLevel'}   = "COR";
		$parameters{'par_endLevel'}     = "IMA";
		$parameters{'par_OBS1_ToSearch'}     = "25";
		$parameters{'par_OBS1_DoPart2'}      = "1";
		$parameters{'par_IBIS_II_ChanNum'}   = "2";
		$parameters{'par_SCW1_BKG_I_isgrBkgDol'}  = "";
		$parameters{'par_SCW1_BKG_I_isgrUnifDol'} = "";

#	071127 - testing new qla parameters
		$parameters{'par_tolerance'} = 0.1;
		$parameters{'par_IBIS_min_rise'} = 16;
		$parameters{'par_IBIS_max_rise'} = 116;
		$parameters{'par_IBIS_P_convFact'} = 7.1;
		$parameters{'par_SCW1_ICOR_probShot'} = 0.0001;
		$parameters{'par_SCW1_BIN_cleanTrk'} = 1;
		$parameters{'par_SCW1_BKG_I_method_cor'} = 1;

		$parameters{'par_SCW1_GTI_PICsIT'}="VETO ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS";
		$parameters{'par_SCW1_GTI_ISGRI'}="VETO ATTITUDE ISGRI_DATA_GAPS";
#BEO 20071213 SCREW 2059
		#$parameters{'par_brSrcDOL'}= "$ENV{ISDC_REF_CAT}"."[ISGRI_FLAG==1 && ISGR_FLUX_1>100]",
		$parameters{'par_brSrcDOL'}= "",
		$parameters{'par_SCW1_BKG_P_method'}=1;
		$parameters{'par_SCW2_ISPE_MethodFit'}=6;
		$parameters{'par_SCW2_BKG_I_method_cor'}=1;
		$parameters{'par_SCW2_BKG_P_method'}=1 ;

        # Testing parameters for QLA with OSA9:
        $parameters{"par_startLevel"}       = "COR";
        $parameters{"par_endLevel"}         = "SPE";
        $parameters{"par_tolerance"}        = 0.1;
        $parameters{"par_IBIS_II_ChanNum"}      = 2;
        $parameters{"par_SCW1_ICOR_InterpLUT2"} = "no";
        $parameters{"par_SCW2_ISPE_idx_isgrResp"}   = "isgri_srcl_res_xte19_clean.fits";
        $parameters{"par_IBIS_SI_inEnergyValues"}   = "isgri_srcl_res_xte19_clean.fits[3]";
        $parameters{"par_OBS1_ToSearch"}        = 5;
        $parameters{"par_OBS1_MinCatSouSnr"}    = 5;
        $parameters{"par_OBS1_MinNewSouSnr"}    = 6;
        $parameters{"par_OBS1_NegModels"}       = 1;
        $parameters{"par_OBS1_ExtenType"}       = 2;
        $parameters{"par_ILCR_delta_t"}         = 500;
        $parameters{"par_ILCR_e_max"}           = "40 100";
        $parameters{"par_ILCR_e_min"}           = "20 40";
        
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csasw1/ ) {
		#	Is it ever going to be anything other than .001???
		$parameters{'par_ogDOL'} = &ISDCLIB::FindDirVers ( "scw/$att{scwid}" )."/swg_ibis.fits[GROUPING]";
		$parameters{'par_endLevel'}     = "BKG_I";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csaob1/ ) {
		$parameters{'par_startLevel'}   = "CAT_I";
		$parameters{'par_endLevel'}     = "IMA";
		$parameters{'par_OBS1_DoPart2'}      = "1";
		$parameters{'par_PICSIT_inCorVar'} = "0";			#	do I really need this as a variable?  Once this all works, fc!, ask NP.
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csasw2/ ) {
		#	Is it ever going to be anything other than .001???
		$parameters{'par_ogDOL'}        = &ISDCLIB::FindDirVers ( "scw/$att{scwid}" )."/swg_ibis.fits[GROUPING]";
		$parameters{'par_startLevel'}   = "IMA2";
		$parameters{'par_PICSIT_inCorVar'} = "0";			#	do I really need this as a variable?  Once this all works, fc!, ask NP.
	}
#	elsif ( $ENV{PROCESS_NAME} =~ /csaob2/ ) { #		Not yet.  #	}
	else {
		&Error ( "No match found for PROCESS_NAME: $ENV{PROCESS_NAME}; proctype: $att{proctype}\n" );
	}

	if ( $ENV{PROCESS_NAME} =~ /csa/ ) {		#	ALL conssa processes
		$parameters{'par_SCW1_GTI_PICsIT'}   = "VETO ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS";
		$parameters{'par_GENERAL_levelList'}    = "COR,GTI,DEAD,BIN_I,BKG_I,CAT_I,IMA,IMA2,BIN_S,SPE,LCR,COMP,CLEAN";
	}
	
	#####################################################################################

	if ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $ENV{REDO_CORRECTION} ) ) {	#	this is only available in consssa
		&Message ( "Redoing Correction step.  Changing startLevel and endLevel." );
		$parameters{'par_startLevel'}   = "COR";
		$parameters{'par_endLevel'}     = "DEAD";

		#	This is the parameter to use the Alberto LUT
		$parameters{'par_SCW1_ICOR_riseDOL'} = "/isdc/integration/linux_dist/addition/files/ibis_isgr_rt_corr.fits";
	}

	#####################################################################################

	if ( $att{INST} =~ /ISGRI|IBIS/ ) {
		$parameters{'par_CAT_refCat'} .= "[ISGRI_FLAG == 1]";	#	only unused for consssa of picsit
		if ( $ENV{PROCESS_NAME} =~ /cssscw/ ) {	
			$parameters{'par_rebinned_corrDol_ima'}     = "$ENV{REP_BASE_PROD}/$att{instdir}/rebinned_corr_ima.fits[1]";
			&ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/consssa/rebinned_corr_ima.fits.gz $ENV{REP_BASE_PROD}/$att{instdir}/" )
				unless ( -e "$ENV{REP_BASE_PROD}/$att{instdir}/rebinned_corr_ima.fits.gz" );
			unless ( -e "energy_bands.fits.gz" ) {
				&ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/consssa/energy_bands.fits.gz ./" );
				&Error ( "energy_bands.fits.gz does not exist!" ) unless ( -e "energy_bands.fits.gz" );
			}
		} elsif ( ( $ENV{PROCESS_NAME} =~ /csasw1/ ) && ( ! -e "energy_bands.fits.gz" ) ) {
			&ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/conssa/energy_bands.fits.gz ./" );
			&Error ( "energy_bands.fits.gz does not exist!" ) unless ( -e "energy_bands.fits.gz" );
		} elsif ( $ENV{PROCESS_NAME} =~ /nqlobs/ ) {	
			$parameters{'par_rebinned_corrDol_ima'}     = "$ENV{REP_BASE_PROD}/$att{instdir}/rebinned_corr_ima.fits[1]";
			&ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/nrtqla/rebinned_corr_ima.fits.gz $ENV{REP_BASE_PROD}/$att{instdir}/" )
				unless ( -e "$ENV{REP_BASE_PROD}/$att{instdir}/rebinned_corr_ima.fits.gz" );
			
			# Also copy files for OSA9 QLA tests:
			&ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/nrtqla/isgri_srcl_res_xte19_clean.fits $ENV{REP_BASE_PROD}/$att{instdir}/" )
                unless ( -e "$ENV{REP_BASE_PROD}/$att{instdir}/isgri_srcl_res_xte19_clean.fits" );
		} elsif ( $ENV{PROCESS_NAME} =~ /nqlscw/ ) {  
            # Also copy files for OSA9 QLA tests:
            &ISDCPipeline::RunProgram ( "$mycp $ENV{ISDC_OPUS}/nrtqla/isgri_srcl_res_xte19_clean.fits $ENV{REP_BASE_PROD}/$att{instdir}/" )
                unless ( -e "$ENV{REP_BASE_PROD}/$att{instdir}/isgri_srcl_res_xte19_clean.fits" );
		}
	}
	elsif ( $att{INST} =~ /PICSIT/ ) {
		$parameters{'par_SWITCH_disableIsgri'}   = "yes";
		$parameters{'par_SWITCH_disablePICsIT'}  = "no";
	}
	else {
		&Error ( "Unknown INST instrument : $att{INST}!" );
	}

	#####################################################################################

	if ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $ENV{MULTISTAGE} ) ) {	#	this is only available in consssa
		&Message ( "multi-stage testing.  Stage: $ENV{MULTISTAGE}.  Changing some parameters." );

		$parameters{'par_SCW1_GTI_attTolerance_X'} = "0.03"; 
		$parameters{'par_SCW1_GTI_attTolerance_Z'} = "0.1"; 
		$parameters{'par_OBS1_ExtenType'}          = "2";  
		$parameters{'par_tolerance'}               = "0.1";
		$parameters{'par_GENERAL_levelList'}       = "COR,GTI,DEAD,BIN_I,BKG_I,CAT_I,IMA,IMA2,BIN_S,SPE,LCR,COMP,CLEAN";
		$parameters{'par_IBIS_IPS_ChanNum'}        = "-1"; 
		$parameters{'par_IBIS_IPS_E_band_max_m'}   = "";   
		$parameters{'par_IBIS_IPS_E_band_max_s'}   = "";   
		$parameters{'par_IBIS_IPS_E_band_min_m'}   = "";   
		$parameters{'par_IBIS_IPS_E_band_min_s'}   = "";   
		$parameters{'par_SCW1_GTI_PICsIT'}         = "VETO ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS";
		$parameters{'par_SCW1_BKG_I_isgrUnifDol'}  = "";
		$parameters{'par_SCW1_BKG_I_isgrBkgDol'}   = "";   
		$parameters{'par_IBIS_min_rise'}           = "16"; 
		$parameters{'par_IBIS_max_rise'}           = "116";
		$parameters{'par_IBIS_P_convFact'}         = "7.1";
		$parameters{'par_IBIS_SPS_ChanNum'}        = "-1"; 
		$parameters{'par_SCW1_GTI_TimeFormat'}     = "IJD";
		$parameters{'par_SCW1_GTI_ISGRI'}          = "VETO ATTITUDE ISGRI_DATA_GAPS";
		$parameters{'par_SCW1_ICOR_probShot'}      = "0.0001";
		$parameters{'par_SCW1_BIN_cleanTrk'}       = "1";  
		$parameters{'par_SCW1_BKG_I_method_cor'}   = "1";  
		$parameters{'par_brSrcDOL'} = "$ENV{ISDC_REF_CAT}"."[ISGRI_FLAG==1 && ISGR_FLUX_1>100]";
		$parameters{'par_SCW1_BKG_P_method'}       = "1";  
		$parameters{'par_SCW1_BKG_picsSUnifDOL'}   = "-";  
		$parameters{'par_SCW1_BKG_picsMUnifDOL'}   = "-";  
		$parameters{'par_OBS1_MapAlpha'}           = "";   
		$parameters{'par_OBS1_MapDelta'}           = "";   
		$parameters{'par_OBS1_SouFit'}             = "1";  
		$parameters{'par_SCW2_ISPE_MethodFit'}     = "6";  
		$parameters{'par_SCW2_BKG_I_method_cor'}   = "1";  
		$parameters{'par_SCW2_BKG_P_method'}       = "1";  
		$parameters{'par_ILCR_num_e'}              = "4";  
		$parameters{'par_ILCR_e_min'}              = "20 40 60 100";
		$parameters{'par_ILCR_e_max'}              = "40 60 100 200";
		$parameters{'par_OBS2_projSel'}            = "STG";
		$parameters{'par_chatter'}                 = "4";  
        $parameters{'par_OBS1_ToSearch'}           = "0";
        $parameters{'par_OBS1_NegModels'}          = "1";
        $parameters{'par_OBS1_MinCatSouSnr'}       = "5";
        $parameters{'par_OBS1_MinNewSouSnr'}       = "0";

		$parameters{'par_startLevel'} = $ENV{ISGRI_STARTLEVEL} if ( $ENV{ISGRI_STARTLEVEL} );
		$parameters{'par_endLevel'}   = $ENV{ISGRI_ENDLEVEL}   if ( $ENV{ISGRI_ENDLEVEL} );
	}

	#####################################################################################

	if ( $att{proctype} =~ /scw/ ) {
		#	Check to ensure that only 1 child on OG
		my $numScwInOG = &ISDCLIB::ChildrenIn ( $parameters{'par_ogDOL'}, "GNRL-SCWG-GRP-IDX" );
		&Error ( "Not 1 child in $parameters{'par_ogDOL'}: $numScwInOG" ) unless ( $numScwInOG == 1 );
	}

	print "\n========================================================================\n";
	print "#######     DEBUG:  ISDC_REF_CAT is $ENV{ISDC_REF_CAT}\n";

	my $dal_retry_open_existed = $ENV{DAL_RETRY_OPEN} if ( exists $ENV{DAL_RETRY_OPEN} );
	$ENV{DAL_RETRY_OPEN} = 5 unless ( exists $ENV{DAL_RETRY_OPEN} );		#	060208 - Jake - SCREW 1807
#	my $dal_retry_open_existed = $ENV{DAL_RETRY_OPEN} if ( defined ( $ENV{DAL_RETRY_OPEN} ) );
#	$ENV{DAL_RETRY_OPEN} = 5 unless ( defined ( $ENV{DAL_RETRY_OPEN} ) );		#	060208 - Jake - SCREW 1807

	my ($retval,@result) = &ISDCPipeline::PipelineStep (
		"step"                        => "$proc - IBIS Analysis $att{proctype}",
		"program_name"                => "ibis_science_analysis",
		%parameters,
		);

	if ($retval) {         #       071204 - Jake - SCREW 997
		if ($retval =~ /35644/) {
			&Message ( "$proc - WARNING:  no data;  continuing." );
		} else {
			print "*******     ERROR:  return status of $retval from ibis_science_analysis not allowed.\n";
			exit 1;      
		}
	} # if error

	delete $ENV{DAL_RETRY_OPEN} unless ( defined ( $dal_retry_open_existed ) );		#	060208 - Jake - SCREW 1807
	$ENV{DAL_RETRY_OPEN} = $dal_retry_open_existed if ( defined ( $dal_retry_open_existed ) );

	&ISDCPipeline::RunProgram("$myrm GNRL-REFR-CAT.fits")
		if ( ( -e "GNRL-REFR-CAT.fits" ) && ( $ENV{PROCESS_NAME} =~ /cssscw|csaob1/ ) );

	&Message ( "COMMONLOGFILE :$ENV{COMMONLOGFILE}:" );

	return;
} # end of ISA

########################################################################

=item B<IPMosaic> ( )

Currently only called from the consssa pipeline when running PICSiT mosaics.

=cut

sub IPMosaic {

	&Carp::croak ( "IBISLIB::IPMosaic: Need even number of args" ) if ( @_ % 2 );
	
	#	my %att = @_;	#	no pars just yet

	my $proc = "CSS (Mosaic) PICSIT";
	print "\n========================================================================\n";
	
	&ISDCPipeline::PipelineStep (
		"step"           => "$proc - cat_extract",
		"program_name"   => "cat_extract",
		"par_refCat"     => "$ENV{ISDC_REF_CAT}",
		"par_instrument" => "PICSIT",
		"par_inGRP"      => "og_ibis.fits[GROUPING]",
		"par_outGRP"     => "-",
		"par_outCat"     => "isgri_catalog.fits(ISGR-SRCL-CAT.tpl)",
		"par_outExt"     => "ISGR-SRCL-CAT",
		"par_date"       => "-1.",
		"par_radiusMin"  => "0",
		"par_radiusMax"  => "20",
		"par_fluxDef"    => "",
		"par_fluxMin"    => "",
		"par_fluxMax"    => "",
		"par_class"      => "",
		"par_clobber"    => "yes",
		);

	&ISDCPipeline::PipelineStep (
		"step"             => "$proc - IBIS PICSIT Image Mosaic",
		"program_name"     => "ip_skymosaic",
		"par_outOG"        => "og_ibis.fits[GROUPING]",
		"par_outMosaic"    => "pics_mosa_ima2.fits(PICS-MOSA-IMA-IDX.tpl)",
		"par_outPicsitCat" => "pics_mosa_res2.fits(PICS-MOSA-RES-IDX.tpl)",
		"par_inCat"        => "isgri_catalog.fits[1]",
		"par_detThr"       => "3.0",
		"par_imgSel"       => "EVT_TYPE=='SINGLE' && E_MIN==252 && E_MAX==336",
		"par_projSel"      => "STG",	#	070625 - Jake - SCREW 2003
#		"par_projSel"      => "-TAN",
		"par_inOG"         => "",
		"par_idxScw"       => "",
		"par_mode"         => "h",
		);
}	#	end IPMosaic
########################################################################

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
