package JMXLIB;

=head1 NAME

I<JMXLIB.pm> - library used by nrtqla, conssa and consssa

=head1 SYNOPSIS

use I<JMXLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

sub JMXLIB::JSA;

$| = 1;

=item B<JSA> ( %att )

=cut

sub JSA {
    my %att = @_;
    $att{proctype}      = "scw" unless $att{proctype} =~ /mosaic/;
    $att{IC_Group}      = "../../idx/ic/ic_master_file.fits[1]" unless ( $att{IC_Group} );
    
    my $proc            = &ProcStep();
    
    &Error ( "*******     ERROR:   JMXLIB::JSA doesn't recognize given JEMX number $att{jemxnum}!\n" )
	unless ( $att{jemxnum} =~ /^(1|2)$/ );
    
    my %parameters = (
		"par_ogDOL"                 => "og_jmx$att{jemxnum}.fits[GROUPING]",
		"par_jemxNum"               => "$att{jemxnum}",
		"par_startLevel"            => "COR",
		"par_endLevel"              => "LCR",
		"par_skipLevels"            => "BKG,SPE",
		"par_nChanBins"             => "5",
		"par_chanHigh"              => "57 118 159 196 235",
		"par_chanLow"               => "30  58 119 160 197",
		"par_CAT_I_radiusMin"       => "0.0 2.4",
		"par_CAT_I_radiusMax"       => "2.4 6.6",
		"par_CAT_I_refCat"          => "$ENV{ISDC_REF_CAT}",
		"par_CAT_I_usrCat"          => "",
		"par_GTI_TimeFormat"        => "IJD",
		"par_IC_Alias"              => "$ENV{IC_ALIAS}",
		"par_IC_Group"              => "$att{IC_Group}",
		"par_IMA_skyImagesOut"      => "RECTI,VARIA,RESID,RECON",
		"par_IMA2_cdelt"            => "0.03",
		"par_IMA2_diameter"         => "20.",
		"par_IMA2_mapSelect"        => "RECTI",
		"par_IMA2_radiusSelect"     => "5.",
		"par_IMA2_viewIntens"       => "Y",
		"par_IMA2_viewSig"          => "Y",
		"par_IMA2_viewTime"         => "N",
		"par_IMA2_viewVar"          => "N",
		"par_LCR_vignCorr"          => "yes",
		"par_BIN_I_rowSelect"       => "",
		"par_BIN_I_evtType"         => "-1",
		"par_BIN_I_gtiNames"        => "",
		"par_BIN_I_shdRes"          => "",
		"par_BIN_S_evtType"         => "-1",
		"par_BIN_S_rowSelectEvts"   => "",
		"par_BIN_S_rowSelectSpec"   => "",
		"par_BIN_T_evtType"         => "-1",
		"par_BIN_T_rowSelect"       => "",
		"par_CAT_I_class"           => "",
		"par_CAT_I_date"            => "-1",
		"par_CAT_I_fluxDef"         => "0",
		"par_CAT_I_fluxMax"         => "",
		"par_CAT_I_fluxMin"         => "",
		"par_COR_gainHist"          => "",
		"par_COR_gainModel"         => "2",
		"par_COR_outputExists"      => "n",
		"par_DEAD_outputExists"     => "n",
		"par_GTI_Accuracy"          => "any",
		"par_GTI_BTI_Dol"           => "",
		"par_GTI_BTI_Names"         => "",
		"par_GTI_MergedName"        => "MERGED",
		"par_GTI_attTolerance"      => "0.05",
		"par_GTI_gtiJemxNames"      => "",
		"par_GTI_gtiScNames"        => "",
		"par_GTI_gtiUser"           => "",
		"par_GTI_limitTable"        => "",
		"par_IMA2_srcFileDOL"       => "",
		"par_IMA2_srcattach"        => "y",
		"par_IMA2_srcselect"        => "",
		"par_IMA_distFuzz"          => "0.0",
		"par_IMA_gridNum"           => "0",
		"par_IMA_relDist"           => "-0.05",
		"par_IMA_searchRad"         => "0.0",
		"par_IMA_fluxLimit"         => "0.000",
		"par_LCR_evtType"           => "-1",
		"par_LCR_fluxScaling"       => "2",
		"par_LCR_precisionLevel"    => "20",
		"par_LCR_rowSelect"         => "",
		"par_LCR_skipHotSpot"       => "n",
		"par_LCR_skipNearDeadAnode" => "y",
		"par_LCR_tAccuracy"         => "3",
		"par_LCR_timeStep"          => "-1",
		"par_LCR_useRaDec"          => "y",
		"par_chatter"               => "2",
		"par_clobber"               => "y",
		"par_ignoreScwErrors"       => "n",
		"par_instMod"               => "",
		"par_nPhaseBins"            => "0",
		"par_osimData"              => "n",
		"par_phaseBins"             => "",
		"par_radiusLimit"           => "122",
		"par_response"              => "",
		"par_timeStart"             => "-1.0",
		"par_timeStop"              => "-1.0",
		"par_IMA2_DECcenter"        => "0.",
		"par_IMA2_RAcenter"         => "-1",
		"par_IMA2_emaxSelect"       => "80.",
		"par_IMA2_eminSelect"       => "0.",
		"par_IMA2_outfile"          => "J_MOSAIC",
		"par_IMA_bkgShdDOL"         => "",
		"par_IMA_detAccLimit"       => "16384",
		"par_IMA_detSigSingle"      => "12.0",
		"par_IMA_dolBPL"            => "",
		"par_IMA_edgeEnhanceFactor" => "1.0",
		"par_IMA_hotPixelLimit"     => "4.0",
		"par_IMA_loopLimitPeak"     => "0.025",
		"par_IMA_makeNewBPL"        => "no",
		"par_IMA_maxNumSources"     => "10",
		"par_IMA_newBackProjFile"   => "",
		"par_IMA_radiusLimit0"      => "120.0",
		"par_IMA_radiusLimit1"      => "120.0",
		"par_IMA_radiusLimit2"      => "117.0",
		"par_IMA_radiusLimit3"      => "110.0",
		"par_IMA_skyImageDim"       => "2",
		"par_IMA_skyRadiusFactor"   => "1.0",
		"par_IMA_useDeadAnodes"     => "no",
		"par_LCR_overrideCollTilt"  => "-1.0",
		"par_GTI_AttStability_Z"    => "0.2",
		"par_IMA2_dolBPL"           => "",
		"par_IMA_collHreduc"        => "0.0",
		"par_IMA_illumNorm"         => "0",
		"par_IMA_signifLim"         => "25",
		"par_COR_randPos"           => "n",
		"par_BIN_I_chanHighDet"     => "95 134 178",
		"par_BIN_I_chanLowDet"      => "46 96 135",
		"par_IMA_detImagesOut"      => "y",
		"par_IMA2_print_ScWs"       => "N",
		"par_BIN_I_shdType"         => "2",
		"par_IMA_interactionDepth"  => "3.0",
		"stoponerror" => "0"
	);

	if ( $ENV{PATH_FILE_NAME} =~ /consssa/ ) {
	    $parameters{'par_endLevel'} = "IMA";
	    $parameters{'par_nChanBins'}       = "10";
	    $parameters{'par_chanLow'}         = "46 59 77 102 130 153 175 199 46 130";
	    $parameters{'par_chanHigh'}        = "58 76 101 129 152 174 198 223 129 223";
	    $parameters{'par_IMA_skyImagesOut'} = "RECONSTRUCTED,VARIANCE,EXPOSURE";
	    $parameters{'par_IMA_detImagesOut'} = "no";
	    $parameters{'par_IMA_userImagesOut'} = "yes";
	} elsif ( $ENV{PATH_FILE_NAME} =~ /conssa/ ) {
	    $parameters{'par_IMA2_cdelt'}           = "0.02";
	    $parameters{'par_IMA2_radiusSelect'}    = "4.8";
	} elsif ( $ENV{PATH_FILE_NAME} =~ /nrtqla/ ) {
	    $parameters{'par_GTI_TimeFormat'}  = "OBT";
	    $parameters{'par_LCR_vignCorr'}    = "no";
	    $parameters{'par_nChanBins'}       = "2";
	    $parameters{'par_chanLow'}         =  "46 129";
	    $parameters{'par_chanHigh'}        = "128 211";
	    $parameters{'par_BIN_I_rowSelect'} = "&& STATUS < 16";
	} else {
	    &Error ( "No match found for PATH_FILE_NAME: $ENV{PATH_FILE_NAME}; PROCESS_NAME: $ENV{PROCESS_NAME}; proctype: $att{proctype}\n" );
	}
	
	
	if ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $att{proctype} =~ /scw/ ) ) {
		$parameters{'par_CAT_I_radiusMax'} = "2.4 5.5";
		$parameters{'par_CAT_I_usrCat'}    = "jmx$att{jemxnum}_catalog.fits";
		
		&ISDCPipeline::PipelineStep (
			"step"           => "$proc - cat_extract",
			"program_name"   => "cat_extract",
			"par_refCat"     => "$ENV{ISDC_REF_CAT}"."[JEMX_FLAG==1]",
			"par_instrument" => "JMX"."$att{jemxnum}",
			"par_inGRP"      => "",
			"par_outGRP"     => $parameters{'par_ogDOL'},
			"par_outCat"     => "$parameters{'par_CAT_I_usrCat'}(JMX$att{jemxnum}-SRCL-CAT.tpl)",
			"par_outExt"     => "JMX$att{jemxnum}-SRCL-CAT",
			"par_date"       => "-1.",
			"par_radiusMin"  => $parameters{'par_CAT_I_radiusMin'},
			"par_radiusMax"  => $parameters{'par_CAT_I_radiusMax'},
			"par_fluxDef"    => "0",
			"par_fluxMin"    => "",
			"par_fluxMax"    => "",
			"par_class"      => "",
			"par_clobber"    => "yes",
			);
		
		my $original_pfiles   = $ENV{PFILES};
		my $original_parfiles = $ENV{PARFILES};
		$ENV{PARFILES} = "/tmp/$$-fcalcpfile/";
		system ( "mkdir $ENV{PARFILES}" );

		&ISDCPipeline::PipelineStep (
			"step"           => "$proc - fcalc",
			"program_name"   => "fcalc",
			"par_infile"     => $parameters{'par_CAT_I_usrCat'},
			"par_outfile"    => $parameters{'par_CAT_I_usrCat'},
			"par_clname"     => "FLAG",
			"par_expr"       => "1",
			"par_clobber"    => "yes",
			);

		system ( "/bin/rm -rf $ENV{PARFILES}" );
		$ENV{PARFILES} = $original_parfiles;
		$ENV{PFILES}   = $original_pfiles;
		$parameters{'par_CAT_I_usrCat'}       .= "[1]";
	}
	elsif ( ( $ENV{PROCESS_NAME} =~ /cssscw/ ) && ( $att{proctype} =~ /mosaic/ ) ) {
	    $parameters{'par_startLevel'}      = "IMA2";
	    $parameters{'par_endLevel'}        = "IMA2";
	    $parameters{'par_IMA2_mapSelect'}    = "RECON";
	    $parameters{'par_IMA2_radiusSelect'} = "4.8";
	    $parameters{'par_IMA2_diameter'}     = "-1";
	    $parameters{'par_IMA_skyImagesOut'}  = "RAWIN,RECTI,VARIA,RESID,RECON";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /nqlobs/ ) {
	    $parameters{'par_startLevel'}      = "IMA2";
	    $parameters{'par_endLevel'}        = "IMA2";
	    $parameters{'par_GTI_TimeFormat'}  = "OBT";
	    $parameters{'par_LCR_vignCorr'}    = "no";
	    $parameters{'par_nChanBins'}       = "2";
	    $parameters{'par_chanLow'}         =  "46 129";
	    $parameters{'par_chanHigh'}        = "128 178";
	    $parameters{'par_skipLevels'}      = "";
	    $parameters{'par_CAT_I_radiusMax'} = "2.4 5.8";
	    $parameters{'par_BIN_I_rowSelect'} = "&& STATUS < 256";
	    $parameters{'par_IMA_skyImagesOut'}= "RECONSTRUCTED,VARIANCE,SIGNIFICANCE";
	    $parameters{'par_IMA_relDist'}     = "1.5";
	    $parameters{'par_IMA_searchRad'}   = "5.00";
	    $parameters{'par_IMA_gridNum'}     = 10;
	    $parameters{'par_IMA_distFuzz'}    = 0.15;
	    $parameters{'par_IMA_detImagesOut'} = 'no ';
	    $parameters{'par_IMA2_mapSelect'} = "RECON";
	    $parameters{'par_IMA2_radiusSelect'} = "4.8";
	    $parameters{'par_IMA2_diameter'} = "0.0";
	    $parameters{'par_IMA2_cdelt'} = "0.026" ;
	    $parameters{'par_IMA2_viewTime'} = "Y";
	    $parameters{'par_IMA2_viewVar'} = "Y";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /nqlscw/ ) {
	    $parameters{'par_skipLevels'}      = "BKG";
	    $parameters{'par_endLevel'}        = "IMA";
	    $parameters{'par_GTI_TimeFormat'}  = "OBT";
	    $parameters{'par_LCR_vignCorr'}    = "no";
	    $parameters{'par_nChanBins'}       = "2";
	    $parameters{'par_chanLow'}         =  "46 129";
	    $parameters{'par_chanHigh'}        = "128 178";
	    $parameters{'par_IMA2_viewTime'}   = "Y";
	    $parameters{'par_IMA2_viewVar'}    = "Y";
	    $parameters{'par_skipLevels'}  = "";
	    $parameters{'par_CAT_I_radiusMax'} = "2.4 5.8";
	    $parameters{'par_BIN_I_rowSelect'} = "&& STATUS < 256";
	    $parameters{'par_BIN_S_timeStep'} = 0;
	    $parameters{'par_IMA_skyImagesOut'} = "RECONSTRUCTED,VARIANCE,SIGNIFICANCE";
	    $parameters{'par_IMA_relDist'} = "1.5";
	    $parameters{'par_IMA_searchRad'} = "5.00";
	    $parameters{'par_IMA_gridNum'} = 10;
	    $parameters{'par_IMA_distFuzz'} = 0.15;
	    $parameters{'par_IMA_detImagesOut'} = 'no';
	    $parameters{'par_IMA_pixelFold'} = 1;
	    $parameters{'par_IMA_userImagesOut'} = 'yes';
	    $parameters{'par_IMA_useTrace'} = 'no';
	    $parameters{'par_IMA_tracestring'} = '0123456789ABCDEF';
	    $parameters{'par_IMA2_mapSelect'} = "RECON";
	    $parameters{'par_IMA2_radiusSelect'} = "4.8";
	    $parameters{'par_IMA2_diameter'} = "0.0";
	    $parameters{'par_IMA2_cdelt'} = "0.026" ;
	    $parameters{'par_COR_osmCnv'} = "";
	    $parameters{'par_COR_osmRaw'} = "";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csaob1/ ) {
	    $parameters{'par_startLevel'}      = "IMA2";
	    $parameters{'par_endLevel'}        = "IMA2";
	    $parameters{'par_skipLevels'}      = "";
	    $parameters{'par_IMA2_viewTime'}     = "Y";
	    $parameters{'par_IMA2_viewVar'}      = "Y";
	}
	elsif ( $ENV{PROCESS_NAME} =~ /csasw1/ ) {
	    $parameters{'par_ogDOL'} = &ISDCLIB::FindDirVers ( "scw/$att{scwid}" )."/swg_jmx$att{jemxnum}.fits[GROUPING]";
	}
	else {
		&Error ( "No match found for PROCESS_NAME: $ENV{PROCESS_NAME}; proctype: $att{proctype}\n" );
	}

	#####################################################################################

	if ( $ENV{REDO_CORRECTION} ) {
	    &Message ( "Redoing Correction step." );
	    $parameters{'par_startLevel'}     = "COR";
	    $parameters{'par_endLevel'}       = "DEAD";
	}
	
	if ( $att{proctype} =~ /scw/ ) {
	    #	Check to ensure that only 1 child in OG
	    my $numScwInOG = &ISDCLIB::ChildrenIn ( $parameters{'par_ogDOL'}, "GNRL-SCWG-GRP-IDX" );
	    &Error ( "Not 1 child in $parameters{'par_ogDOL'}: $numScwInOG" ) unless ( $numScwInOG == 1 );
	}

	print "\n========================================================================\n";
	print "#######     DEBUG:  ISDC_REF_CAT is $ENV{ISDC_REF_CAT}.\n";

	my ($retval,@result) = &ISDCPipeline::PipelineStep(
		"step"                      => "$proc - JMX $att{proctype}",
		"program_name"              => "jemx_science_analysis",
		%parameters
		);
	
	if ($retval) {
		if ($retval =~ /382099|35644/) {
			&Message ( "WARNING:  no data;  skipping indexing." );
		}
		else {
			print "*******     ERROR:  return status of $retval from jemx_science_analysis not allowed.\n";
			exit 1;      
		}
	} # if error

	return;

} # end of JSA

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
