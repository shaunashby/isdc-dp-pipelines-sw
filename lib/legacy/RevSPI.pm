package RevSPI;

=head1 NAME

RevSPI.pm - NRT Revolution File Pipeline SPI Module

=head1 SYNOPSIS

use I<RevSPI.pm>;
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

sub RevSPI::SPICal;
sub RevSPI::DPspec;
sub RevSPI::PSD;


$| = 1;

##########################################################################

=item B<SPICal> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the SPI Calibration

=cut

sub SPICal {		
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my ($retval,@result);

	&Message ( "Starting the SPI Calibration." );

	my $revstart = "";
	my $revstop  = "";
        
	#       cannot use ISDCPipeline::ConvertTime bc REVNUM returns 2 needed numbers
	($retval,@result) = &ISDCPipeline::PipelineStep(              
		"step"          => "$proc - convert REVNUM $revno to IJD",
		"program_name"  => "converttime",
		"par_informat"  => "REVNUM",
		"par_intime"    => "$revno",
		"par_outformat" => "IJD",
		"par_dol"       => "",
		"par_accflag"   => "3",
		);
	&Error ( "Converttime failed." ) if ( $retval ); 	
        
	foreach (@result) { 
		next unless /^.*.IJD.:\s+Boundary\s+(\S+)\s+(\S+)\s*$/i;
		$revstart = $1;
		$revstop  = $2;
		last;					#	grab the first match (there can be only one)
	}

	#	040608 - Jake - A single time for the entire revolution may be a problem.
	#	I choose to use $revstart to begin with.  Will try the other if there are problems.
	my $revtime  = $revstart;
        
	my $spigaincfgDOL = &ISDCPipeline::GetICFile (
		"structure" => "SPI.-GAIN-CFG",
		"select"    => "( VSTART <= $revtime ) && (VSTOP >= $revtime)",
		);
	&Error ( "No IC file SPI.-GAIN-CFG with ( VSTART <= $revtime ) && (VSTOP >= $revtime) found." ) 
		unless ($spigaincfgDOL);	

	#	Log_1   : Task dal_dump running in SINGLE mode
	#	Log_1   : Beginning parameters
	#	Log_1   : Parameter inDol = 
	#		/isdc/integration/isdc_int/sw/dev/prod/opus/nrtrev/unit_test/test_data/ic/spi/cfg/spi_gain_cfg_0005.fits[SPI.-GAIN-CFG,1,BINTABLE]
	#	Log_1   : Parameter column = USE_SE
	#	Log_1   : Parameter outFormat = 1
	#	Log_1   : Ending parameters
	#	Log_1   : Running in scripting mode, no parameter prompting
	#	Log_1   :    1
	#	Log_1   : Task dal_dump terminating with status 0

	my %se_pars;
	foreach ( "SE", "PE", "ME", "AON", "AOFF", "CRVE" ) {
		chomp ( my $tempval = &ISDCLIB::GetColumn ( "$spigaincfgDOL", "USE_$_" ) );
		$se_pars{$_} = ( $tempval == 0 ) ? "no" : "yes";		#	Sweet!

#		my $initialcommonlogfile = $ENV{COMMONLOGFILE};
#		$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} 
#			unless ( $ENV{COMMONLOGFILE} =~ /^\+/ );    #  040608 - Jake - ( caused by SCREW 1437 )
#		($retval, @result) = &ISDCPipeline::RunProgram ( 
#			#	040608 - Jake - MUST quote the DOL
#			"dal_dump inDol=\"$spigaincfgDOL\" column=USE_$_ outFormat=1"
#			);
#		$ENV{COMMONLOGFILE} = $initialcommonlogfile;
#		foreach my $line ( @result ) {
#			chomp $line;
#			next unless ( $line =~ /^\s*Log_1\s*:\s*(\d+)\s*$/ );				#	040608 - Jake - this could be better but ...
#			&Error ( "ERROR examining output; cannot determine value (should be 0 or 1):  \n@result" ) 
#				unless (( $1 == 0 ) || ( $1 == 1 ));
#			$se_pars{$_} = ( $1 == 0 ) ? "no" : "yes";		#	Sweet!
#			last;
#		}
#		&Error ( "ERROR examining output; No value found for $_:  \n@result" ) 
#			unless ( $se_pars{$_} );
	}

	chdir "$workdir";		#	040610 - Jake - must manually create a temp index as it does not exist
	chomp ( $workdir = `pwd` );
	open SCW_LIST, "> tmp_working_scws_list" 
		or &Error ( "Could not open tmp_working_scws_list." );
#		or die "*******     ERROR:  Could not open tmp_working_scws_list";
	foreach my $scwid ( `$myls $ENV{SCWDIR}/$revno/$revno*/swg.fits` ) {
		chomp $scwid;
		print SCW_LIST "$scwid"."[1]\n";
	}
	close SCW_LIST;

	`$mychmod -w $ENV{SCWDIR}/$revno/$revno*/swg.fits`; 		#	040729 - Jake - SPR 3793
	&ISDCPipeline::RunProgram (									#	040624 - Jake - SPR 3732
		"txt2idx index=$workdir/tmp_working_scws_index.fits template=GNRL-SCWG-GRP-IDX.tpl element=tmp_working_scws_list"
		#	"txt2idx index=tmp_working_scws_index.fits template=GNRL-SCWG-GRP-IDX.tpl element=tmp_working_scws_list"
		);
	`$mychmod +w $ENV{SCWDIR}/$revno/$revno*/swg.fits`;
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - spi_gain_hist for SE.",
		"program_name" => "spi_gain_hist",
		"par_inGrpDOL" => "$workdir/tmp_working_scws_index.fits[1]",
		"par_inGtiDOL" => "",
		"par_outDOL"   => "aca/spi_cal_se_spectra.fits",
		"par_minOBT"   => "",
		"par_maxOBT"   => "",
		"par_append"   => "no",
		"par_slice"    => "no",
		"par_nopart"   => "no",
		"par_ontime"   => "14400",
		"par_useSE"    =>   "$se_pars{SE}",
		"par_usePE"    =>   "$se_pars{PE}",
		"par_useCRVE"  => "$se_pars{CRVE}",
		"par_useME"    =>   "$se_pars{ME}",
		"par_useAON"   =>  "$se_pars{AON}",
		"par_useAOFF"  => "$se_pars{AOFF}",
		"par_clobber"  => "yes",
		"par_verbose"  => "3",			#	070222 - Jake - temp change from 3 to 4 for Bruce's testing
		);

	if ( !-e "aca/spi_cal_se_spectra.fits" ) { 
		&Message ( "WARNING:  no results from spi_gain_hist for SE;  skipping remaining SPI Calibration steps" );
		return;
	}
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - spi_gain_hist for ME.",
		"program_name" => "spi_gain_hist",
		"par_inGrpDOL" => "$workdir/tmp_working_scws_index.fits[1]",
		"par_inGtiDOL" => "",
		"par_outDOL"   => "aca/spi_cal_me_spectra.fits",
		"par_minOBT"   => "",
		"par_maxOBT"   => "",
		"par_append"   => "no",
		"par_slice"    => "no",
		"par_nopart"   => "no",
		"par_ontime"   => "14400",
		"par_useSE"    => "no",
		"par_usePE"    => "no",
		"par_useCRVE"  => "no",
		"par_useME"    => "yes",
		"par_useAON"   => "no",
		"par_useAOFF"  => "no",
		"par_clobber"  => "yes",
		"par_verbose"  => "3",			#	070222 - Jake - temp change from 3 to 4
		);

	my $se_linesDOL =  &ISDCPipeline::GetICFile(
		"structure" => "SPI.-LINE-SCT",
#		"select" => "EVT_TYPE == 'SINGLE'",
		"select" => "EVT_TYPE == 'SE'",			#	change me back to ....
		);
#	&Error ( "No IC file SPI.-LINE-SCT with EVT_TYPE == 'SINGLE' found." ) unless ($se_linesDOL);	
	&Error ( "No IC file SPI.-LINE-SCT with EVT_TYPE == 'SE' found." ) unless ($se_linesDOL);	

#	my $se_lines = $se_linesDOL;
#	$se_lines =~ s/^(.*)fits\[.*$/$1fits/;
#	my ( $se_lines ) = ( $se_linesDOL =~ /^(.*)fits\[.*$/ )."fits";
	( my $se_lines = $se_linesDOL ) =~ s/^(.*)fits\[.*$/$1fits/;

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - spi_line_fit - single + PSD event spectra",
		"program_name"  => "spi_line_fit",
		"par_inSctDOL"  => "$se_linesDOL",
		"par_inGtiDOL"  => "aca/spi_cal_se_spectra.fits",
		"par_inDspDOL"  => "aca/spi_cal_se_spectra.fits",
		"par_outSrtDOL" => "aca/spi_cal_se_results.fits",
		"par_ptid"      => "0-1000",
		"par_clobber"   => "yes",
		"par_verbose"   => "3",
		);

	my $me_linesDOL =  &ISDCPipeline::GetICFile(
		"structure" => "SPI.-LINE-SCT",
#		"select"    => "EVT_TYPE == 'DOUBLE'",
		"select"    => "EVT_TYPE == 'ME'",
		);
#	&Error ( "No IC file SPI.-LINE-SCT with EVT_TYPE == 'DOUBLE' found." ) unless ($me_linesDOL);
	&Error ( "No IC file SPI.-LINE-SCT with EVT_TYPE == 'ME' found." ) unless ($me_linesDOL);

#	my $me_lines = $me_linesDOL;
#	$me_lines =~ s/^(.*)fits\[.*$/$1fits/;
#	my ( $me_lines ) = ( $me_linesDOL =~ /^(.*)fits\[.*$/ )."fits";
	( my $me_lines = $me_linesDOL ) =~ s/^(.*)fits\[.*$/$1fits/;

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - spi_line_fit - multiple event spectra",
		"program_name"  => "spi_line_fit",
		"par_inSctDOL"  => "$me_linesDOL",
		"par_inGtiDOL"  => "aca/spi_cal_me_spectra.fits",
		"par_inDspDOL"  => "aca/spi_cal_me_spectra.fits",
		"par_outSrtDOL" => "aca/spi_cal_me_results.fits",
		"par_ptid"      => "0-1000",
		"par_clobber"   => "yes",
		"par_verbose"   => "3",
		);

#	SCREW 1775
#
#	from
#
#	- pha0eng,s,q,"23.438 198.392 309.88 584.54 882.51 1764.4",,,"Energies of the lines selected for the low-energy range"  
#	- pha1eng,s,q,"2223.27 2754.03",,,"Energies of the lines selected for the high-energy range"  
#	
#	to	( these are apparently bad, so not using )
#	
#	- pha0eng,s,q,"23.438 198.392 309.88 438.619 882.51 1764.4",,,"Energies of the lines selected for the low-energy range"  
#	- pha1eng,s,q,"2754.03 6128.63",,,"Energies of the lines selected for the high-energy range" 

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - spi_gain_result",
		"program_name"  => "spi_gain_result",
		"par_sctNameSE" => "$se_lines",
		"par_sctNameME" => "$me_lines",
		"par_srtNameSE" => "aca/spi_cal_se_results.fits",
		"par_srtNameME" => "aca/spi_cal_me_results.fits",
		"par_gainAscii" => "spi_gain_result.tmp",
		"par_tstdmp"    => "0",
		"par_etol"      => "1.0",
#	Old
		"par_pha0eng"   => "23.438 198.392 309.88 584.54 882.51 1764.4",
# BEO 25/2/2009 SCREW 1775, uncomment below		"par_pha1eng"   => "2223.27 2754.03",

#	New (apparently bad)
#		"par_pha0eng"   => "23.438 198.392 309.88 438.619 882.51 1764.4",
		"par_pha1eng"   => "2754.03 6128.63",
		);

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - spi_gain_calib",
		"program_name"  => "spi_gain_calib",
		"par_gainAscii" => "spi_gain_result.tmp",
		"par_gainDOL"   => "aca/spi_gain_coeff",
		"par_chkevdeg0" => "4",
		"par_kevchdeg0" => "4",
		"par_chkevdeg1" => "2",
		"par_kevchdeg1" => "2",
		"par_calfct0"   => "1",
		"par_calfct1"   => "0",
		"par_fitopt"    => "yes",
		"par_tstdmp"    => "0",
		"par_engerr"    => "0.05",
		);

	return;
} # end SPICal


##########################################################################

=item B<DPspec> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for spi_raw_specoff files:

=cut

sub DPspec {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;


#	my $onoff = $dataset;
#	$onoff =~ s/.*(off|on).*/$1/;
	( my $onoff = $dataset ) =~ s/.*(off|on).*/$1/;


	my $ONOF = $onoff;
	$ONOF =~ s/off/of/;
	$ONOF =~ tr/a-z/A-Z/;
#	my $tpl = "SPI.-AC".$ONOF;
#	$tpl .= "-GRP.tpl";
	my $tpl = "SPI.-AC$ONOF-GRP.tpl";
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect raw file",
		"program_name" => "$mychmod -w raw/$dataset",
		"subdir"       => "$workdir",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect raw file",
		"program_name" => "$mychmod -w raw/$dataset",
		"subdir"       => "$workdir",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - SPI create spec group",
		"program_name" => "dal_create",
		"par_obj_name" => "prp/spi_prp_ac${onoff}_spectra_$stamp",
		"par_template" => "$tpl",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - SPI attach raw to group",
		"program_name" => "dal_attach",
		"par_Parent"   => "prp/spi_prp_ac${onoff}_spectra_$stamp.fits[GROUPING]",
		"par_Child1"   => "raw/$dataset"."[SPI.-AC$ONOF-CRW]",
		"par_Child2"   => "",
		"par_Child3"   => "",
		"par_Child4"   => "",
		"par_Child5"   => "",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - SPI spectra OBT",
		"program_name" => "spi_spec_obt_calc",
		"par_accuracy" => "ANY",
		"par_swgDOL"   => "",
		);
	
	return;
} # end of DPspec


#############################################################################

=item B<PSD> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

SPI PSD analysis

=cut

sub PSD {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my ($retval,@result);
	my $limits;
	my $nopart;
	my $coeff;
	
	#  Get index of science windows PRP
	if (!-e "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits") {
		&Message ( "No PRP science window index found;  on hold" );
		exit 5;
	}  # if scw index doesn't exist
	
	&ISDCPipeline::FindIndex(
		"index"    => "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits",
		"select"   => "REVOL == $revno",
		"workname" => "working_prpscws_index.fits",
		"sort"     => "OBTEND",
		"subdir"   => "$workdir",
		"required" => 0,
		# Since it may exist, if arc_prep and RevIBIS::ICAidpHK was just run
		) unless (-e "working_prpscws_index.fits");
	
	if (!-e "working_prpscws_index.fits") {
		
		if  ($ENV{OSF_DATASET} =~ /arc_prep/) {
			&Message ( "No science windows from rev $revno found;  skipping SPI PSD analysis" );
			return;
		}
		else {
			&Message ( "No science windows from rev $revno found;  on hold" );
			exit 5;
		}
	}
	
	#  Get the limit tables:
	#  (Uses ic_find, returns an index of all, regardless of validity time,
	#   and then the executable selects the one(s) corresponding to the data.)
	
	$limits = &ISDCPipeline::GetICIndex(
		"structure" => "SPI.-ALRT-LIM",
		);
	&Error ( "No IC files SPI.-ALRT-LIM found." ) unless ($limits);
	
	#  Decide whether to use nopart=yes or no:  
	#  If nopart=yes, "slices" of time which do not fill up an ontime interval
	#  will not be processed.  This means results will be fewer (i.e. coverage
	#  less complete) but more  accurate.  
	#  In Cons., if there's a time gap, it's permanent, so then
	#  nopart=no to force those ranges to be analyzed anyway.  Maximum coverage
	#  with available data.
	#  In NRT, if there are gaps, they may be filled later.  So use nopart=yes
	#  until the end of the revolution.  At the end of the revolution, in 
	#  arc_prep, do a last run with nopart=no to make sure to use all data 
	#  available.  
	#  
#	if ( ($ENV{PATH_FILE_NAME} =~ /nrt/) && ($type !~ /arc/) ) {
#		$nopart = "yes";
#	} else {
#		$nopart = "no";
#	}
	$nopart = ( ($ENV{PATH_FILE_NAME} =~ /nrt/) && ($type !~ /arc/) ) ? "yes" : "no";
	
	#
	#  Now, different for each type, except do all if arc_prep
	#
	if ( ($type =~ /spa/) || ($type =~ /arc/)) {
		
		#  Get current file:
		if (-e "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_adcgain.fits") {
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - COPY previous version",
				"program_name" => "COPY",
				"filename"     => "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_adcgain.fits",
				"newdir"       => "$workdir/osm",
				"subdir"       => "$workdir",
				"needfiles"    => 1,
				);

			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - remove write protection",
				"program_name" => "$mychmod +w osm/spi_psd_adcgain.fits",
				"subdir"       => "$workdir",
				);
		}
		else {
			&Message ( "WARNING:  no previous version found at "
				."$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_adcgain.fits.");
		}
		
		&ISDCPipeline::PipelineStep(
			"step"          => "$proc - spi_psd_adcgain",
			"program_name"  => "spi_psd_adcgain",
			"par_inDOL"     => "working_prpscws_index.fits[GROUPING]",#
			"par_alertDOL"  => "$limits",#
			"par_outDOL"    => "osm/spi_psd_adcgain.fits",#
			"par_minOBT"    => "",
			"par_maxOBT"    => "",
			"par_append"    => "yes",
			"par_slice"     => "yes",
			"par_nopart"    => "$nopart",#
			"par_ontime"    => "$ENV{SPI_PSD_ADC_DELTA}",#
			"par_dopmax"    => "6",
			"par_fitmin"    => "505.0",
			"par_fitmax"    => "785.0",
			"par_usemode0"  => "yes",
			"par_usemode2"  => "yes",
			"par_usemode3"  => "yes",
			"par_usedete00" => "yes",
			"par_usedete01" => "yes",
			"par_usedete02" => "yes",
			"par_usedete03" => "yes",
			"par_usedete04" => "yes",
			"par_usedete05" => "yes",
			"par_usedete06" => "yes",
			"par_usedete07" => "yes",
			"par_usedete08" => "yes",
			"par_usedete09" => "yes",
			"par_usedete10" => "yes",
			"par_usedete11" => "yes",
			"par_usedete12" => "yes",
			"par_usedete13" => "yes",
			"par_usedete14" => "yes",
			"par_usedete15" => "yes",
			"par_usedete16" => "yes",
			"par_usedete17" => "yes",
			"par_usedete18" => "yes",
			"par_limcheck"  => "$ENV{SPI_PSD_LIMCHECK}",#
			"par_alert0"    => "yes",
			"par_alert1"    => "yes",
			"par_alert2"    => "yes",
			"par_alert3"    => "yes",
			"par_minCRVE"   => "1000",
			"par_clobber"   => "no",
			"par_mode"      => "ql",
			"subdir"        => "$workdir",
			);
		
		&ISDCPipeline::PutAttribute("osm/spi_psd_adcgain.fits[1]","REVOL","$revno",
			"DAL_INT","Revolution number (set by pipeline)") 
			if (-e "osm/spi_psd_adcgain.fits");
		
	}
	if ( ($type =~ /spp/) || ($type =~ /arc/)) {
		
		#  Get current file:
		if (-e "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_performance.fits") {
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - COPY previous version",
				"program_name" => "COPY",
				"filename"     => "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_performance.fits",
				"newdir"       => "$workdir/osm",
				"subdir"       => "$workdir",
				"needfiles"    => 1,
				);
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - remove write protection",
				"program_name" => "$mychmod +w osm/spi_psd_performance.fits",
				"subdir"       => "$workdir",
				);
		}
		else {
			&Message ( "WARNING:  no previous version found at "
				."$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_performance.fits." );
		}
		&ISDCPipeline::PipelineStep(
			"step"         => "$proc - spi_psd_performance",
			"program_name" => "spi_psd_performance",
			"par_inDOL"    => "working_prpscws_index.fits[GROUPING]",#
			"par_alertDOL" => "$limits",#
			"par_outDOL"   => "osm/spi_psd_performance.fits",#
			"par_minOBT"   => "",
			"par_maxOBT"   => "",
			"par_append"   => "yes",
			"par_slice"    => "yes",
			"par_nopart"   => "$nopart",#
			"par_ontime"   => "$ENV{SPI_PSD_PERF_DELTA}",
			"par_limcheck" => "$ENV{SPI_PSD_LIMCHECK}",
			"par_alert0"   => "yes",
			"par_alert1"   => "yes",
			"par_alert2"   => "yes",
			"par_alert3"   => "yes",
			"par_minPE"    => "2500",
			"par_minCRVE"  => "100",
			"par_clobber"  => "no",
			"par_mode"     => "ql",
			"subdir"       => "$workdir",
			);
		
		&ISDCPipeline::PutAttribute(
			"osm/spi_psd_performance.fits[1]",
			"REVOL",
			"$revno",
			"DAL_INT",
			"Revolution number (set by pipeline)"
			) if (-e "osm/spi_psd_performance.fits");
		
	}
	
	if ( ($type =~ /spe/)  || ($type =~ /arc/)) {
		
		#  Get current file:
		if (-e "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_efficiency.fits") {
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - COPY previous version",
				"program_name" => "COPY",
				"filename"     => "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_efficiency.fits",
				"newdir"       => "$workdir/osm",
				"subdir"       => "$workdir",
				"needfiles"    => 1,
				);
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - remove write protection",
				"program_name" => "$mychmod +w osm/spi_psd_efficiency.fits",
				"subdir"       => "$workdir",
				);
		}
		else {
			&Message ( "WARNING:  no previous version found at "
				."$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_efficiency.fits." );
		}
		$coeff =  &ISDCPipeline::GetICIndex(
			"structure" => "SPI.-COEF-CAL",
			);
		&Error ( "No IC file SPI.-COEF-CAL found." ) unless ($coeff);	#	040820 - Jake - SCREW 1533
		
		&ISDCPipeline::PipelineStep(
			"step"           => "$proc - spi_psd_efficiency",
			"program_name"   => "spi_psd_efficiency",
			"par_inDOL"      => "working_prpscws_index.fits[GROUPING]",#
			"par_coeffDOL"   => "$coeff",
			"par_alertDOL"   => "$limits",#
			"par_outDOL"     => "osm/spi_psd_efficiency.fits",#
			"par_minOBT"     => "",
			"par_maxOBT"     => "",
			"par_append"     => "yes",
			"par_slice"      => "yes",
			"par_nopart"     => "$nopart",#
			"par_ontime"     => "$ENV{SPI_PSD_EFFI_DELTA}",
			"par_onground"   => "no",
			"par_thresnoerr" => "yes",
			"par_engmin"     => "50.0",
			"par_engmax"     => "8000.0",
			"par_engbin"     => "10.0",
			"par_ignore"     => "yes",
			"par_saveSpec"   => "no",
			"par_saveDOL"    => "spec.fits",
			"par_reportfit"  => "no",
			"par_limcheck"   => "$ENV{SPI_PSD_LIMCHECK}",
			"par_alert0"     => "yes",
			"par_alert1"     => "yes",
			"par_alert2"     => "yes",
			"par_alert3"     => "yes",
			"par_minPE"      => "4000",
			"par_clobber"    => "no",
			"par_mode"       => "ql",
			"subdir"         => "$workdir",
			);
		&ISDCPipeline::PutAttribute("osm/spi_psd_efficiency.fits[1]","REVOL","$revno",
			"DAL_INT","Revolution number (set by pipeline)") 
			if (-e "osm/spi_psd_efficiency.fits");
		
	}
	if ( ($type =~ /sps/) || ($type =~ /arc/))  {
		
		#  Get current file:
		if (-e "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_si.fits") {
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - COPY previous version",
				"program_name" => "COPY",
				"filename"     => "$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_si.fits",
				"newdir"       => "$workdir/osm",
				"subdir"       => "$workdir",
				"needfiles"    => 1,
				);
			&ISDCPipeline::PipelineStep(
				"step"         => "$proc - remove write protection",
				"program_name" => "$mychmod +w osm/spi_psd_si.fits",
				"subdir"       => "$workdir",
				);
		}
		else {
			&Message ( "WARNING:  no previous version found at "
				."$ENV{SCWDIR}/$revno/rev.000/osm/spi_psd_si.fits." );
		}
		
		if (!defined($coeff)) {
			$coeff =  &ISDCPipeline::GetICIndex(
				"structure" => "SPI.-COEF-CAL",
				) unless (-e "SPI.-COEF-CAL-IDX.fits");
			$coeff = "SPI.-COEF-CAL-IDX.fits[GROUPING]" if (-e "SPI.-COEF-CAL-IDX.fits");
			&Error ( "No IC file SPI.-COEF-CAL found." ) unless ($coeff);	#	040820 - Jake - SCREW 1533
		}
		
		#  Note:  for this one, just get the DOL, no need for different
		#    validity times.  
		my $lines =  &ISDCPipeline::GetICFile(
			"structure" => "SPI.-LINE-SCT",
			"select"    => "EVT_TYPE == 'PSD_SI'",				#	040705 - Jake - SPR 3748
			);
		&Error ( "No IC file SPI.-LINE-SCT found." ) unless ($lines);	#	040820 - Jake - SCREW 1533
		
		&ISDCPipeline::PipelineStep(
			"step"          => "$proc - spi_psd_si",
			"program_name"  => "spi_psd_si",
			"par_inDOL"     => "working_prpscws_index.fits[GROUPING]",#
			"par_coeffDOL"  => "$coeff",
			"par_lineDOL"   => "$lines",
			"par_alertDOL"  => "$limits",#
			"par_outDOL"    => "osm/spi_psd_si.fits",#
			"par_minOBT"    => "",
			"par_maxOBT"    => "",
			"par_append"    => "yes",
			"par_slice"     => "yes",
			"par_nopart"    => "$nopart",#
			"par_ontime"    => "$ENV{SPI_PSD_SI_DELTA}",
			"par_onground"  => "no",
			"par_siThres"   => "4.0",
			"par_engtolRel" => "1.0",
			"par_ignore"    => "yes",
			"par_bgddop"    => "1",
			"par_maxlines"  => "1",
			"par_thres"     => "4.0",
			"par_engtol"    => "5.0",
			"par_engtol2"   => "5.0",
			"par_minSigma"  => "0.5",
			"par_maxSigma"  => "2.0",
			"par_reportpar" => "no",
			"par_reportfit" => "no",
			"par_limcheck"  => "$ENV{SPI_PSD_LIMCHECK}",
			"par_alert0"    => "yes",
			"par_alert1"    => "yes",
			"par_alert2"    => "yes",
			"par_alert3"    => "yes",
			"par_minPE"     => "6000",
			"par_clobber"   => "no",
			"par_mode"      => "ql",
			"subdir"        => "$workdir",
			);

		&ISDCPipeline::PutAttribute(
			"osm/spi_psd_si.fits[1]",
			"REVOL",
			"$revno",
			"DAL_INT",
			"Revolution number (set by pipeline)"
			) if (-e "osm/spi_psd_si.fits");
	}
	
	return;
}  # end sub PSD 
#############################################################

1;

__END__


=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrvdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

