#!perl

=head1 NAME

nswosm.pl -  Science Window Observation Status Monitoring

=head1 SYNOPSIS

I<nswosm.pl> - Run from within B<OPUS>.  This is the fourth step of a five stage pipeline which processes science windows.  This performs the Observation Status Monitoring of the science window data.

=head1 DESCRIPTION

All gti related functionality has been moved to the correction step for the reprocessing.  This step now does mostly just osm_monitor for the instruments.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location of the repository, i.e. REP_BASE_PROD usually.

=item B<WORKDIR>

This is set to the B<nrt_work> entry in the path file.  It is the location of the working directory, i.e. OPUS_WORK.

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the scw part of the repository.

=item B<CFG_DIR>

This is the templates directory, set to the B<cfg_dir> entry in the path file, usually ISDC_ENV/tempates.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the location of all log files seen by OPUS.  The real files are located in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the location of the pipeline parameter files.  

=item B<ALERTS>

This is the centralized alerts repository.  This is set to the B<nrt_alerts> entry in the path file, usually /isdc/alert/ntr.

=back

=head1 SUBROUTINES

=over

=cut

use strict;
use lib "$ENV{ISDC_OPUS}/pipeline_lib/";
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

sub genericOSM;
sub spiOSM;

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","CFG_DIR","ALERTS","REV_WORK");


#########              set processing type:  NRT or CONS
my $proc = &ISDCLIB::Initialize();
#	my $proc = &ProcStep();

#	&Message ( "STARTING" );  

#########

my $revno = &ISDCPipeline::RevNo($ENV{OSF_DATASET});
my $prevrev = sprintf "%04d", ( $revno - 1 );

#########             Set group name and extension

my $grpdol = "swg.fits[GROUPING]";						

#
#	DO NOT REMOVE THIS STEP, UNLESS YOU REPLACE IT WITH SOMETHING 
#	ELSE THAT WILL SET THE COMMONLOGFILE VARIABLE!! - JAKE 040308
#
#	Failure to do this may either cause a crash, because the variable is not set
#	or write log information to the wrong log file or even create a common_log.txt
#	which may cause a crash because it will be an unexpected "junk" file.
#
#$ENV{COMMONLOGFILE} = "+$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
chdir &ISDCPipeline::FindScw( "$ENV{OSF_DATASET}" );
&Message ( `pwd` );
#	&ISDCPipeline::PipelineStep(
#		"step"         => "$proc - dummy step to set COMMONLOGFILE variable that swg_clean on swg_prp.fits used to do",
#		"program_name" => "$myecho",
#		);

########################################################################
#########             find required limit files
########################################################################


# find latest version of alert limits
my $missing;
my $inst;
my $struct;
my %lim_alert;
my @result;
my $alert_list;
foreach $inst ("ibis","spi","omc","jmx1","jmx2", "sc") {
	
	$struct = $inst."-ALRT-LIM";
	$struct =~ s/sc/intl/;
	$struct = uc($struct);
	$struct =~ s/(OMC|SPI)/$1\./;
	
	@result = &ISDCPipeline::GetICFile(
		"structure" => "$struct",
		"sort"      => "VSTART",
		"error"     => 0,
		"filematch" => "$grpdol",
		);

	if (@result) {
		$lim_alert{$inst} = $result[$#result];
		$alert_list .= "-----   ".$lim_alert{$inst}."\n";
	} else {
		print "*****     Missing ALRT limit for $inst\n";
		$missing .= "$struct";
	}
	
}

&Error ( "Cannot find the following IC structures:\n$missing\n" ) 
	if ($missing); #  040820 - Jake - SCREW 1533

&Message ( "ALERT LIMITS:\n$alert_list-----   " );  

########################################################################
#########             generic first steps
########################################################################

&ISDCPipeline::PipelineStep(
	"step"             => "$proc - osm_data_check",
	"program_name"     => "osm_data_check",
	"par_InSWGroup"    => "",
	"par_OutSWGroup"   => "$grpdol",
	"par_MIN_DURATION" => "8",
	"par_MAX_DURATION" => "24000",		
	"par_ACC_OVERLAP"  => "$ENV{OSM_ACC_OVERLAP}",
	);

my ($retval,@results) = &ISDCPipeline::PipelineStep(
	"step"                    => "$proc - osm_timeline",
	"program_name"            => "osm_timeline",
	"par_OutSWGroup"          => "$grpdol",
	"par_InSWGroup"           => "",
	"par_ATT_TOLERANCE_EXP"   => "0.1",
	"par_ATT_TOLERANCE_NOEXP" => "5",
	"par_EXPO_TOLERANCE_GT"   => "30",
	"par_EXPO_TOLERANCE_LT"   => "30",
	"par_UNDEF_TOLERANCE"     => "20",
	"par_MODE_TOLERANCE"      => "20",
	"stoponerror"             => 0,
	);
# check exact return status;  if 12304 (missing PDEF) or 12305 (PDEF contains
#  no data for current time) and we are in NRT, don't want to stop.  
#  All other errors, die as usual.
if ($retval) {
	if ((($retval == 12304) || ($retval == 12305)) && ($ENV{PATH_FILE_NAME} =~ /nrt/)){
		&Message ( "WARNING:  osm_timeline missing pointing definition "
			."in NRT pipeline;  continuing." );  
	} else {
		print "Return status of $retval from osm_timeline is not allowed\n";
		exit 1;
	}
}


########################################################################
#########             call each instrument subroutine
########################################################################

foreach ( "SC", "IBIS", "JMX1", "JMX2", "OMC", "SPI" ) {
	&genericOSM ( $_ );
}

&spiOSM();

########################################################################
#########             generic last steps
########################################################################

#  Check for any indices I forgot to clean up in IBIS.
my @junk = glob("working*");
foreach (@junk) { unlink "$_"; }

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - clean OSM science window group", 
	"program_name" => "swg_clean",
	"par_object"   => "swg.fits[1]",						
	"par_showonly" => "no",
	) if ( -r "swg.fits" ) ;

&Message ( "done" );  

exit 0;

########################################################################
#########             done with main
########################################################################

=item B<genericOSM> ( $INST )

=cut

sub genericOSM {
	my ($INST) = @_;				#	Should be uppercase, but solely for aesthetics
	my $inst = lc ( $INST );

	&Message ( "$INST starting" );  
	
	&ISDCPipeline::PipelineStep(
		"step"           => "$proc - $INST osm_monitor",
		"program_name"   => "osm_monitor",
		"par_InSWGroup"  => "",
		"par_OutSWGroup" => "$grpdol",
		"par_Data"       => "",
		"par_ModeTable"  => "",
		"par_TimeInfo"   => "",
		"par_LimitTable" => "$lim_alert{ $inst }",
		);
}


########################################################################

=item B<spiOSM> ( )

SPI osm_const_param wrapper

=cut

sub spiOSM {
	
	&ISDCPipeline::PipelineStep(
		"step"             => "$proc - SPI osm_const_param",
		"program_name"     => "osm_const_param",
		"par_InSWGroup"    => "",
		"par_OutSWGroup"   => "$grpdol",#
		"par_AlertLevel"   => "2",
		"par_SPI_Params1"  => "R__AF__HNRG_MOO__Li R__AF__HVPS_MOO__Li R__AF__HV__Li R__AF__LVPS_MOO__Li R__AF__LWDT__Li R__AF__MAIN_MDF__Li R__AF__REDT_MDF__Li R__AF__WRK_MM_DA__Li R__AF__WRK_MRNG__Li R__PD__DE_MOP__Li R__PD__EVWK5__Li R__PD__GNC_MDGL__L R__PD__GNC_MOP__L R__PD__GN_MCONV__Li R__PD__LIB_MSEL__Li R__PD__LOW_MTH__Li R__PD__LWD_MDG__Li R__PD__LWD_MOP__Li R__PD__NB_MCRV_MCD__L R__PD__NB_MCRV_MOP__L R__PD__NB_MEV_M8H__L R__PD__NB_MSTEP__Li R__PD__NB_MTMPL__Li R__PD__OFS_MCONV__Li R__PD__PARAK3__Li S__AS__CYL_MCT__Li S__AS__VCU_DHW__L R__PD__HIGH_MTH__Li",
		"par_SPI_Params2"  => "R__AS__EVT_MTGR__Li R__AS__HV_MOO__Li R__AS__HV__Li R__AS__ISB_MED__Li R__AS__NRG_MDSC__Li R__AS__OVL_MMSK__1 R__AS__RATE_MMT__Li R__AS__RESP_MED__Li R__AS__VCUWDG_MED__L R__AS__VTO_MCND__Li R__AS__VTO_MDLYP__Li R__AS__VTO_MDLY__Li R__AS__VTO_MDRV__Li R__AS__VTO_MED__Li R__AS__VTO_MMSK__Li R__AS__VTO_MNML__Li R__AS__VTO_MOVG__Li R__AS__VTO_MPLSR__L R__AS__VTO_MTST__Li R__DF__TG_MAF__Li R__DF__TR_MLGTH__L R__DF__WDW__L R__DF__XGTE_MAB__L R__DF__XGTE_MBL__L R__DIAG__HK__Li R__DIAG__SA_MID__Li R__DT__AC_MMOD__L R__DT__DROP_MNRJ__L R__DT__ER_MTHR__L R__DT__INIB_MASIC__L R__DT__INIB_MCODE__L R__DT__INIB_MRAM__L R__DT__LOW_MMOD__L R__DT__ROUT_MMOD__L R__PD__DE_MDG__Li",
		"par_SPI_Params3"  => "R__AS__WDOG_MED__Li R__CR__ANLG_M1__L R__CR__ANLG_M2__L R__CR__CRY_MRNG__L R__DF__ACQ__L R__DF__ASSO__L R__DF__CNT_MAF__Li R__DF__CNT_MVPSD__L R__DF__DIAL__L R__DF__DLY_MPD__L R__DF__DLY_MVTPL__L R__DF__DT_MAFST__L R__DF__DT_MAF__L R__DF__DT_MVGT__L R__DF__ENB_MAF__Li R__DF__ENB_MPD__L R__DF__ENHSLERF__L R__DF__ENHSLER__L R__DF__EVT_MNVTO__L R__DF__FMTPE__L R__DF__FR_MAF__Li R__DF__GT_MPDVTO__L R__DF__GT_MVTPL__L R__DF__ME__L R__DF__MS_MNRG__L R__DF__PE__L R__DF__POBJ__L R__DF__PP__L R__DF__PROCPE__L R__DF__RCVE__L R__DF__RST_MDTO__L R__DF__RST_MPSD__L R__DF__RST_MTFR__L R__DF__SAF_MAF__Li R__DF__SERIAL__L R__DF__SE__L R__DF__SP_MMOD__L",
		"par_SPI_Params4"  => "R__PD__PY_M8H_MCD__L R__PD__PY_M8H_MOP__L R__PD__TIME_MDG__L R__PD__TIME_MOP__L R__PD__TRG_MDG__L R__PD__TRG_MOP__L R__PS_MHV_M_D1 R__PS_MHV_M_D2 R__PS__VTO_MTST__L91 R__RW__AS_MOO__L R__SW__AF_MLV_MFT__L R__SW__AF_MLV_MTH__L R__SW__AF_MLV_MTP__L R__SW__AF_MNRG__L R__SW__AF_MOO__L R__SW__AS_MMEM_MDY__L R__SW__BGD_MCAP__L R__SW__CNT_MFTR__L R__SW__COLD_MCAP__L R__SW__COLD_MTHR__L1 R__SW__COLD_MTHR__L2 R__SW__COLD_MTHR__L3 R__SW__COLD_MTHR__L4 R__SW__COR_MCAP__L R__SW__DF_MMEM_MDY__L R__SW__DF_MOO__L R__SW__DY_MAF_MRD__L R__SW__DY_MBF_MAT__L R__SW__DY_MBF_MCF__L R__SW__DY_MBF_MEC__L R__SW__DY_MBF_MRD__L R__SW__ESAM__L R__SW__FT_MRD_MNO__L R__SW__FT_MRD_MOV__L R__SW__HK_MAQ_MRT__L R__SW__IM_MEC__L R__SW__IM_MSW__L R__SW__LG_MHL_MDF__L R__SW__LSL_MERR__L R__SW__PD_MMEM_MDY__L R__SW__PD_MOO__L R__SW__RAD_MCAP__L R__SW__RAD_MMOD__L R__SW__RCONF_MCAP__L R__SW__SPECTRA__L R__SW__SP_MAC_MDU_ML R__SW__TH_MRD_MNO__L R__SW__TH_MRD_MOV__L",
		"par_IBIS_Params"  => "",
		"par_JEMX1_Params" => "",
		"par_JEMX2_Params" => "",
		"par_OMC_Params"   => "",
		"par_SC_Params"    => "",
		);
} # spiOSM





########################################################################

__END__ 

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

