#!/usr/bin/perl

=head1 NAME

nrvgen.pl - NRT Revolution Generic script

=head1 SYNOPSIS

I<nrvgen.pl> - Run from within B<OPUS>.  This is the main step of a three 
stage pipeline which processes files written into the revolution
directory of the repository, i.e. RRRR/rev.000/raw/.  

=head1 DESCRIPTION

This process is triggered by the completion of the nrvst pipeline step
through the OSF.    It creates a working directory in the WORKDIR scratch
space named for the input file, copies that file into the "raw" subdir, and 
creates working outdirs for "prep", "aca", and "cfg".  It contains a 
switch on the name of the dataset and performs different operations 
depending on the file type.  

This script then calls the appropriate function from the instrument module, 
e.g. for an IBIS dump, it calls RevIBIS::DPidp and RevIBIS::ICAidp.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location 
of the repository, i.e. REP_BASE_PROD usually.

=item B<WORKDIR>

This is set to the B<rev_work> entry in the path file.  It is the 
location of the working directory, i.e. OPUS_WORK.

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the 
scw part of the repository.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the 
location of all log files seen by OPUS.  The real files are located
in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the 
location of the pipeline parameter files.  

=item B<ALERTS>

This is set to the B<alerts> entry in the path file and is where to 
write alerts. 

=item B<SCW_INPUT>

This is the input directory for the science window pipeline, used to check 
whether a revolution is ready for archiving.

=item B<SPI_ACA_DELTA>

Time interval between SPI ACA analysis runs.  

=item B<SPI_INT_TIME>

Integration time of a SPI ACA analysis run every SPI_ACA_DELTA minutes.

=item B<PICSIT_ICA_DELTA>

Integration time for PICsIT ICA analysis.

=item B<TPF_DIR>

Where to copy tar file of TPFs to be sent to MOC via IFTS.

=item B<ILT_ALERT>

Alert level for notifying operators of a TPF tar file in IFTS outbox.temp.

=item B<IBIS_VETO_ACA_DELTA>

Integration time for IBIS veto ACA processing.

=item B<PICSIT_ACA_DELTA>

Integration time of PICsIT ACA analysis.

=item B<ARC_TRIG>

Directory for archive ingest triggers.

=item B<ECS_DIR>

Where to copy Exposure Completion Status report to be sent to MOC via IFTS.

=back

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use OPUSLIB;
use Datasets;
use Archiving;
use RevIBIS;
use RevJMX;
use RevOMC;
use RevSPI;
use RevIREM;

my $DoNOTIndex;

my ($retval,@results);

print "\n=================================================================\n";

########################################################################
##  machinations to get right environment from path/resource/env vars...
##
&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","ALERTS","SCW_INPUT");

#  SPI processing only:
&ISDCPipeline::EnvStretch("SPI_ACA_DELTA","SPI_INT_TIME") 
	if ($ENV{PROCESS_NAME} =~ /rvspi/);

#  IDP procesing only (PICsIT fault checking):
&ISDCPipeline::EnvStretch("PICSIT_ICA_DELTA") 
	if ($ENV{PROCESS_NAME} =~ /rvidp/); 

#  Needed in ILT only:
&ISDCPipeline::EnvStretch("IFTS_OUTBOX","TPF_DIR","ILT_ALERT","REV_INPUT") 
	if ($ENV{PROCESS_NAME} =~ /nrvidp/);

#	iii
&ISDCPipeline::EnvStretch("REV_INPUT") 
	if ($ENV{OSF_DATASET} =~ /iii/);

#  IRC processing only:
&ISDCPipeline::EnvStretch("IBIS_VETO_ACA_DELTA") 
	if ($ENV{PROCESS_NAME} =~ /rvirv/);

#  PRC processing only, which is only during arc trigger:
&ISDCPipeline::EnvStretch("PICSIT_ACA_DELTA","ARC_TRIG","ECS_DIR") 
	if ($ENV{OSF_DATASET} =~ /arc/);

print "*******     OSF_DCF_NUM is $ENV{OSF_DCF_NUM}\n";

##########################################################################
# machinations to get correct log file, link, and OSF name
##
my $osfname = $ENV{OSF_DATASET};
my ($dataset,$type,$revno,$prevrev,$nexrev,$use,$vers) = &Datasets::RevDataset("$osfname");
exit 0 unless ($use); #  Doesn't happen in pipeline, but in README.test.
print "*******     Dataset is $dataset\nOSF is $osfname\n";

#########              set processing type:  NRT or CONS
my $proc = &ISDCLIB::Initialize();
#	my $proc = &ProcStep();


#my $stamp = $dataset;
#$stamp =~ s/.*(\d{14})_\d{2}\.fits$/$1/;
my ( $stamp ) = ( $dataset =~ /.*(\d{14})_\d{2}\.fits$/ );



if ( ($dataset =~ /arc_prep/) || ($dataset =~ /iii_prep/) ){
	#	040804 - Jake - SPR 3802 - no longer use OSF timestamp for arc_prep and iii_prep file's $stamp
	my $revstart;
	my $revstop;
	#       cannot use &ISDCPipeline::ConvertTime bc REVNUM returns 2 needed numbers
	($retval,@results) = &ISDCPipeline::PipelineStep(              
		"step"          => "$proc - convert REVNUM $revno to IJD",
		"program_name"  => "converttime",
		"par_informat"  => "REVNUM",
		"par_intime"    => "$revno",
		"par_outformat" => "IJD",
		"par_dol"       => "",
		"par_accflag"   => "3",
		);
	&Error ( "Converttime failed." ) if ( $retval ); 	#	040820 - Jake - SCREW 1533

	#       Example 
	#       ./converttime revnum 24 ijd
	#        Log_1  : Beginning parameters
	#        Log_1  : Parameter informat = REVNUM
	#        Log_1  : Parameter intime = 0024
	#        Log_1  : Parameter outformat = IJD
	#        Log_1  : Parameter dol = 
	#        Log_1  : Parameter accflag = 3
	#        Log_1  : Ending parameters
	#        Log_1  : Running in scripting mode, no parameter prompting
	# > Log_1  : Input Time(REVNUM): 0024 Output Time(IJD): Boundary 1088.19982851851864324999 1091.19060398148167223553
	#        Log_1  : Task converttime terminating with status 0
	foreach (@results) { 
		next unless /^.*.IJD.:\s+Boundary\s+(\S+)\s+(\S+)\s*$/i;
		$revstart = $1;
		$revstop  = $2;
		last;
	}
	
	$stamp = &ISDCPipeline::ConvertTime(
		"informat"  => "IJD",
		"intime"    => "$revstop",
		"outformat" => "UTC",
		);		#		2002-12-27T04:33:24.000
	$stamp =~ s/^.*(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.\d{3}.*$/$1$2$3$4$5$6/;
}

#  Remove a previous log file.  (There will always be one, either initialized 
#   in startup, or in a previous DP run.)
#  Then re-initialize it.  The reason we do this is so that:
#    a)  after startup, something appears, just a place holder until here
#    b)  DP then puts in the pipeline info again 
#    c)  re-runs have the same thing, but only one run in the log.

my $logfile = "$ENV{LOG_FILES}/$osfname.log";
my $reallogfile = "$ENV{SCWDIR}/$revno/rev.000/logs/$dataset";
$reallogfile =~ s/\.fits/_log\.txt/;
$reallogfile .= "_log.txt" if ($type =~ "arc|iii") ;

unlink "$reallogfile";
$retval = &ISDCPipeline::PipelineStart(
	"dataset"     => "$ENV{OSF_DATASET}",
	"type"        => "$type",
	"reallogfile" => "$reallogfile",
	"logfile"     => "$logfile",
	"logonly"     => 1, # just re-init log
	);

#die "******     ERROR:  could not start Rev pipeline:  $retval" 
&Error ( "Could not start Rev pipeline:  $retval" )
	if ($retval);

&Message ( "STARTING" );


#########################################################################
#  create working directories
#
my $workdir = "$ENV{WORKDIR}/$osfname";

# remove if previous run exists
if (-e "$workdir") {
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - remove previous run",
		"program_name" => "$myrm -rf $workdir",
		);
}

foreach ( "raw", "prp", "aca", "cfg", "osm" ) {
	&ISDCLIB::DoOrDie ( "$mymkdir -p $workdir/$_" ) unless ( -d "$workdir/$_" );
}
my $outdir = "$ENV{SCWDIR}/$revno/rev.000";

chdir("$workdir") 
	or &Error ( "Cannot chdir into new working dir $workdir!" );


unless ( $dataset =~ /iii_prep|arc_prep|ilt|spi_psd/ ) {

	#	050908 - Jake
	#	Why is the raw file copied out and then, eventually, copied back? Is it modified?  Why?  
	#	We had a problem copying it back once because it was write-protected.  Should I also write-enable
	#	$outdir/raw/$dataset before copying it out so that I can overwrite it when I'm done?
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - copy input file to working dir",
		"program_name" => "$mycp $outdir/raw/$dataset $workdir/raw",
		);
}


########################################################################
##  Alert if version != 00

if ( ($vers ne "00") && ($type !~ /arc|iii/) ){
	&ISDCPipeline::WriteAlert(
		"step"    => "$proc - ALERT",
		"message" => "Dataset $dataset received with non-zero version",
		"level"   => 1,
		"id"      => "301",
		"subdir"  => $workdir,
		);
}


########################################################################
##  If on the file type;  
##

##########
##  ScW Prep:  
##########
if ($dataset =~ /iii_prep/) {

	# SPI Calibration ( SCREW 1347 ) - Jake added 040122
	&RevSPI::SPICal($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);

}	#	end iii_prep

##########
##  Arc Prep:  
##########
elsif ($dataset =~ /arc_prep/) {
	
	#  ICA:  PICsIT fault checking with all ScW HK data:
	&RevIBIS::ICAidpHK($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  ACA PICsIT calibrations:  not working yet:
	#  &RevIBIS::ACAprc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&DummyStep("ACA");
	&WriteProtect();

	#
	#	ACA ibis_isgr_cal_energy TESTING	- SCREW 791 and SCREW 1424 
	#
	&RevIBIS::ACAirc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  SPI PSD analysis
	&RevSPI::PSD($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	# SPI ACA analysis 
	#  &RevSPI::ACAspec($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&DummyStep("ACA");
	
	#	Remove and delete all *RAW structures from swg_raw.fits - 040422 - Jake added - SCREW 1415
	&Archiving::RawRemoval($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) 
		if ($ENV{PATH_FILE_NAME} =~ /cons/);
	
	#  OSM Exposure completeness report generation, cleaning HK averages,  etc.:
	&Archiving::OSMarc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	
} # end arc_prep


##########
##  IBIS dumps
##########
if ($dataset =~ /ibis_raw_dump/) {
	
	#  DP for IBIS dumps;  OBT and decoding
	&RevIBIS::DPidp($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  ICA for each dump;  PICsIT fault checking and trigger ISGRI threshholding
	#   (the latter NRT only, distinguished within this subroutine)
	&RevIBIS::ICAidp($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	
}


##########
##  ISGRI low thresholding
##########
elsif ($dataset =~ /ibis_ilt/) {
	#  ICA low thresholding, will only trigger in NRT case:
	$DoNOTIndex = &RevIBIS::ICAiltNRT($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	
}


##########
##  ISGRI noise maps
##########
elsif ($dataset =~ /isgri_raw_noise/) { 
	
	#  DP OBT calculation 
	&RevIBIS::DPirn($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  ICA noisy pixel maps
	&RevIBIS::ICAirn($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
}  

##########
##  PICsIT calibrations
##########
elsif ($dataset =~ /picsit_raw_cal/) {
	#  DP OBt calculation
	&RevIBIS::DPprc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	#  ACA: not done file-by-file but in arc_prep only (NRT and CONS) 
}

##########
##  IBIS Veto calibrations
##########
elsif ($dataset =~ /ibis_raw_veto/) {
	
	#  DP OBT calculation
	&RevIBIS::DPirv($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  ACA analysis
	&RevIBIS::ACAirv($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	#  &RevIBIS::ACAirvCU($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&DummyStep("ACA CU");
}

##########
##  JEMX FRSS spectra calibrations
##########
elsif ($dataset =~ /raw_frss/) {
	
	#  DP OBT calculation
	&RevJMX::DPjm($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	#  Can't write protect here;  it gets written to in next step.
	&WriteProtect("aca_frss"); # except aca_frss file
	
	#  ACA fitting
	&RevJMX::ACAjm($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
}

##########
##  JEMX electronic calibrations
##########
elsif ($dataset =~ /jemx(1|2)_raw_ecal/) {
	
	#  DP OBT calculation
	&RevJMX::DPjme($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	&RevJMX::ACAjme($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	#  &DummyStep("ACA");
}

##########
##  IREM
##########
elsif ($dataset =~ /irem/) {
	#
	#	040407 - Jake - DPire crashes if all Rows = 0 in irem file
	#	SCREW 1428 solution to dal_list and wrap all these in an if statement
	#	tested with modified (empty) irem_raw_20021226220109_00.fits 
	#
	my $rows = 0;
	$rows = &ISDCLIB::RowsIn ( "raw/$dataset\+1", "", "IREM-SYNC-SRW" );
#	$rows = &ISDCLIB::RowsIn ( "raw/$dataset"."[1]", "", "IREM-SYNC-SRW" );

	print ">>>>>>>     There are $rows total rows in IREM-SCIE-RAW and IREM-HK..-HRW of the file: $dataset \n";
	if ($rows <= 0) {
		print ">>>>>>>     Not processing it.\n\n";
	} else {
		print ">>>>>>>     Processing it.\n\n";
		#  Note:  no write protection of PRP file between steps;  keeps adding to it.
		
		#  DP tele check, OBT, and Hk conversion
		&RevIREM::DPire($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
		&WriteProtect("irem_prp"); # except irem_prp file
	
		#  ICA status
		&RevIREM::ICAire($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
		#  ACA Bpar calc and Spec calc
		&RevIREM::ACAire($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	}
}

##########
##  OMC calibrations
##########
elsif ($dataset =~ /omc_raw_(bias|dark|flatfield|sky)/) {
	#  DP calculation only
	&RevOMC::DPomc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
}    

##########
##  SPI calibration spectra
##########
elsif ($dataset =~ /spi_raw_ac(off|on)_spectra/) {
	
	# DP OBT calculation
	&RevSPI::DPspec($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
}

##########
##  SPI PSD analyses
##########

elsif ($dataset =~ /spi_psd/) {
	&RevSPI::PSD($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
}


##########
## ISGRI calibrations
##########
elsif ($dataset =~ /isgri_raw_cal/) {
	
	# DP OBT calculation
	&RevIBIS::DPirc($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&WriteProtect();
	
	#  ACA analysis
	#  &RevIBIS::ACAircOld($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev);
	&DummyStep("ACA");
}

##########
##  JMX and SPI dumps
##########
elsif ( ($dataset =~ /(.*)_raw_.*dump_*/) || ($dataset =~ /(spi)_acs_cal/) ) {
	#  raw_.*dump catches JEMX dfeedumps and SPI {as,df,pd}dumps as well
	#     (NOT TESTED)
	my $inst = $1;
	my $struct;
	
	$inst =~ tr/a-z/A-Z/;
	# add . to structure for SPI and OMC
	$inst =~ s/(SPI|OMC)/$1\./;
	
	# annoying fudge for JemX;  is this jemx or jmx?!
	$inst =~ s/JEMX/JMX/;
	
	# another annoying fudge for JEMX DFEE
	if ($dataset =~ /dfee/){ 
		$struct = "$inst-DFEE-CRW";
	}
	
	elsif ($dataset =~ /spi_acs_cal/) {
		$struct = "SPI.-ACS.-CRW";
	}
	elsif ($dataset =~ /spi_raw_(as|df|pd)dum/) {
		#  Shouldnt' this be a CRW?  
		$struct = "SPI.-".uc($1)."MD-HRW";
	}
	else {
		$struct = "$inst-DUMP-CRW";
	}
	
	&ISDCPipeline::PipelineStep(
		"step"                => "$proc - $inst dump dp_obt_calc",
		"program_name"        => "dp_obt_calc",
		"par_InSWGroup"       => "",
		"par_OutSWGroup"      => "",
		"par_RawData"         => "",
		"par_ConvertedData"   => "",
		"par_AttributeData"   => "raw/$dataset"."[$struct]",
		"par_TimeInfo"        => "",
		"par_IN_STRUCT_NAME"  => "",
		"par_OUT_STRUCT_NAME" => "",
		"par_ATT_STRUCT_NAME" => "$struct",
		"par_LOBT_2X4_NAMES"  => "",
		"par_LOBT_1X8_NAMES"  => "",
		"par_PKT_NAMES"       => "",
		"par_LOBT_ATTR"       => "",
		"par_PKT_ATTR"        => "PCKSTART OBTSTART PCKEND OBTEND",
		"par_OBT_TYPE"        => "",
		);
	
}

else {
	##  If none of the above, do nothing (shouldn't get here, but in case)
	&Message ( "$dataset" );
} # end of if

#######
#  Last steps:  
#######

######  Write protection:
&WriteProtect();

######  Move results to repository:
&MoveProducts();

######  Update indices and links:
&Indexing( $DoNOTIndex );

######  Copy alerts:
my $stream = ($ENV{PATH_FILE_NAME} =~ /nrt/) ? "realTime" : "consolidated";

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ALERTS}" ) unless ( -d "$ENV{ALERTS}" );
my $scw_prp_index = "";
$scw_prp_index = "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits[GROUPING]"
	if ((-e "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits") && ( $ENV{CONSREV_UNIT_TEST} !~ /TRUE/ ) );

&ISDCPipeline::PipelineStep(
	"step"           => "$proc - copy alerts to $ENV{ALERTS}",
	"program_name"   => "am_cp",
	"par_OutDir"     => "$ENV{ALERTS}",
	"par_OutDir2"    => "$ENV{SCWDIR}/$revno/rev.000/logs/",
	"par_Subsystem"  => "REV",
	"par_DataStream" => "$stream",
	"subdir"         => "$workdir",
	"par_ScWIndex"   => "$scw_prp_index",
	) if (`$myls $workdir/*alert* 2> /dev/null`);



########################################################################
# cleanup:

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - cleanup workdir",
	"program_name" => "$myrm -rf $workdir",
	"subdir"       => "$ENV{WORKDIR}",
	);

if ( ($dataset =~ /iii_prep/) && ($ENV{PATH_FILE_NAME} =~ /cons/) ){

	($retval,@results) = &ISDCPipeline::RunProgram("$mytouch $ENV{REV_INPUT}/${revno}_rev.done");
#	die "*******     ERROR:  cannot \'$mytouch $ENV{REV_INPUT}/${revno}_rev.done\':  @results" 
	&Error ( "Cannot \'$mytouch $ENV{REV_INPUT}/${revno}_rev.done\':  @results" )
		if ($retval);

	&ISDCPipeline::BBUpdate(
		"path"      => "consscw",
		"match"     => "^$revno",
		"matchstat" => "^$osf_stati{SCW_DP_C_COR_H}\$",
		"fullstat"  => "$osf_stati{SCW_DP_C}",
		);
}


print "\n========================================================================\n";
exit 0;

########################################################################
##
## DONE
##
########################################################################







########################################################################

=item B<WriteProtect> ( $except )

=cut

sub WriteProtect {
	#  Can only give one string to except from this.
	my ($except) = @_;
	
	chdir("$workdir") or &Error ( "Cannot chdir into new working dir $workdir!" );
	my @products = sort(glob("*/*"));
	if (!(@products)) {
		print "*******     No products found in */*.  Returning.\n";
		return;
	}
	print "*******     Found products @products\n";
	my $chmodlist;
	if ($except) {
		foreach (@products) { $chmodlist .= " $_" if ( (-w "$_") && !(/$except/) ); }
	}
	else {
		foreach (@products) { $chmodlist .= " $_" if (-w "$_"); } 
	}
	$chmodlist .= " *alert"  if (`$myls $workdir/*alert 2> /dev/null`);
	
	print "*******     Will write protect $chmodlist\n";
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect",
		"program_name" => "$mychmod -w $chmodlist",
		"subdir"       => "$workdir",
		) if ($chmodlist);
	return;
} # end sub WriteProtect


########################################################################

=item B<MoveProducts> ( )

=cut

sub MoveProducts {
	my @products;
	my $product;
	my $element_ref;
	my $subdir;
	my $overwrite;
	
	foreach $subdir (keys( %{ $Datasets::Products{$type} } )) {
		$overwrite = 0; 
		print "*******     Examining subdir $subdir contents\n";
		@products = ();#  need to empty out products.
		$element_ref = \$Datasets::Products{$type}{$subdir};
		if (ref( $element_ref ) eq "SCALAR") {
			#  This for single entries:
			push @products, "$subdir/$Datasets::Products{$type}{$subdir}*";
		}
		else {
			#  This for array entries: (which are REF not ARRAY?)
			foreach ( @{ $Datasets::Products{$type}{$subdir} } ) {
				#  Watch it:  only case where it's the suffix instead of the prefix:
				push @products, "$subdir/$_*" unless (/TPF|INT/);
				push @products, "$subdir/*$_" if (/TPF|INT/);
			}
		}
		print "*******     Expecting the following products in $subdir:\n".join(' ',@products)."\n";
		foreach $product (@products) {
			if (`$myls $workdir/$product 2> /dev/null`) {
				
				print "*******     Found and moving product $product to repository.\n";
				# for replacing raw files which exist but aren't write protected:
				$overwrite = 1 if ($subdir =~ /raw/);  
				# file replacing files which not only exist but are write protected.
				$overwrite = 2 if ($product =~ /irem_lctr|spi_psd/); 
				&ISDCPipeline::PipelineStep(
					"step"         => "$proc - move results to repository",
					"program_name" => "COPY",
					"filename"     => "$product",
					"newdir"       => "$ENV{SCWDIR}/$revno/rev.000/$subdir",
					"subdir"       => "$workdir",
					"overwrite"    => $overwrite,
					);


#				#	050301 - Jake - SCREW 1308
#				#	Compare 2 products (source and target)
#				chomp ( my $sourcefile = `$myls $workdir/$product` );
#				my $sourcebase = basename ($sourcefile);
#				chomp ( my $targetfile = `$myls $ENV{SCWDIR}/$revno/rev.000/$subdir/$sourcebase` );
#
#				&ISDCPipeline::RunProgram( "diff $sourcefile $targetfile");
#
#				&ISDCPipeline::PipelineStep(
#					"step" => "$proc - dal_verify source data",
#					"program_name" => "dal_verify",
#					"par_indol" => "$sourcefile+1",
#					"par_check" => "yes",
#					"par_detachmem" => "no",
#					"par_detachother" => "no",
#					"par_checkloops" => "yes",
#					"par_checksums" => "no",
#					"par_backpointers" => "yes",
#					"par_fixback" => "no",
#					"par_chatty" => "no",
#					"par_templates" => "yes",
#					"par_ignore" => "",
#					"par_mode" => "ql",
#					"stoponerror" => 0,							#	<---------------------------------  JUST FOR NOW because some spi files aren't correct
#					);
#
#				&ISDCPipeline::PipelineStep( 
#					"step" => "$proc - dal_verify target data",
#					"program_name" => "dal_verify",
#					"par_indol" => "$targetfile+1",
#					"par_check" => "yes",
#					"par_detachmem" => "no",
#					"par_detachother" => "no",
#					"par_checkloops" => "yes",
#					"par_checksums" => "no",
#					"par_backpointers" => "yes",
#					"par_fixback" => "no",
#					"par_chatty" => "no",
#					"par_templates" => "yes",
#					"par_ignore" => "",
#					"par_mode" => "ql",
#					"stoponerror" => 0,							#	<---------------------------------  JUST FOR NOW because some spi files aren't correct
#					);
#

			} # end if product found
			else {
				&Message ( "WARNING:  no product $product found" );
			}
		} # foreach product in each subdir
		
	} # end foreach subdir 
	
	return;
	
} # end sub MoveResults


########################################################################

=item B<DummyStep> ( $step )

=cut

sub DummyStep {
	my ($step) = @_;
	#	040820 - Jake - SCREW 1533
	&Message ( "$dataset DUMMY step for $step;  processing inoperative." );
}


########################################################################

=item B<Indexing> ( $DoNOTAttachList )

Now, we need to update all indices, different for every file type:

Dataset::Indices* are hashes of hashes;  the top level has has keys
which are the types;  then each type has a hash of a root name
and data structure, e.g. for type idp, we have:
	"idp" => {
		"cfg/isgri_context"=>"ISGR-CTXT-GRP"
		"cfg/picsit_context"=>"PICS-CTXT-GRP"
		"cfg/hepi_context"=>"PICS-LUTS-GRP"
	},

=cut

sub Indexing {
	my $DoNOTAttachList = $_[0];
	my $count;
	my $dir;
	my $root;
	my $idx_name;
	my $file_name;
	my $structure;
	my $extn;
	my $collect; 
	my $prefix;
	my $sort;
	
	##########################
	#  Indices under REP_BASE_PROD/scw/RRRR/rev.000/idx:
	##########################
	print "********************************************************************\n"
		."********************************************************************\n"
		."*******     Checking for rev.000 indices to update;  expecting:\n"
		."********************************************************************\n";
	print join("\n",keys( %{ $Datasets::IndicesRev{$type} })),"\n";
	
	#	... $type = "ilt"
	foreach $root (keys(%{ $Datasets::IndicesRev{$type} })) {
		#	... $root = "cfg/isgri_context_dead"
		$structure = $Datasets::IndicesRev{$type}{$root};		#	... $structure = "ISGR-DEAD-CFG"
		# split off subdir
		$root =~ /(.*)\/(.*)/;
		$dir = $1;			#	... $dir = "cfg"
		$prefix = $2;		#	... $prefix = "isgri_context_dead"
		next if ( $DoNOTAttachList =~ /$prefix/ );

		$idx_name = $prefix."_index";
		
		if ($dir =~ /raw/) {
			#  Raw files
			$file_name = "${prefix}_${stamp}_$vers.fits";
		}
		elsif ($prefix =~ /ecs|spi_psd|hk_averages|spi_gain_coeff/) {
			#  Things without stamps:
			$file_name = $prefix.".fits";
		}
		else {
			#  Everything else
			$file_name = "${prefix}_$stamp.fits";
		}
		
		if (-e "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/$dir/$file_name") {
			print "*******     Found $dir/$file_name\n";
		}
		else {
			print "*******     WARNING:  skipping, did not find "
				."$ENV{REP_BASE_PROD}/scw/$revno/rev.000/$dir/$file_name\n";
			next;
		}
		
		$extn = $structure unless ($structure =~ /GRP$/);
		$extn = "GROUPING" if ($structure =~ /GRP$/);
		print "*******     Root is $root;  structure is $structure;  extn is $extn;  "
			."file_name is $file_name;  idx_name is $idx_name\n";

		&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/idx" ) unless ( -d "$ENV{SCWDIR}/$revno/rev.000/idx" );

		#  Special case for JMX?-FRSS-CRW and -GAIN-CAL:  the datasets are all 
		#   indices, and we want to collect the contents here:
		if ($prefix =~ /frss/) { 
			print "*******     Calling MakeIndex with collect=y\n";
			$collect = "y";
			$extn = "GROUPING";
		}
		else { $collect = "n"; }
		
		&ISDCPipeline::MakeIndex(
			"root"     => "$idx_name",
			"subdir"   => "$ENV{SCWDIR}/$revno/rev.000/idx",
			"add"      => 1,
			"clean"    => 2,
			"template" => "$structure-IDX.tpl",
			"osfname"  => "$osfname",
			"files"    => "../$dir/$file_name",
			"ext"      => "[$extn]",
			"collect"  => "$collect",
			);
		
		&ISDCPipeline::LinkUpdate(
			"root"   => "$idx_name",
			"subdir" => "$ENV{SCWDIR}/$revno/rev.000/idx",
			);
	} # foreach index
	
	
	##########################
	#  Indices under REP_BASE_PROD/idx/rev
	##########################
	print "********************************************************************\n"
		."********************************************************************\n"
		."*******     Checking for global indices to update:  expecting:\n"
		."********************************************************************\n";
	print join("\n",keys( %{ $Datasets::IndicesGlobal{$type} })),"\n";
	
	#	... $type = "ilt"
	foreach $root (keys(%{ $Datasets::IndicesGlobal{$type} })) {
		#  $root is the file root ("aca/jemx1_aca_frss" for example), and it's the
		#   key pointing to the structure name, (e.g. "JMX1-GAIN-CAL").
		#  (Not to be confused with the root parameter of MakeIndex, which is
		#  the root name of the index, which in this context is the structure!)
		#	... $root = "cfg/isgri_context_dead"
		$structure = $Datasets::IndicesGlobal{$type}{$root}; #	... $structure = "ISGR-DEAD-CFG"
		# split off subdir									#	040412 - Jake
		$root =~ /(.*)\/(.*)/;								#	040412 - Jake
		$dir = $1;			#	... $dir = "cfg"
		$prefix = $2;		#	... $prefix = "isgri_context_dead"
		next if ( $DoNOTAttachList =~ /$prefix/ );

		$extn = $structure unless ($structure =~ /GRP$/);
		$extn = "GROUPING" if ($structure =~ /GRP$/);
		print "*******     \$structure is $structure\n";
		if ($root =~ /raw/) {
			#  Raw files:
			$file_name = "${root}_${stamp}_$vers.fits";
		}
		elsif ($root =~ /ecs|spi_psd|hk_averages|spi_gain_coeff/) {
			#  Things without stamps:
			$file_name = $root.".fits";
		}
		else {
			#  Everything else:
			$file_name = "${root}_$stamp.fits";
		}
		
		#  Sort only IREM indices (read by IOSM);  others take too long.
		#  (SCREW 443 and SPR 2117)
		$sort = "TSTART" if ($file_name =~ /irem/);
		
		
		if ( -e "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/$file_name") {
			print "*******     Found $file_name\n";
		}
		else {
			print "*******     WARNING:  skipping, didn't find "
				."$ENV{REP_BASE_PROD}/scw/$revno/rev.000/$file_name\n";
			next;
		}
		
		#  Special case for JMX?-GAIN-CAL:  the datasets are all indices, and we
		#   want to collect hte contents here:
		if ($structure =~ /GAIN-CAL/) { 
			print "*******     Calling MakeIndex with collect=y\n";
			$collect = "y";
			$extn = "GROUPING";
		}
		else { $collect = "n"; }
		
		&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{OUTPATH}/idx/rev" ) unless ( -d "$ENV{OUTPATH}/idx/rev" );
		&ISDCPipeline::MakeIndex(
			"root"     => "$structure-IDX",
			"subdir"   => "$ENV{OUTPATH}/idx/rev",
			"clean"    => 2,
			"add"      => "1",
			"template" => "$structure-IDX.tpl",
			"osfname"  => "$ENV{OSF_DATASET}",
			"files"    => "../../scw/$revno/rev.000/$file_name",
			"ext"      => "[$extn]",
			"sort"     => "$sort",
			"collect"  => "$collect",
			);
		
		&ISDCPipeline::LinkUpdate("root" => "$structure-IDX","subdir" => "$ENV{OUTPATH}/idx/rev")  
			if (-e "../../scw/$revno/rev.000/$file_name");
		
	} # for each global index
	
	return;
} # sub Indexing
########################################################################

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.  For further information on the 
instrument specific processing, see the instrument specific module help by 
typing, e.g.

perldoc RevIBIS.pm

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

