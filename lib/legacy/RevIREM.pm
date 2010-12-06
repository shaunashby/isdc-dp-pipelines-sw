package RevIREM;

=head1 NAME

RevIREM.pm - NRT Revolution File Pipeline IREM Module

=head1 SYNOPSIS

use I<RevIREM.pm>;
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

sub RevIREM::DPire;
sub RevIREM::ICAire;
sub RevIREM::ACAire;

$| = 1;

##########################################################################

=item B<DPire> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for irem_raw files:

The pipeline first creates the output group in the "prep" subdirectory 
and containing all output data structures for the IREM processing.  Each
data structure in the raw file is then attched to the output group, and that
group then used in all subsequent processing.  

The pipeline checks for a previous LCTR (IREM local time) file in the current
or previous revolution and copies it into the "cfg" subdir.  It then calls 
the B<irem_tele_check> executable, 
giving either the previous LCTR or a template to create a new one.  The 
executable is called from and the output file is written to the cfg 
subdirectory of the workdir, and the result named "ilct" in place of 
"raw".  

The executable B<irem_obt_calc> is then run on the output group with the
ILCT file as input as well.  This calculates the on board time.

The ILCT result only is then moved to the repository and write protected. 

=cut

sub DPire {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	#  Check for a hold:
	if (-e "$ENV{WORKDIR}/IREM_clock_reset.stop") {
		
		&Message ( "IREM on hold for clock reset update" );
		exit 5;
	}
	
	# create output group and attach raw data group
	my $outgroup = $dataset;
	$outgroup =~ s/raw/prp/;
	$outgroup =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/cfg" ) unless ( -d "$ENV{SCWDIR}/$revno/rev.000/cfg" );
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect raw file",
		"program_name" => "$mychmod -w raw/$dataset",
		"subdir"       => "$workdir",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - IREM create output group",
		"program_name" => "dal_create",
		"par_obj_name" => "$outgroup",
		"par_template" => "IREM-CHNK-GRP.tpl",
		"subdir"       => "$workdir/prp",
		);
	chdir("$workdir") or &Error ( "Cannot change back to $workdir" );
#		or die "cannot change back to $workdir";
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - IREM attach raw data",
		"program_name" => "dal_attach",
		"par_Parent"   => "prp/$outgroup"."[GROUPING]",
		"par_Child1"   => "raw/$dataset"."[IREM-SYNC-SRW]",
		"par_Child2"   => "raw/$dataset"."[IREM-SCIE-RAW]",
		"par_Child3"   => "raw/$dataset"."[IREM-HK..-HRW]",
		"par_Child4"   => "",
		"par_Child5"   => "",
		);			       

	my ($status,$cobt0) = &ISDCPipeline::GetAttribute ("raw/$dataset"."[GROUPING]" , "COBT0");		
	&Error ( "COBT0 not found in raw/$dataset" ) if ($status);	

	&ISDCPipeline::PutAttribute (																							
		"prp/$outgroup"."[GROUPING]", "COBT0", $cobt0, 
		"DAL_CHAR", "Synchronization Central-OBT - copied from RAW");

	my $histgrp = "-";
	my $histcheck = "0";
	
	# needs LCTR history file; if not, create (for some backward compatibility)
	my $histtpl;
	my $newvers;
	my $ilcthist;
	
	my @ilcthists = sort(glob("$ENV{REP_BASE_PROD}/aux/adp/ref/crst/irem_clock_reset_*.fits*"));	#	060518 - Jake - currently not, but could be gzipped
	#  The above is correct after SPR 2132, but add the below for backward
	#   compatibility.
	@ilcthists = sort(glob("$ENV{REP_BASE_PROD}/aux/adp/ref/crst/irem_lctr_*.fits*")) 	#	060518 - Jake - currently not, but could be gzipped
			unless (-e "$ilcthists[$#ilcthists]");
	
	if (-e "$ilcthists[$#ilcthists]") {
		$ilcthist = $ilcthists[$#ilcthists]."[IREM-LCTR-HIS]";
	}
	else {
		#  Can't do this anymore;  must have an initialized file.
		
		&Error ( "No ILCT file found in $ENV{REP_BASE_PROD}/aux/adp/ref/crst/irem_clock_reset_*.fits*" );
		
	} # end if not found in aux/adp/ref/crst
	
	#  Also need previous IREM group, but previous just befor this one!
	print "*******     LOOKING for previous IREM group:\n";
	my $histstamp;
	my $i;
	my @others = sort(glob("$ENV{SCWDIR}/$revno/rev.000/prp/irem_prp*.fits"));
	@others = sort(glob("$ENV{SCWDIR}/$prevrev/rev.000/prp/irem_prp*.fits*")) 												#	060517 - Jake - these could be gzip'd ??
			unless (@others);
	#  Go backward through current results and stop at first which has the
	#    time just before this one (of those that finished;  this means it
	#    doesn't right now spot a missing file):
	for ($i = $#others; $i >= 0; $i--) {
#		$histstamp = $others[$i];
#		$histstamp =~ s/^.*prp_(\d{14})\.fits/$1/;
		( $histstamp = $others[$i] ) =~ s/^.*prp_(\d{14})\.fits/$1/;
		print "*******     Found $others[$i];  comparing $histstamp with $stamp (current)\n";
		next if ($histstamp > $stamp);
		print "*******     SUCCESS:  $histstamp less than $stamp;  using this file\n";
		$histgrp = $others[$i];
		last;
	}
	
	if (-e "$histgrp") {
		print "*******     SUCCESS:  histgrp exists;  setting histcheck=1\n";
		$histcheck = 1;
		$histgrp .= "[GROUPING]";
	}
	else {
		print "*******     WARNING:  found no previous IREM prp files in $revno or $prevrev;  "
			."running with histcheck=0\n";
	}
	
	# SCREW-02137
	# Check to see if there is a problem with the current dataset (i.e. that there was a clock reset during nrt processing).
	# For problem datasets - datasets listed in HISTCKi keywords in the clock reset file - set HistCheck parameter to 0.
	# 
	# Extract datestamp from filename string stored in $dataset variable, e.g. irem_raw_20090329030936_00.fits:
	my ($ds) = ($dataset =~ m|irem_raw_(.*?)_.*?$|);
	# Use fkeyprint to access the list of HISTCKi in the clock reset file:
	my $clock_reset_file = $ilcthists[$#ilcthists]."\[1]";
	my $skip_datasets=[];
	
	open(DUMP, "fkeyprint $clock_reset_file HISTCK |") or die __PACKAGE__."::DPire: Trying to run\"fkeyprint\"".$!."\n";
	
	while(<DUMP>) {
	    chomp;
	    # Skip empty lines:
	    next if /^\s+$/;
	    # Skip commented lines:
	    next if /^#/;
	    # Strip out datestamp from OSF ID:
	    my ($ds) = ($_ =~ m|.*?=.*?_(.*?)_.*?_ire.*?$|);
	    push(@$skip_datasets,$ds);
        }
	
	# Now check for current dataset timestamp in the list of problematic datasets.
	# If the input dataset timestamp exists in the list of problem 
	# datasets, set HistCheck parameter to 0:
	if (grep { $_ eq $ds } @$skip_datasets) {		
	    $histcheck=0;
	    print "**** HistCheck set to 0 for dataset $dataset (known clock resets: ".join(",",@$skip_datasets).")\n";
	}	
	
	my ($retval,@result) = &ISDCPipeline::PipelineStep (
		"step"                 => "$proc - IREM irem_tele_check",
		"program_name"         => "irem_tele_check",
		# Must use out group to update keywords
		"par_inIREMgroup"      => "",
		"par_outIREMgroup"     => "prp/$outgroup"."[GROUPING]",
		"par_SCIEraw"          => "-",
		"par_HKraw"            => "-",
		"par_SYNCraw"          => "-",
		"par_SYNCprp"          => "-",
		"par_ClockCheck"       => "$ENV{IREM_CLOCK_CHECK}",
		"par_HistCheck"        => "$histcheck",
		"par_inHistIREMgroup"  => "$histgrp",
		"par_outHistIREMgroup" => "-",
		"par_HistSCIEraw"      => "-",
		"par_HistHKraw"        => "-",
		"par_HistSYNCraw"      => "-",
		"par_ILCTRinobject"    => "$ilcthist",
		"subdir"               => "$workdir",
		"stoponerror"          => 0,
		);
	
	###############
	#  Check return status for IREM clock reset:
	if ($retval)  {
		chomp $result[$#result];
		my @strsplit = split('-',$result[$#result]);
		if ( ($strsplit[$#strsplit] eq "550000") && ($ENV{PATH_FILE_NAME} =~ /nrt/) ) {
			&Message ( "WARNING:  IREM clock reset found;  suspending all IREM processing." );
			
			($retval,@result) = &ISDCPipeline::RunProgram("$mytouch $ENV{WORKDIR}/IREM_clock_reset.stop");
#			die "*******     ERROR:  cannot touch $ENV{WORKDIR}/IREM_clock_reset.stop:  @result" 
			&Error ( "Cannot touch $ENV{WORKDIR}/IREM_clock_reset.stop:  @result" ) if ($retval);
			
			#  irem_tele_check should have written an alert
			
			if (`$myls $workdir/*alert* 2> /dev/null`) {
#				my $stream;
#				$stream = "realTime" if ($ENV{PROCESS_NAME} =~ /^n/);
#				$stream = "consolidated" if ($ENV{PROCESS_NAME} =~ /^c/);
				my $stream = ( $ENV{PROCESS_NAME} =~ /^n/ ) ? "realTime" : "consolidated";
				&ISDCPipeline::PipelineStep(
					"step"           => "$proc - am_cp",
					"program_name"   => "am_cp",
					"par_OutDir"     => "$ENV{ALERTS}",
					"par_OutDir2"    => "$ENV{SCWDIR}/$revno/rev.000/logs/",
					"par_Subsystem"  => "REV",
					"par_DataStream" => "$stream",
					"subdir"         => "$workdir",
					"par_ScWIndex"   => "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits[1]",	#	050131 - jake - SPR 3978
					);
			}
			else {
				#  If irem_tele_check exited 550000 but didn't send an alert, 
				#   exit with error
				&Error ( "irem_tele_check exited 550000 but didn't issue an alert!" );
#				die "*******     ERROR:  irem_tele_check exited 550000 but didn't issue an alert!";
			}
			#  Exit special status to put this on hold.
			exit 5;
			
		}
		# Any other error, we stop.  Or if reset detected in CONS, stop;  should
		#  have been detected in NRT and ILCT should already list this reset!
		else {
			print "Return status of $strsplit[$#strsplit] from irem_tele_check is not allowed.\n";
			exit 1;
		}
	}
	
	chdir("$workdir") or &Error ( "Cannot chdir back to $workdir" );
#		or die "cannot chdir back to $workdir";
	
	#  SPR 2149:  must copy keywords, in case raw HK is empty
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - IREM copy keywords",
		"program_name" => "dal_attr_copy",
		"par_indol"    => "prp/$outgroup"."[GROUPING]",
		"par_outdol"   => "prp/$outgroup"."[IREM-HK..-CNV]",
		"par_keylist"  => "CREATOR,CONFIGUR,REVOL,ERTFIRST,ERTLAST,OBTSTART,OBTEND,TSTART,TSTOP",
		);
	
#	my $irem_cnv;
	my $irem_cnv = &ISDCPipeline::GetICFile(
		"structure" => "IREM-CONV-MOD",
		"filematch" => "prp/$outgroup"."[GROUPING]",
		);
	
	&Error ( "No IC file IREM-CONV-MOD found." ) unless ($irem_cnv);	
	&ISDCPipeline::PipelineStep (
		"step"                => "$proc - IREM dp_hkc",
		"program_name"        => "dp_hkc",
		"par_InSWGroup"       => "prp/$outgroup"."[GROUPING]",
		"par_OutSWGroup"      => "",
		"par_RawData"         => "",
		"par_ConvertedData"   => "",
		"par_TimeInfo"        => "",
		"par_LOBT_2X4_NAMES"  => "",
		"par_LOBT_1X8_NAMES"  => "",
		"par_ConversionCurve" => "$irem_cnv",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"             => "$proc - IREM irem_obt_calc",
		"program_name"     => "irem_obt_calc",
		"par_outIREMgroup" => "prp/$outgroup"."[GROUPING]",
		"par_SCIEraw"      => "-",
		"par_HKraw"        => "-",
		"par_SYNCprp"      => "-",
		"par_inIREMgroup"  => "-",
		"par_SCIEprp"      => "-",
		"par_HKcnv"        => "-",
		"par_ILCTRobject"  => "$ilcthist",
		);
	
	return;
} # end of DPire



##########################################################################

=item B<ICAire> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Inst.Config.Anal. for irem_raw files:

Raw science windows from the current revolution are selected using the executable B<idx_find> and the index in REP_BASE_PROD/idx/scw/raw/GNRL-SCWG-GRP-IDX.fits.  The INTL-SVM2-HRW data is then collected using B<idx_collect>.  This is given to the executable B<irem_status> along with the IREM group.  

=cut

sub ICAire {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $outgroup = $dataset;
	$outgroup =~ s/raw/prp/;
	$outgroup =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	
	
	#  To select HK for IREM, need ERTs, since we need raw science windows
	#   which don't have OBTs.  So get ERT of this file, convert to IJD,
	#   subtract IREM_STATUS_SELECT hours, convert back to ERT, and use that
	#   to select science windows.
	
	my ($status,$ertlast) = &ISDCPipeline::GetAttribute("prp/$outgroup"."[GROUPING]","ERTLAST");
	
	&Error ( "Cannot find ERTLAST of prp/$outgroup:\n$ertlast" ) if ($status);	#	040820 - Jake - SCREW 1533
	print "*******     ERTLAST is $ertlast\n";
	
	my $ijdthis = &ISDCPipeline::ConvertTime (
		"informat"  => "UTC",
		"intime"    => "$ertlast",
		"outformat" => "IJD",
		"dol"       => "",
		"accflag"   => "3",
		);
	
	#  IREM_STATUS_SELECT is hours of HK data to give to irem_status, and
	#   select using IJD, therefore turn into days:
	
	my $diff = $ENV{IREM_STATUS_SELECT} / 24; # h
	my $ijdmin = $ijdthis - $diff;  
	print "*******     IJD minimum is then $ijdmin\n";
	
	my $utcmin = &ISDCPipeline::ConvertTime(
		"informat"  => "IJD",
		"intime"    => "$ijdmin",
		"outformat" => "UTC",
		);
	

#	060302 - Jake - Added REVOL to selection expr instead of using dal_clean

	my $expr = "( ERTLAST >= '$utcmin' ) && (ERTFIRST <= '$ertlast') && (REVOL == $revno)";
	print "expression is $expr\n";
	
	# index of raw science windows
	&ISDCPipeline::FindIndex(
		"workname" => "working_rawscws_index.fits",
		"select"   => "$expr",
		"index"    => "$ENV{OUTPATH}/idx/scw/raw/GNRL-SCWG-GRP-IDX.fits",
		"sort"     => "ERTLAST",
		"subdir"   => "$workdir",
		);
	&Error ( "index selection resulted in no members." ) unless (-e "$workdir/working_rawscws_index.fits");

#	&ISDCLIB::QuickDalClean ( "$workdir/working_rawscws_index.fits[GROUPING]" );
	
	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - collect index of SVM2 HK",
		"program_name"  => "idx_collect",
		"par_element"   => "working_rawscws_index.fits[GROUPING]",
		"par_template"  => "INTL-SVM2-HRW-IDX.tpl",
		"par_index"     => "working_hrw_index.fits",
		"par_sort"      => "ERTLAST",
		"par_sortType"  => "1",
		"par_sortOrder" => "1",
		"par_update"    => "1",
		"par_stamp"     => "0",
		);
	
	&ISDCPipeline::PipelineStep (
		"step"               => "$proc - IREM irem_status",
		"program_name"       => "irem_status",
		"par_outIREMgroup"   => "prp/$outgroup"."[GROUPING]",
		"par_SCIEraw"        => "",
		"par_SCIEprp"        => "",
		"par_inIREMgroup"    => "",
		"par_SCIEsta"        => "",
		"par_SVM2indexGroup" => "working_hrw_index.fits[GROUPING]",
		"subdir"             => "$workdir",
		);
	
} # end of ICAire


##########################################################################

=item B<ACAire> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Auto.Calib.Analy. for irem_raw files:

The executable B<irem_data_corr> is run on the output group created in the
B<nrvdp> step.  It uses the index of IREM-GRNP-CFG files found in the
"ic" repository.  Then B<irem_bpar_calc> is called.  Lastly, B<irem_spec_calc>
is called using the response files found in the "ic" repository.  

=cut

sub ACAire {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $outgroup = $dataset;
	$outgroup =~ s/raw/prp/;
	$outgroup =~ s/(\d{14})_\d{2}\.fits/$1\.fits/;
	my @files;
	my $grnp = &ISDCPipeline::GetICFile(
		"structure" => "IREM-GRNP-CFG",
		"filematch" => "prp/$outgroup"."[GROUPING]",
		);
	
	&Error ( "No IC file IREM-GRNP-CFG found." ) unless ($grnp);	#	040820 - Jake - SCREW 1533
	
	&ISDCPipeline::PipelineStep(
		"step"             => "$proc - IREM irem_data_corr",
		"program_name"     => "irem_data_corr",
		"par_outIREMgroup" => "prp/$outgroup"."[GROUPING]",
		"par_SCIEraw"      => "-",
		"par_SYNCprp"      => "-",
		"par_inIREMgroup"  => "-",
		"par_SCIEcor"      => "-",
		"par_IREMGrndParTable" => "$grnp",
		);
	
	&ISDCPipeline::PipelineStep( 
		"step"                 => "$proc - IREM irem_bpar_calc",
		"program_name"         => "irem_bpar_calc",
		"par_outIREMgroup"     => "prp/$outgroup"."[GROUPING]",
		"par_SCIEraw"          => "-",
		"par_SCIEprp"          => "-",
		"par_inIREMgroup"      => "-",
		"par_ORBMAG"           => "-",
		"par_IntMagFieldModel" => "1",
		"par_IntMagFieldYear"  => "2000",
		"par_ExtMagFieldModel" => "1",
		"par_MagFieldPar0"     => "0.0",
		"par_MagFieldPar1"     => "0.0",
		"par_MagFieldPar2"     => "0.0",
		"par_MagFieldPar3"     => "0.0",
		"par_MagFieldPar4"     => "0.0",
		"par_MagFieldPar5"     => "0.0",
		"par_MagFieldPar6"     => "0.0",
		"par_MagFieldPar7"     => "0.0",
		"par_MagFieldPar8"     => "0.0",
		"par_MagFieldPar9"     => "0.0",
		);
	
	my $el_ic = &ISDCPipeline::GetICFile(
		"structure" => "IREM-ELEC-RSP",
		"filematch" => "prp/$outgroup"."[GROUPING]",
		);
	
	&Error ( "No IC file IREM-ELEC-RSP found." ) unless ($el_ic);	#	040820 - Jake - SCREW 1533
	
	my $prot_ic = &ISDCPipeline::GetICFile(
		"structure" => "IREM-PROT-RSP",
		"filematch" => "prp/$outgroup"."[GROUPING]",
		);
	
	&Error ( "No IC file IREM-PROT-RSP found." ) unless ($prot_ic);	#	040820 - Jake - SCREW 1533
	
	&ISDCPipeline::PipelineStep( 
		"step"              => "$proc - IREM irem_spec_calc",
		"program_name"      => "irem_spec_calc",
		"par_outIREMgroup"  => "prp/$outgroup"."[GROUPING]",
		"par_SCIEprp"       => "-",         
		"par_SCIEcor"       => "-",
		"par_inIREMgroup"   => "-",               
		"par_PARTdsp"       => "-",
		"par_ProtDRM"       => "$prot_ic",
		"par_ElecDRM"       => "$el_ic",
		"par_MinHistCounts" => "0",
		"par_NumselC"       => "15",
		"par_selC01"        => "1",
		"par_selC02"        => "2",
		"par_selC03"        => "3",
		"par_selC04"        => "4",
		"par_selC05"        => "5",
		"par_selC06"        => "6",
		"par_selC07"        => "7",
		"par_selC08"        => "8",
		"par_selC09"        => "9",
		"par_selC10"        => "10 ",
		"par_selC11"        => "11 ",
		"par_selC12"        => "12 ",
		"par_selC13"        => "13 ",
		"par_selC14"        => "14 ",
		"par_selC15"        => "15 ",
		"par_NumpnormC"     => "4",
		"par_pnormC01"      => "6",
		"par_pnormC02"      => "7",
		"par_pnormC03"      => "8",
		"par_pnormC04"      => "9",
		"par_NumenormC"     => "2",
		"par_enormC01"      => "12 ",
		"par_enormC02"      => "13 ",
		"par_ProtonModel"   => "1",
		"par_pPar1StartVal" => "30.0 ",
		"par_pPar2StartVal" => "1.0",
		"par_pPar3StartVal" => "-2.0 ",
		"par_pPar1Var"      => "0",
		"par_pPar2Var"      => "1",
		"par_pPar3Var"      => "1",
		"par_pPar1Low"      => "0.0",
		"par_pPar2Low"      => "0.0",
		"par_pPar3Low"      => "0.0",
		"par_pPar1High"     => "0.0",
		"par_pPar2High"     => "0.0",
		"par_pPar3High"     => "10.",
		"par_ElectronModel" => "2",
		"par_ePar1StartVal" => "0.5",
		"par_ePar2StartVal" => "1.0",
		"par_ePar3StartVal" => "-3.0 ",
		"par_ePar1Var"      => "0",
		"par_ePar2Var"      => "1",
		"par_ePar3Var"      => "1",
		"par_ePar1Low"      => "0.0",
		"par_ePar2Low"      => "0.0",
		"par_ePar3Low"      => "0.0",
		"par_ePar1High"     => "0.0",
		"par_ePar2High"     => "0.0",
		"par_ePar3High"     => "10.",
		);
	#  because there's (was?) a junk file left in the run dir
	unlink("$workdir/fort.1") if (-e "$workdir/fort.1");
	
} # end of ICAire
#############################################################################

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

