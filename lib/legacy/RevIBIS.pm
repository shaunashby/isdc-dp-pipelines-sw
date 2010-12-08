package RevIBIS;

=head1 NAME

RevIBIS.pm - NRT Revolution File Pipeline IBIS Module

=head1 SYNOPSIS

use I<RevIBIS.pm>;
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
use warnings;

use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use OPUSLIB qw(:osf_stati);
use TimeLIB;

$| = 1;

##########################################################################

=item B<DPirv> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for ibis_raw_veto files:

The executable B<ibis_veto_obt_calc> is called on this file to add the correct on board time to the raw file.  The result is then write protected and added to index files kept in the work directory. 

=cut

sub DPirv {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - IBIS ibis_veto_obt_calc",
		"program_name" => "ibis_veto_obt_calc",
		"par_vetoSpec" => "raw/$dataset"."[IBIS-VETO-CRW]",
		);
	
	return;
} # end of DPirv


##########################################################################

=item B<ACAirv> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Auto.Calib.Analy. for ibis_raw_veto files:
(VETO only;  see ACAirvCU for Calibration Unit modules)

The indices of raw veto files and of ACA results are found in the repository for the curent and previous revolutions and collected with B<idx_collect>.  An empty working version of the latter is created if no  results yet exist.  The MODULE and OBT_ACQ keywords are found from the raw input file and used to create a selection expression to find raw files with same module and within a time range  of about one revolution.  Then, that expresion and the B<idx_find> executable create a selected index of veto spectra.  The selected index of veto spectra and the previous results are given to the B<ibis_aca_veto> executable which creates a result in the "aca" subdirectory of the repository.  The result is then added to the results index in the pipeline directory.  

=cut

sub ACAirv {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $prevrevdir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$prevrev/rev" );
	
	my $module;
	my $obtacq;
	my $status;
	my $this_index = "";
	my $prev_index = "";
	my @results_indices = "";
	my $prev_results;
	my $root;
	my $work_root;
	my $raw_intt;
	my ($retval,@results);
	my $outdol;
	
	
	##
	#  Construct selection expression for previous veto spectra and results:
	##
	($status,$obtacq) = &ISDCPipeline::GetAttribute ( "raw/$dataset"."[IBIS-VETO-CRW]", "OBTFIRST" );
	&Error ( "Cannot find OBTFIRST of $dataset:\n$obtacq" ) if ($status);	#	040820 - Jake - SCREW 1533
	print "OBTFIRST is $obtacq\n";
	
	#  some reasonable revolution length number goes here?
	#  seconds * 2**20 (since full OBT is in units of 2**20 seconds
	my $diff = $ENV{IBIS_VETO_ACA_DELTA} * 2**20;
	#  This is necessary to get the math to work right with such big
	#  integers.  (They are 4 bytes, Perl handles 2 bytes only (I think.))
	my $obtmin = &ISDCPipeline::DiffOBTs ( $obtacq, $diff );
	
	my $expr = "( ( OBTFIRST <= \'$obtacq\' ) && ( OBTFIRST >= \'$obtmin\' ) ) ";
	print "expression is $expr\n";
	
	
	##
	#   Create working indices for both previous aca results and prepared
	#    spectra, both needed by ibis_aca_veto:
	##
	print "\n*******     Now looking for indices of previous results and spectra:\n";  
	foreach $root ("idx/ibis_aca_veto_index","idx/ibis_raw_veto_index") {
		$prev_results = 0;
		#  Root name of the working indices (various versions):
		( $work_root = $root ) =~ s/.*ibis_(.*)_veto.*/working_index_$1/;
		##
		#  Collect previous and current revolutions into intermediate index, 
		#   selecting for time and module to reduce immediately the number.  
		#   Then will collect the two sub-indices together.  
		##
		
		# first look for previous rev results and select into new local index
		$prev_index = "$prevrevdir/$root.fits" if ( $prevrevdir );

		if ( ( -e "$prev_index" ) || ( -e "$prev_index.gz" ) ) {
			&ISDCPipeline::FindIndex (
				"index"    => "$prev_index",
				"workname" => "${work_root}_prev.fits",
				"select"   => "$expr",
				"sort"     => "OBTFIRST",
				# NOTE:  due to something odd (in DALgroupCopy according to LL), if you give
				#  no selection, the order of the rows gets messed up.  So give a selection
				#  even if the above expression has to be fudged out:
				#			      "select" => "#ROW >= 1",
				#			      "sort" => "",
				"required" => 0, # no error if nothing matches
				);
			$prev_results++ if (-e "${work_root}_prev.fits");
		} # if prev rev results exist
		else {
			print "*******     WARNING:  didn't find previous revolution's index for "
				."$work_root:  $prev_index\n";
		}
		#  Now look for current rev results index:
		$this_index =  "$ENV{SCWDIR}/$revno/rev.000/$root.fits";
		if (-e "$this_index") {

			&ISDCPipeline::FindIndex (
				"index"    => "$this_index",
				"workname" => "${work_root}_this.fits",
				"select"   => "$expr",
				"sort"     => "OBTFIRST",
				# NOTE:  due to something odd (in DALgroupCopy according to LL), if you give
				#  no selection, the order of the rows gets messed up.  So give a selection
				#  even if the above expression has to be fudged out:
				#			      "select" => "#ROW >= 1",
				#			      "sort" => "",
				"required" => 0,
				);
			$prev_results++ if (-e "${work_root}_this.fits");
			
		} # if this rev results exist
		else {
			print "*******     WARNING:  didn't find $this_index\n";
		}
		print "*******     Number of previous indices found for $work_root:  $prev_results\n";
		#  Now, if you have prev and this, add prev to this:
		if ($prev_results > 1) {
			&ISDCPipeline::CollectIndex (
				"index"    => "${work_root}_prev.fits",
				"workname" => "${work_root}_this.fits",
				"sort"     => "OBTFIRST",
				"caution"  => 0, # no temp version needed.
				);
		}
		#  Now, for aca results, have to create a dummy if no previous results:
		elsif ( !($prev_results) && ($root =~ /aca/) ) {
			&ISDCPipeline::PipelineStep (
				"step"         => "$proc - create empty results index",
				"program_name" => "dal_create",
				"par_obj_name" => "${work_root}_this.fits",
				"par_template" => "IBIS-VETO-CAL-IDX.tpl",
				"subdir"       => "$workdir",
				);
		}
		elsif (($prev_results) && (-e "${work_root}_prev.fits")){
			#  In this case, there's only one previous result and it's the previous
			#   so we just rename prev to this so that it still gets the final
			#   selection done.  (This will just happen for the first each rev.)
			&ISDCPipeline::PipelineStep (
				"step"         => "$proc - no further data to add;  just rename",
				"program_name" => "$mymv ${work_root}_prev.fits ${work_root}_this.fits",
				"subdir"       => $workdir,
				);
			
		}
		#  Now, if raw results, add current spectra:
		if ($root =~ /raw/) {
			#  Note:  can give dol and template, and executable will figure
			#   out what to do if it exists and if not.
			&ISDCPipeline::PipelineStep (
				"step"          => "$proc - add current spectrum to working index",
				"program_name"  => "idx_add",
				"par_index"     => "${work_root}_this.fits[GROUPING]",
				"par_template"  => "IBIS-VETO-CRW-IDX.tpl",
				"par_element"   => "raw/$dataset"."[IBIS-VETO-CRW]",
				"par_sort"      => "OBTFIRST",
				"par_update"    => "0",
				"par_stamp"     => "0",
				"par_security"  => "0",			
				"par_sortType"  => "1",			
				"par_sortOrder" => "1",			
				);
		}
		#  Finally, we have a ${work_root}_this.fits which has previous rev
		#   results if they existed, which is empty if there were no previous
		#   aca results, and which has the current spectra if it's the raw index.
		#   These indices are sorted in revers OBT order, i.e. latest first.
		
		#  Now, just rename the this index, which is either empty or
		#   has only one entry.  
		print "*******     No further selection necessary;  just rename what we have.\n";
		&ISDCPipeline::RunProgram ( "$mymv ${work_root}_this.fits ${work_root}_selected.fits" );
		
	} # foreach raw and aca index
	
	
	my $veto_results = $dataset;
	$veto_results =~ s/raw/aca/;
	$veto_results =~ s/_(\d{14})_\d{2}/_$1/;
	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/aca/" ) unless ( -d "$ENV{SCWDIR}/$revno/rev.000/aca/" );

	&Message ( "INFO: ibis_aca_veto disabled (SCREW-02111)." );

	my $disable_ibis_aca_veto = 1;
	if ($disable_ibis_aca_veto == 1) {
	    &Message ( "INFO: ibis_aca_veto disabled (SCREW-02111)....deleting file aca/".$veto_results );
	    unlink "aca/".$veto_results;
	}

	# remove the temporary indices
	chdir $workdir;
	unlink(glob("working*"));
}  # end of ACAirv



##########################################################################

=item B<ACAirvCU> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Auto.Calib.Analy. for ibis_raw_veto files:
(Calibration Unit only;  see ACAirv for VETO modules)

=cut

sub ACAirvCU {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $prevrevdir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$prevrev/rev" );
	
	my $module;
	my $obtacq;
	my $status;
	my $this_index = "";
	my $prev_index = "";
	my @results_indices = "";
	my $prev_results;
	my $root;
	my $work_root;
	my $raw_intt;
	my ($retval,@results);
	my $outdol;
	
	
	##
	#  Construct selection expression for previous cu spectra and results,
	#   remembering that the result is UNIT but the raw data is still VETO
	##
	($status,$obtacq) = &ISDCPipeline::GetAttribute ( "raw/$dataset"."[IBIS-VETO-CRW]", "OBTFIRST" );
	&Error ( "Cannot find OBTFIRST of $dataset:\n$obtacq" ) if ( $status );	
	print "OBTFIRST is $obtacq\n";
	
	#  some reasonable revolution length number goes here?
	#  seconds * 2**20 (since full OBT is in units of 2**20 seconds
	my $diff = $ENV{IBIS_CU_ACA_DELTA} * 2**20;
	#  This is necessary to get the math to work right with such big
	#  integters.  (They are 4 bytes, Perl handles 2 bytes only (I think.))
	my $obtmin = &ISDCPipeline::DiffOBTs ( $obtacq, $diff );
	
	my $expr = "( ( OBTFIRST <= \'$obtacq\' ) && ( OBTFIRST >= \'$obtmin\' ) ) ";
	print "expression is $expr\n";
	
	
	##
	#   Create working indices for both previous aca results and prepared
	#    spectra, both needed by ibis_aca_cu:
	##
	print "\n*******     Now looking for indices of previous results and spectra:\n";  
	foreach $root ("idx/ibis_aca_cu_index","idx/ibis_raw_veto_index") {
		$prev_results = 0;
		#  Root name of the working indices (various versions):
		( $work_root = $root ) =~ s/.*ibis_(.*)_index/working_index_$1/;
		##
		#  Collect previous and current revolutions into intermediate index, 
		#   selecting for time and module to reduce immediately the number.  
		#   Then will collect the two sub-indices together.  
		##
		
		# first look for previous rev results and select into new local index
		$prev_index = "$prevrevdir/$root.fits" if ($prevrevdir);

		if ( ( -e "$prev_index" ) || ( -e "$prev_index.gz" ) ) {		#	060420
			unlink "${work_root}_prev.fits" if (-e "${work_root}_prev.fits");
			&ISDCPipeline::FindIndex (
				"index"    => "$prev_index",
				"workname" => "${work_root}_prev.fits",
				"select"   => "$expr",
				"sort"     => "OBTFIRST",
				"required" => 0, # no error if nothing matches
				);
			$prev_results++ if (-e "${work_root}_prev.fits");
		} # if prev rev results exist
		else {
			print "*******     WARNING:  didn't find previous revolution's index for "
				."$work_root:  $prev_index\n";
		}
		#  Now look for current rev results index:
		$this_index =  "$ENV{SCWDIR}/$revno/rev.000/$root.fits";
		if (-e "$this_index") {

			unlink "${work_root}_this.fits"  if (-e "${work_root}_this.fits" );
			&ISDCPipeline::FindIndex (
				"index"    => "$this_index",
				"workname" => "${work_root}_this.fits",
				"select"   => "$expr",
				"sort"     => "OBTFIRST",
				"required" => 0,
				);
			$prev_results++ if (-e "${work_root}_this.fits");
			
		} # if this rev results exist
		else {
			print "*******     WARNING:  didn't find $this_index\n";
		}
		print "*******     Number of previous indices found for $work_root:  $prev_results\n";
		#  Now, if you have prev and this, add prev to this:
		if ($prev_results > 1) {
			&ISDCPipeline::CollectIndex (
				"index"    => "${work_root}_prev.fits",
				"workname" => "${work_root}_this.fits",
				"sort"     => "OBTFIRST",
				"caution"  => 0, # no temp version needed.
				);
		}
		#  Now, for aca results, have to create a dummy if no previous results:
		elsif ( !($prev_results) && ($root =~ /aca/) ) {
			&ISDCPipeline::PipelineStep (
				"step"         => "$proc - create empty results index",
				"program_name" => "dal_create",
				"par_obj_name" => "${work_root}_this.fits",
				"par_template" => "IBIS-UNIT-CAL-IDX.tpl",
				"subdir"       => "$workdir",
				);
		}
		elsif (($prev_results) && (-e "${work_root}_prev.fits")){
			#  In this case, there's only one previous result and it's the previous
			#   so we just rename prev to this so that it still gets the final
			#   selection done.  (This will just happen for the first each rev.)
			&ISDCPipeline::PipelineStep (
				"step"         => "$proc - no further data to add;  just rename",
				"program_name" => "$mymv ${work_root}_prev.fits ${work_root}_this.fits",
				"subdir"       => $workdir,
				);
			
		}
		#  Now, if raw results, add current spectra:
		if ($root =~ /raw/) {
			#  Note:  can give dol and template, and executable will figure
			#   out what to do if it exists and if not.
			&ISDCPipeline::PipelineStep (
				"step"          => "$proc - add current spectrum to working index",
				"program_name"  => "idx_add",
				"par_index"     => "${work_root}_this.fits[GROUPING]",
				"par_template"  => "IBIS-VETO-CRW-IDX.tpl",
				"par_element"   => "raw/$dataset"."[IBIS-VETO-CRW]",
				"par_sort"      => "OBTFIRST",
				"par_update"    => "0",
				"par_stamp"     => "0",
				"par_security"  => "0",			
				"par_sortType"  => "1",			
				"par_sortOrder" => "1",			
				);
		}
		#  Finally, we have a ${work_root}_this.fits which has previous rev
		#   results if they existed, which is empty if there were no previous
		#   aca results, and which has the current spectra if it's the raw index.
		#   These indices are sorted in revers OBT order, i.e. latest first.
		
		#  Now, just rename the this index, which is either empty or
		#   has only one entry.  
		print "*******     No further selection necessary;  just rename what we have.\n";
		&ISDCPipeline::RunProgram ( "$mymv ${work_root}_this.fits ${work_root}_selected.fits" );
		
	} # foreach raw and aca index
	
	my $veto_results = "ibis_aca_cu_$stamp.fits";
	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/aca/" ) unless (  -d "$ENV{SCWDIR}/$revno/rev.000/aca/");
	
	# Loop over all MODULES (16-17):
	for ($module=16;$module<=17;$module++) {
		
		#  Won't exist first time, and maybe second:
		if ( !-e "$workdir/aca/$veto_results" ) {
			&ISDCPipeline::PipelineStep (
				"step"         => "$proc - Create ACA result",
				"program_name" => "dal_create",
				"par_obj_name" => "$veto_results",
				"par_template" => "IBIS-UNIT-CAL.tpl",
				"subdir"       => "$workdir/aca",
				);    
		}
		$outdol = "aca/".$veto_results."[IBIS-UNIT-CAL]" ;
		($retval,@results) = &ISDCPipeline::PipelineStep (
			"step"          => "$proc - IBIS ibis_aca_cu for module $module",
			"program_name"  => "ibis_aca_cu",
			"par_inVETO"    => "working_index_raw_veto_selected.fits[GROUPING]",
			"par_inArchive" => "working_index_aca_cu_selected.fits[GROUPING]",
			"par_outVCAL"   => "$outdol",
			"par_lastScW"   => "",
			"par_module"    => "$module",
			"par_verbosity" => "5",
			"par_mode"      => "h",
			"subdir"        => "$workdir",
			);
		
	} # end of for modules 16-17
	
	my $rows = &ISDCLIB::RowsIn ( $outdol );
	print ">>>>>>>     There are $rows rows in $outdol;  ";
	if ($rows == 0) {
		print "deleting it.\n";
		unlink "aca/".$veto_results;
		&Message ( "There are $rows rows in $outdol; deleting it." );
	} elsif ($rows <= 0) {
		&Error ( "ERROR examining $outdol; cannot determine number of rows in $outdol" );
	} else {
		print "NOT deleting it.\n";
		&ISDCPipeline::PutAttribute ( "$outdol", "REVOL", "$revno", "DAL_INT", "Revolution number (set by pipeline)" );
	}

	
	# remove the temporary indices
	chdir $workdir;
	unlink(glob("working*"));
	
}  # end of ACAirvCU


##########################################################################

=item B<DPirn> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for ibis_raw_noise files:

The pipeline will run  B<ibis_noisy_obt_calc> on this file to calculate the on board time and write it to the raw input file.  It then write protects and adds the result to the corresponding index.

=cut

sub DPirn {
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	&ISDCPipeline::PipelineStep (
		"step"           => "$proc - IBIS ibis_noisy_obt_calc",
		"program_name"   => "ibis_noisy_obt_calc",
		"par_rawMapDOL"  => "raw/$dataset"."[ISGR-NOIS-CRW]",
		"par_prpMapName" => "prp/isgri_prp_noise_$stamp.fits",
		);
	
	return;
} # end of DPirn


##########################################################################
=item B<ICAirn> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Inst. Config. Anal. for ibis_raw_noise files:

The index of ISGRI context groups is collected (or the pipeline stops with an error if it does not exist, since the context table must be sent and processed before the noisy pixel maps.)  If previous (prepared) noisy pixel maps exist, an index of those is also collected.  If previous pixel switch lists exist, an index is collected of those as well.  Finally, the executable B<ibis_isgr_nois_map> is given the three indices and the current noisy pixel, and it creates a new pixel switch list.  This is then added to the corresponding index.  

Though B<ibis_isgr_nois_map> has been delivered, it has yet to be modified for the Revolution File Pipeline.  

=cut

sub ICAirn {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	my $prevrevdir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$prevrev/rev" );
	
	my $isgr_context_index = "$ENV{OUTPATH}/scw/$revno/rev.000/idx/isgri_context_index.fits";
	my $working_context_index = "";
	if (-e "$isgr_context_index") {

		&ISDCPipeline::FindIndex (
			"workname" => "working_context_index.fits",
			"template" => "ISGR-CTXT-GRP-IDX.tpl",
			"index"    => "$isgr_context_index",
			"sort"     => "CTXT_OBT",
			#  New contexts are also in same index
			"select"   => "CTXT_ORG == 'TM_DUMP'",
			);
		$working_context_index = "working_context_index.fits[GROUPING]";
	}
	else {
		&Message ( "WARNING:  No ISGRI context index found;  continuing without." );
	}
	my $working_maps_index = "";
	if (-e "$ENV{OUTPATH}/scw/$revno/rev.000/idx/isgri_prp_noise_index.fits") {
		$working_maps_index = "$ENV{OUTPATH}/scw/$revno/rev.000/idx/isgri_prp_noise_index.fits" ;
	}
	elsif ( ( -e "$prevrevdir/idx/isgri_prp_noise_index.fits" ) 
		|| ( -e "$prevrevdir/idx/isgri_prp_noise_index.fits.gz" ) ) {
		$working_maps_index = "$prevrevdir/idx/isgri_prp_noise_index.fits";
	}
	
	#  if previous results exist, collect 
	if ($working_maps_index) {
		&ISDCPipeline::CollectIndex (
			"workname" => "working_maps_index.fits",
			"template" => "ISGR-NOIS-CPR-IDX.tpl",
			"index"    => "$working_maps_index",
			"sort"     => "OBTFIRST",
			);
		$working_maps_index = "working_maps_index.fits[GROUPING]";
	}
	my $working_pxlswtch_index = "";
	if (-e "$ENV{OUTPATH}/scw/$revno/rev.000/idx/isgri_pxlswtch_index.fits") {
		$working_pxlswtch_index = "$ENV{OUTPATH}/scw/$revno/rev.000/idx/isgri_pxlswtch_index.fits" ;
	}
	elsif ( ( -e "$prevrevdir/idx/isgri_pxlswtch_index.fits" )
		|| ( -e "$prevrevdir/idx/isgri_pxlswtch_index.fits.gz" ) ) {
		$working_pxlswtch_index = $prevrevdir."/idx/isgri_pxlswtch_index.fits";
	}
	
	#if previous results exist, collect 
	if ($working_pxlswtch_index) {
		&ISDCPipeline::CollectIndex (
			"workname" => "working_pxlswtch_index.fits",
			"template" => "ISGR-SWIT-STA-IDX.tpl",
			"index"    => "$working_pxlswtch_index",
			"sort"     => "OBTFIRST",
			);
		$working_pxlswtch_index = "working_pxlswtch_index.fits[GROUPING]";
	}
	
	&ISDCPipeline::PipelineStep (
		"step"            => "$proc - IBIS ibis_isgr_nois_map",
		"program_name"    => "ibis_isgr_nois_map",
		"par_newMapDOL"   => "prp/isgri_prp_noise_$stamp.fits[ISGR-NOIS-CPR]",
		"par_outPixList"  => "cfg/isgri_pxlswtch_$stamp.fits",
		"par_idxCtxt"     => "$working_context_index",
		"par_idxNoisyMap" => "$working_maps_index",
		"par_idxPixList"  => "$working_pxlswtch_index",
		"par_nPeriod"     => "6",
		);
	
	chdir $workdir;
	unlink(glob("working*"));
	
} # end of ICAirn


##########################################################################

=item B<DPirc> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for isgri_raw_cal files:

The ISGRI CDTE group is created using B<dal_create> and the raw file attached.  Then the executable B<ibis_cdte_obt_calc> is run.  The group is then write protected and indexed.  

=cut

sub DPirc {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - write protect raw file",
		"program_name" => "$mychmod -w raw/$dataset",
		"subdir"       => "$workdir",
		);
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - ISGRI create CDTE group",
		"program_name" => "dal_create",
		"par_obj_name" => "prp/isgri_prp_cal_$stamp",
		"par_template" => "ISGR-CTPR-GRP.tpl",
		);

	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - ISGRI attach raw to group",
		"program_name" => "dal_attach",
		"par_Parent"   => "prp/isgri_prp_cal_$stamp.fits[GROUPING]",
		"par_Child1"   => "raw/$dataset"."[ISGR-CDTE-CRW]",
		"par_Child2"   => "raw/$dataset"."[ISGR-CDTE-PRW]",
		"par_Child3"   => "",
		"par_Child4"   => "",
		"par_Child5"   => "",
		);
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - ISGRI ibis_cdte_obt_calc",
		"program_name" => "ibis_cdte_obt_calc",
		"par_inGrp"    => "",
		"par_outGrp"   => "prp/isgri_prp_cal_$stamp.fits[GROUPING]",
		"par_outPrp"   => "",
		"par_clobber"  => "no",
		);
	
	return;
	
} # end of DPirc


##########################################################################

=item B<ACAirc> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Auto. Calib. Anal. for isgri_raw_cal
( arc_prep trigger) both for NRT and CONS.

=cut

sub ACAirc {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my $revstart = "";
	my $revstop  = "";
	
	my ($retval,@result) = &ISDCPipeline::PipelineStep (
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
		last;
	}

	my $revtime  = $revstart;
	
	my $isgricdtecor     = "osm/isgri_cdte_cor.fits";						
	my $isgricdtecor_DOL = "osm/isgri_cdte_cor.fits[ISGR-CDTE-COR]";	

	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - ISGRI create CDTE-COR",
		"program_name" => "dal_create",
		"par_obj_name" => "$isgricdtecor",
		"par_template" => "ISGR-CDTE-COR.tpl",
		);
	&Error ( "$isgricdtecor does not exist." ) unless ( -w $isgricdtecor );	
	
	my $isgrrisemod = &ISDCPipeline::GetICFile (
		"structure" => "ISGR-RISE-MOD",	#	like ic/ibis/cal/ibis_isgr_rt_corr_0007.fits[ISGR-RISE-MOD,1,BINTABLE]
		"select"    => "( VSTART <= $revtime ) && (VSTOP >= $revtime)",
		);
	&Error ( "No IC file ISGR-RISE-MOD found." ) unless ( $isgrrisemod );
	
	my $isgroffsmod = &ISDCPipeline::GetICFile (
		"structure" => "ISGR-OFFS-MOD",	#	like ic/ibis/cal/ibis_isgr_gain_offset_0009.fits[ISGR-OFFS-MOD,1,BINTABLE]
		"select"    => "( VSTART <= $revtime ) && (VSTOP >= $revtime)",
		);
	&Error ( "No IC file ISGR-OFFS-MOD found." ) unless ( $isgroffsmod );

	my $isgrdropmod = &ISDCPipeline::GetICFile (
		"structure" => "ISGR-DROP-MOD",
		"select"    => "( VSTART <= $revtime ) && (VSTOP >= $revtime)",
		);
	&Error ( "No IC file ISGR-DROP-MOD found." ) unless ( $isgrdropmod );
	
	&ISDCPipeline::FindIndex (
		"index"    => "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits",
		"select"   => "REVOL == $revno",
		"workname" => "working_prpscws_index.fits",
		"sort"     => "TSTART",
		"subdir"   => "$workdir",
		"required" => 0,
		) if ( (-e "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits") && ( ! -e "working_prpscws_index.fits" ) );
	&Error ( "No index created for ibis_isgr_cal_energy." ) unless ( -e "working_prpscws_index.fits" );	
	my $working_scws_indexDOL = "working_prpscws_index.fits[GROUPING]";

	&ISDCPipeline::PipelineStep (	#	VERSION 3.0 will not work here
		"step"           => "$proc - IBIS ISGRI RAW CALIBRATION energy corrections",
		"program_name"   => "ibis_isgr_cal_energy",
		"par_inGRP"      => "$ENV{SCWDIR}/$revno/rev.000/idx/isgri_prp_cal_index.fits[GROUPING]",		#       a link to the latest index
		"par_outCorEvts" => "$isgricdtecor_DOL",
		"par_hkCnvDOL"   => "$working_scws_indexDOL",		
		"par_riseDOL"    => "$isgrrisemod",
		"par_eraseALL"   => "y",
		"par_chatter"    => "2",
		"par_pathBipar"  => "",			
		"par_icDOL"      => "$isgrdropmod",
		"par_GODOL"      => "$isgroffsmod",
		) if (( -w "$isgricdtecor" ) && ( -r "$ENV{SCWDIR}/$revno/rev.000/idx/isgri_prp_cal_index.fits" ));
	
	if ( -r $isgricdtecor ) {			
		my $rows = &ISDCLIB::RowsIn ( $isgricdtecor_DOL );
		print ">>>>>>>     There are $rows rows in $isgricdtecor_DOL;  ";
		if ($rows == 0) {
			print "deleting $isgricdtecor.\n";
			unlink "$isgricdtecor";
		} elsif ($rows <= 0) {
			&Error ( "ERROR examining $isgricdtecor_DOL; cannot determine number of rows in $isgricdtecor_DOL" );
		} else {
			print "NOT deleting $isgricdtecor.\n";
		}
	}
	
	if ( -r $isgricdtecor ) {
		#
		#	get ISGRI pixel switch lists
		#
		my $working_pxlswtch_index = "";
		my $revfiledir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$revno/rev" );
		my $prevrevfiledir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$prevrev/rev" );
		
		if (-e "$revfiledir/idx/isgri_pxlswtch_index.fits") {
			$working_pxlswtch_index = "$revfiledir/idx/isgri_pxlswtch_index.fits" ;
		} elsif ( (-e "$prevrevfiledir/idx/isgri_pxlswtch_index.fits") 
			|| (-e "$prevrevfiledir/idx/isgri_pxlswtch_index.fits.gz") ) {
			$working_pxlswtch_index = "$prevrevfiledir/idx/isgri_pxlswtch_index.fits";
		}

		#  if previous results exist, make index and run isgr_evts_tag
		if ($working_pxlswtch_index) {
			unlink "working_pxlswtch_index.fits" if (-e "working_pxlswtch_index.fits");
			&ISDCPipeline::FindIndex (
				# temp index named for dataset, since parallel processing...
				"workname" => "working_pxlswtch_index.fits",
				"template" => "ISGR-SWIT-STA-IDX.tpl",
				"index"    => "$working_pxlswtch_index",
				"sort"     => "OBTFIRST"
				);
			$working_pxlswtch_index = "working_pxlswtch_index.fits"."[GROUPING]";
				&ISDCPipeline::PipelineStep (
					"step"             => "$proc - ibis ibis_isgr_evts_tag",
					"program_name"     => "ibis_isgr_evts_tag",
					"par_inGRP"        => "",
					"par_outGRP"       => "$isgricdtecor_DOL",
					"par_isgrRawEvts"  => "",
					"par_isgrPrpEvts"  => "",
					"par_isgrCorEvts"  => "",
					"par_idxSwitch"    => "$working_pxlswtch_index",
					"par_isgrSeleEvts" => "",
					"par_seleEXT"      => "ISGR-CDTE-COR",
					"par_seleCol"      => "SELECT_FLAG",
					"par_probShot"     => "0.001"
					);
		} else {
			# if no pixel switch lists, continue, but without isgr_evts_tag (useless)
			&Message ( "WARNING:  skipping ibis ibis_isgr_evts_tag;  no pixel switch list found" );
		}
	} else {
		&Message ( "WARNING:  skipping ibis ibis_isgr_evts_tag;  $isgricdtecor not found." );
	}
} # end of sub ACAirc


##########################################################################

=item B<DPidp> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Data Preparation for ibis_raw_dump files:

The executable B<dp_obt_calc> is run on the raw dump, then the context group is created using B<dal_create>, and then both are given to the executable B<ibis_isgr_dump_decod> which creates the ISGRI context table.  The PICsIT context is similarly created with the executable B<ibis_pics_dump_decod> and the IASW with B<ibis_iasw_dump_decod>.  All are then write protected and indexed.  

At this time, only B<ibis_isgr_dump_decod> exists and is implemented.  

=cut

sub DPidp {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my $ground = "";
	
	&ISDCPipeline::PipelineStep (
		"step"                => "$proc - IBIS memory dump OBT",
		"program_name"        => "dp_obt_calc",
		"par_InSWGroup"       => "",
		"par_OutSWGroup"      => "",
		"par_RawData"         => "",
		"par_ConvertedData"   => "",
		"par_AttributeData"   => "raw/$dataset"."[IBIS-DUMP-CRW]",
		"par_TimeInfo"        => "",
		"par_IN_STRUCT_NAME"  => "",
		"par_OUT_STRUCT_NAME" => "",
		"par_ATT_STRUCT_NAME" => "IBIS-DUMP-CRW",
		"par_LOBT_2X4_NAMES"  => "",
		"par_LOBT_1X8_NAMES"  => "",
		"par_PKT_NAMES"       => "",
		"par_LOBT_ATTR"       => "",
		"par_PKT_ATTR"        => "PCKSTART OBTSTART PCKEND OBTEND",
		"par_OBT_TYPE"        => ""
		);
	
	# SPR 2655:  need times to get correct contexts
	my $ertfirst = &ISDCPipeline::GetAttribute ( "raw/$dataset"."[IBIS-DUMP-CRW]", "ERTFIRST" );
	my $ijdfirst = &ISDCPipeline::ConvertTime (
		"informat"  => "UTC",
		"intime"    => "$ertfirst",
		"outformat" => "IJD",
		"dol"       => "",
		"accflag"   => "3",
		);
	
	############
	#
	#  ISGRI decoding
	#
	############
	if (defined($ENV{ISGR_GRNDCTXT_COMP}) && ($ENV{ISGR_GRNDCTXT_COMP} =~ /^[1-3]$/)) {
		print "*******     ISGR_GRNDCTXT_COMP is $ENV{ISGR_GRNDCTXT_COMP};  "
			."using ground context as input to ibis_isgr_dump_decod for comparison.\n";
		
		#  Get the ground (i.e. IC) context for comparison:
		$ground = &ISDCPipeline::GetICFile (
			"structure" => "ISGR-CTXT-GRP",
			"select"    => "( VSTART <= $ijdfirst ) && (VSTOP >= $ijdfirst)",
			);
		&Error ( "No IC file ISGR-CTXT-GRP found." ) unless ($ground);	
		
	}
	else {
		print "*******     ISGR_GRNDCTXT_COMP is $ENV{ISGR_GRNDCTXT_COMP};  "
			."NOT giving ground context as input to ibis_isgr_dump_decod\n" 
			if (defined($ENV{ISGR_GRNDCTXT_COMP}));
		print "*******     ISGR_GRNDCTXT_COMP is not defined;  NOT giving ground "
			."context as input to ibis_isgr_dump_decod\n" 
			unless (defined($ENV{ISGR_GRNDCTXT_COMP}));
		#  Still have to give an integer for the alert level, or PIL error.
		#   In this case, though, the ground context isn't given, so no alerts
		#   will be sent anyway.
		$ENV{ISGR_GRNDCTXT_COMP} = "0";
	}
	
	my ($retval,@result) = &ISDCPipeline::PipelineStep (
		"step"               => "$proc - ibis_isgr_dump_decod",
		"program_name"       => "ibis_isgr_dump_decod",
		"par_chatter"        => "3",								
		"par_dumpDOL"        => "raw/$dataset"."[IBIS-DUMP-CRW]",
		"par_groundCtxt"     => "$ground",
		"par_newCtxtName"    => "cfg/isgri_context_$stamp.fits(ISGR-CTXT-GRP.tpl)",
		"par_emailIC"        => "$ENV{ISGR_DUMP_EMAIL}",
		"par_percentPresent" => "100.0",
		"par_alertLevel"     => "$ENV{ISGR_GRNDCTXT_COMP}",
		"subdir"             => "$workdir",
		"stoponerror"        => "0"
		);

	if ( $retval ) {			#	if $retval is NOT ISDC_OK
		if (`$myls $workdir/*alert* 2> /dev/null`) {
			&Message ( "WARNING: found alert from ibis_isgr_dump_decod; copying." );
			&ISDCPipeline::PipelineStep (
				"step"           => "$proc - am_cp",
				"program_name"   => "am_cp",
				"par_OutDir"     => "$ENV{ALERTS}",
				"par_OutDir2"    => "$ENV{SCWDIR}/$revno/rev.000/logs/",
				"par_Subsystem"  => "REV",
				"par_DataStream" => "realtime",
				"par_ScWIndex"   => "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits[1]",
				"subdir"         => "$workdir",
				);
		}
		&Error ( "Return status of $retval from ibis_isgr_dump_decod is not allowed." );
	}
	
	############
	#
	#  PICsIT decoding
	# 
	############
	$ground = "";
	if (defined($ENV{PICS_GRNDCTXT_COMP}) && ($ENV{PICS_GRNDCTXT_COMP} =~ /^[1-3]$/)) {
		print "*******     PICS_GRNDCTXT_COMP is $ENV{PICS_GRNDCTXT_COMP};  "
			."using ground context as input to ibis_pics_dump_decod for comparison.\n";
		
		#  Get the ground (i.e. IC) context for comparison:
		$ground = &ISDCPipeline::GetICFile (
			"structure" => "PICS-CTXT-GRP",
			"select"    => "( VSTART <= $ijdfirst ) && (VSTOP >= $ijdfirst)",
			);
		&Error ( "No IC file PICS-CTXT-GRP found." ) unless ($ground);	
		
	}
	else {
		print "*******     PICS_GRNDCTXT_COMP is $ENV{PICS_GRNDCTXT_COMP};  "
			."NOT giving ground context as input to ibis_pics_dump_decod\n" 
			if (defined($ENV{PICS_GRNDCTXT_COMP}));
		print "*******     PICS_GRNDCTXT_COMP is not defined;  NOT giving "
			."ground context as input to ibis_pics_dump_decod\n" 
			unless (defined($ENV{PICS_GRNDCTXT_COMP}));
		#  Still have to give an integer for the alert level, or PIL error.
		#   In this case, though, the ground context isn't given, so no alerts
		#   will be sent anyway.
		$ENV{PICS_GRNDCTXT_COMP} = "0";
	}
	
	&ISDCPipeline::PipelineStep (
		"step"               => "$proc - ibis_pics_dump_decod",
		"program_name"       => "ibis_pics_dump_decod",
		"par_dumpDOL"        => "raw/$dataset"."[IBIS-DUMP-CRW]",
		"par_ctxtRef"        => "$ground",
		"par_ctxtName"       => "cfg/picsit_context_$stamp.fits",
		"par_emailIC"        => "$ENV{PICS_DUMP_EMAIL}",
		"par_percentPresent" => "93.4",
		"par_alertLevel"     => "$ENV{PICS_GRNDCTXT_COMP}",
		"par_chatter"        => "3",					
		);
	
	###########
	#
	#  HEPI decoding
	#
	###########
	
	$ground = "";
	if (defined($ENV{HEPI_GRNDCTXT_COMP}) && ($ENV{HEPI_GRNDCTXT_COMP} =~ /^[1-3]$/) ) {
		print "*******     HEPI_GRNDCTXT_COMP is $ENV{HEPI_GRNDCTXT_COMP};  "
			."using ground context as input to ibis_hepi_dump_decod for comparison.\n";
		
		#  Get the ground (i.e. IC) context for comparison:
		$ground = &ISDCPipeline::GetICFile (
			"structure" => "PICS-HEPI-GRP",
			"select"    => "( VSTART <= $ijdfirst ) && (VSTOP >= $ijdfirst)",
			);
	}
	else {
		print "*******     HEPI_GRNDCTXT_COMP is $ENV{HEPI_GRNDCTXT_COMP};  "
			."NOT giving ground context as input to ibis_hepi_dump_decod\n" 
			if (defined($ENV{HEPI_GRNDCTXT_COMP}));
		print "*******     HEPI_GRNDCTXT_COMP is not defined;  "
			."NOT giving ground context as input to ibis_hepi_dump_decod\n" 
			unless (defined($ENV{HEPI_GRNDCTXT_COMP}));
		$ENV{HEPI_GRNDCTXT_COMP} = 0;
	}
	
	&ISDCPipeline::PipelineStep (
		"step"               => "$proc - ibis_hepi_dump_decod",
		"program_name"       => "ibis_hepi_dump_decod",
		"par_dumpDOL"        => "raw/$dataset"."[IBIS-DUMP-CRW]",
		"par_ctxtRef"        => "$ground",
		"par_ctxtName"       => "cfg/hepi_context_$stamp.fits",
		"par_emailIC"        => "$ENV{HEPI_DUMP_EMAIL}",
		"par_percentPresent" => "20.0",
		"par_alertLevel"     => "$ENV{HEPI_GRNDCTXT_COMP}",
		"par_chatter"        => "3",						
		);
	
	###########
	#
	#  VETO decoding
	#
	###########
	
	$ground = "";
	if (defined($ENV{VETO_GRNDCTXT_COMP}) && ($ENV{VETO_GRNDCTXT_COMP} =~ /^[1-3]$/) ) {
		print "*******     VETO_GRNDCTXT_COMP is $ENV{VETO_GRNDCTXT_COMP};  "
			."using ground context as input to ibis_veto_dump_decod for comparison.\n";
		
		#  Get the ground (i.e. IC) context for comparison:
		$ground = &ISDCPipeline::GetICFile (
			"structure" => "IBIS-VCTX-GRP",
			"select"    => "( VSTART <= $ijdfirst ) && (VSTOP >= $ijdfirst)",
			);
	}
	else {
		print "*******     VETO_GRNDCTXT_COMP is $ENV{VETO_GRNDCTXT_COMP};  "
			."NOT giving ground context as input to ibis_veto_dump_decod\n" 
			if (defined($ENV{VETO_GRNDCTXT_COMP}));
		print "*******     VETO_GRNDCTXT_COMP is not defined;  "
			."NOT giving ground context as input to ibis_veto_dump_decod\n" 
			unless (defined($ENV{VETO_GRNDCTXT_COMP}));
		$ENV{VETO_GRNDCTXT_COMP} = 0;
	}
	
	&ISDCPipeline::PipelineStep (
		"step"               => "$proc - ibis_veto_dump_decod",
		"program_name"       => "ibis_veto_dump_decod",
		"par_dumpDOL"        => "raw/$dataset"."[IBIS-DUMP-CRW]", 
		"par_ctxtRef"        => "$ground",
		"par_ctxtName"       => "cfg/veto_context_$stamp.fits",
		"par_emailIC"        => "$ENV{VETO_DUMP_EMAIL}",
		"par_percentPresent" => "6.0",
		"par_alertLevel"     => "$ENV{VETO_GRNDCTXT_COMP}",
		"par_chatter"        => "3",						
		);
	
	###########
	#
	#  IASW decoding:  
	#
	###########
	
	$ground = "";
	if (defined($ENV{IASW_GRNDCTXT_COMP}) && ($ENV{IASW_GRNDCTXT_COMP} =~ /^[1-3]$/) ) {
		print "*******     IASW_GRNDCTXT_COMP is $ENV{IASW_GRNDCTXT_COMP};  "
			."using ground context as input to ibis_iasw_dump_decod for comparison.\n";
		
		#  Get the ground (i.e. IC) context for comparison:
		$ground = &ISDCPipeline::GetICFile (
			"structure" => "ISGR-LUT.-GRP",
			"select"    => "( VSTART <= $ijdfirst ) && (VSTOP >= $ijdfirst)",
			);
	}
	else {
		print "*******     IASW_GRNDCTXT_COMP is $ENV{IASW_GRNDCTXT_COMP};  "
			."NOT giving ground context as input to ibis_iasw_dump_decod\n" 
			if (defined($ENV{IASW_GRNDCTXT_COMP}));
		print "*******     IASW_GRNDCTXT_COMP is not defined;  "
			."NOT giving ground context as input to ibis_iasw_dump_decod\n" 
			unless (defined($ENV{IASW_GRNDCTXT_COMP}));
		$ENV{IASW_GRNDCTXT_COMP} = 0;
	}
	
	&ISDCPipeline::PipelineStep (
		"step"               => "$proc - ibis_iasw_dump_decod",
		"program_name"       => "ibis_iasw_dump_decod",
		"par_dumpDOL"        => "raw/$dataset"."[IBIS-DUMP-CRW]", 
		"par_ctxtRef"        => "$ground",
		"par_ctxtName"       => "cfg/iasw_context_$stamp.fits",
		"par_emailIC"        => "$ENV{IASW_DUMP_EMAIL}",
		"par_percentPresent" => "11.0",
		"par_alertLevel"     => "$ENV{IASW_GRNDCTXT_COMP}",
		"par_chatter"        => "3",						
		);
	
	return;
	
} # end of DPidp


##########################################################################

=item B<ICAidp{NRT,CONS}> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Inst.Config.Anal. for ibis_raw_dump files:
It is called for each dump during *both* NRT and CONS.

If a PICsIT context was created in DP, the following is done:

=over 5

The raw science windows are collected for the previous three days using B<idx_find>.  If there is no previous pixel fault list in the pipeline workspace (where a copy is stored of the last), an empty first version is created with B<dal_create>.  The executable B<ibis_pics_fault_check> uses both and the PICsIT context group to create a new pixel fault list.  The result is copied to the workspace.  

In NRT, this is done with individual context groups, while in Consolidated, it will be done with an index of all of them at the end of the revolution.  (The latter ensures that all data is present.)  

=back

If an ISGRI context was created in DP and the processing is NRT, the following is done:

=over 5

The executable B<ibis_isgr_low_thres> is given the context and the last index of noisy pixel maps (if there exists one from the previous revolution.)  This creates a new context group, which is given to the B<ibis_isgr_ctxt2obsms> executable to convert it into a new OBSMS file.  

There is no processing done one the ISGRI context table in Consolidated processing.

=back

=cut

sub ICAidp {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my $prevrevdir = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$prevrev/rev" );
	
	my ($retval,$obtacq);
	
	###
	#   PICsIT dump
	###
	
	#
	#  if a PICsIT file resulted from the decode, do the fault check
	#
	if (-e "$workdir/cfg/picsit_context_$stamp.fits") {
		
		($retval,$obtacq) = &ISDCPipeline::GetAttribute ("raw/$dataset"."[IBIS-DUMP-CRW]", "OBTSTART" );
		&Error ( "Cannot find OBTSTART of $dataset:\n\n$obtacq" ) if ($retval);	
		my $delta = $ENV{PICSIT_ICA_DELTA} * 2**20;
		#  This is necessary to get the math to work right with such big
		#  integers.  (They are 4 bytes, Perl handles 2 bytes only (I think.))
		my $diff = &ISDCPipeline::DiffOBTs ( $obtacq, $delta );
		
		&ISDCPipeline::FindIndex (
			"index"    => "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits",				
			"select"   => "OBTEND > \'$diff\' && REVOL==$revno",
			"workname" => "working_prpscws_index.fits",
			"sort"     => "OBTEND",
			"subdir"   => "$workdir",
			"required" => 0,
			) if (-e "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits");				
		# this will take the last fault list as input along with the current 
		#  decoded dump and the index of science windows created by DP 
		#  and write out a new fault list;  it will take an empty string
		#  for *any* of the input parameters except the name of the result.  
		#
		#  In this NRT case, the context will always be input (but in CONS, 
		#  only the ScWs index.  
		
		my $working_prpscws_index = "working_prpscws_index.fits[GROUPING]" 
			if (-e "working_prpscws_index.fits");

		my @flt_lists = sort(glob("$ENV{SCWDIR}/$revno/rev.000/cfg/picsit_fault_list*.fits"));
		@flt_lists = sort(glob($prevrevdir."/cfg/picsit_fault_list*.fits")) unless (@flt_lists);
		
		my $flt_list = $flt_lists[$#flt_lists]."[PICS-FALT-STA]" if (@flt_lists);
		
		&ISDCPipeline::PipelineStep (
			"step"           => "$proc - IBIS ibis_pics_fault_check",
			"program_name"   => "ibis_pics_fault_check",
			"par_ctxtDOL"    => "cfg/picsit_context_$stamp.fits[GROUPING]",
			"par_idxSwgDOL"  => "$working_prpscws_index",
			"par_faultyDOL"  => "$flt_list",
			"par_faultyName" => "cfg/picsit_fault_list_$stamp.fits",
			"subdir"         => "$workdir",
			);
		
		&ISDCPipeline::PutAttribute ( "cfg/picsit_fault_list_$stamp.fits[1]", "REVOL", "$revno", "DAL_INT", "Revolution number (set by pipeline)" ) 
			if ( -e "cfg/picsit_fault_list_$stamp.fits" );
		
		unlink(glob("working*"));
		
	} # done with PICsIT context
	else {
		&Message ( "no PICsIT context;  skipping PICsIT fault handling" );
	}
	
	###
	#   ISGRI dump
	###
	
	#  If an ISGRI dump came out, and only for NRT, set up the ILT trigger:  
	if ( (-e "$workdir/cfg/isgri_context_$stamp.fits") && ($proc =~ /NRT/))  {
		my %ilts;
		my $iltvers = 0;
		my $lastvers = 0;
		my $ilt;
		my $vers;
		##
		##  All we do here is create the ILT OSF.  nrvmon will wake up and look
		##   at that time trigger, and when enough time has passed since, it will
		##   change the status of the ILT OSF from cchwww to ccwwww
		##   and the processing is done in RevIBIS::ICAiltNRT.  
		##
		##  Note that we use the DCF here.  Checking the repository isn't reliable,
		##   since perhaps finish has a problem, and we must keep track of ILT
		##   versions for the tar file sent to MOC.  
		
		## Check for any others:
		
		my ($retval,@result) = &ISDCPipeline::RunProgram ( "osf_test -p nrtrev.path -t ilt -pr dataset dcf_num" );
		&Error ( "Cannot check blackboard for other ilts:\n@result" ) if ($retval);
		
		if (scalar(@result)) {
			print "*******     There are ".scalar(@result)." other ILTs on the blackboard;  "
				."looking for revolution $revno....found:\n";
			#  Each line returned is:
			#   <dataset> <dcf>
			#  but we don't know what order they'll come in from osf_test nor
			#  whether the order of the stamps is the order of the versions
			#  (if for example, one had a problem and was later fixed...)
			#
			#  So we need to get the last version/DCF of the current revolution:
			foreach (@result) { 
				next unless (/^$revno/); 
				chomp;
				print "$_\n";
				#  Watch it!  Trailing spaces left by osf_test and chomp.
				#  (Remember the ? means minimal matching.)
				if (/^(.*?)\s(.*?)\s+$/) { 
					$ilt = $1;
					$vers = $2;
					$ilts{$ilt} = $vers;
					print "*******     Looking at $ilt with version $vers\n";
					$lastvers = $vers unless ($lastvers > $vers);
					print "*******     Lastversion now $lastvers.\n";
				}
				else {
					&Error ( "Cannot parse $_" );
				}
			}
			
			print "*******     There are ".scalar(keys(%ilts))." other ILTs for "
			."revolution $revno on the blackboard\n";

			&Error ( "Found ".scalar(keys(%ilts))." ILTs on blackboard, but last version is $lastvers;  something's wrong here!" )
					if ((scalar(keys(%ilts)) != $lastvers) && ($lastvers > 0));
			
		} # end of if results of osf_test
		else {
			print "*******     No previous ILT triggers found on the blackboard.\n";
		}
		
		#  Now, set version for next ILT trigger:
		$iltvers = $lastvers + 1;
		
		#  Check that this isn't a ridiculous number;  we only expect a couple 
		#   in each revolution.   (And only one digit of version is expected;  
		#   see ICAiltNRT below.)
		#  Leave the 9th free for a manual intervention in the case of severely
		#   odd behavior.  
		if ( $iltvers > 1 ) {
			my @dumps = sort(glob("$ENV{SCWDIR}/$revno/rev.000/raw/ibis_raw_dump*"));
			my $count;
			foreach (@dumps) { $count++;  last if (/$stamp/);}
			
			&ISDCPipeline::WriteAlert (
				"step"    => "$proc - WARNING:  already at least 8 ISGRI dumps;  skipping low threshold analysis.",
				"message" => "Received ${count}th IBIS dump for revolution $revno.",
				"level"   => 2,
				"id"      => "3001",
				);
			return;
		}
		&ISDCPipeline::PipelineStep (
			"step"         => "$proc - create ILT OSF",
			"program_name" => "osf_create -p nrtrev.path -f ${revno}_${stamp}_00_ilt "
				."-t ilt -n $iltvers -s $osf_stati{REV_GEN_H}",	
			);
		#  To fake it and make sure the trigger is there:
		&ISDCPipeline::PipelineStep (
			"step"         => "$proc - create trigger ILT",
			"program_name" => "$mytouch $ENV{REV_INPUT}/${revno}_${stamp}_00_ilt.trigger_processing",
			);
		
		#  set up log, since startup not run
		open(ILT_LOG,">$ENV{SCWDIR}/$revno/rev.000/logs/ibis_ilt_${stamp}_00_log.txt") 
			or &Error ( "Cannot open log to write at $ENV{SCWDIR}/$revno/rev.000/logs/ibis_ilt_${stamp}_00_log.txt" );
		
		print ILT_LOG 
		"-----   ".&TimeLIB::MyTime().":  Starting pipeline with:\n";
		print ILT_LOG "-----   Path file         : $ENV{PATH_FILE_NAME}\n";
		print ILT_LOG "-----   Input             : $dataset\n";
		print ILT_LOG "-----   Type              : $type\n";
		close ILT_LOG;
		symlink "$ENV{SCWDIR}/$revno/rev.000/logs/ibis_ilt_${stamp}_00_log.txt", 
			"$ENV{OPUS_WORK}/nrtrev/logs/${revno}_${stamp}_00_ilt.log";
	}
	
	elsif ($proc =~ /CONS/) {
		print "*******     CONS case;  no ILT processing.\n";
	}
	else {
		&Message ( "No ISGRI context;  skipping ISGRI low threshold processing" );
	}
} # end of ICAidp


##########################################################################

=item B<ICAiltNRT> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

This function performs the Inst.Config.Anal. for ISGRI:
ILT generation, using ilt OSF created by ICAidp

=cut

sub ICAiltNRT {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my ($retval,@result);	
	my $prev_nois_index;
	
	#  Two digit tail of tar file version:
	print "*******     OSF_DCF_NUM is $ENV{OSF_DCF_NUM}\n";
	my $iltvers = $ENV{OSF_DCF_NUM};
	if (!defined($ENV{OSF_DCF_NUM})) {
		$iltvers = "1";
	}
	#  Prep normally done by DP:
	$workdir = "$ENV{WORKDIR}/$ENV{OSF_DATASET}";
	
	if (-e "$workdir") {
		chdir "$ENV{OPUS_WORK}" or &Error ( "Cannot chdir into $ENV{OPUS_WORK}!" );
		($retval,@result) = &ISDCPipeline::RunProgram ( "$myrm -rf $workdir" );
		&Error ( "Cannot clean $workdir:\n@result" ) if ($retval);	
	}

	&ISDCLIB::DoOrDie ( "$mymkdir -p $workdir/raw" ) unless ( -d "$workdir/raw" );
	chdir("$workdir") or &Error ( "Cannot chdir into new working dir $workdir!" );

	&ISDCLIB::DoOrDie ( "$mymkdir -p $workdir/cfg" ) unless ( -d "$workdir/cfg" );
	
	($retval,@result) = &ISDCPipeline::RunProgram ( "$mycp $ENV{OUTPATH}/scw/$revno/rev.000/cfg/isgri_context_$stamp.fits $workdir/cfg" );
	&Error ( "Cannot find $ENV{OUTPATH}/scw/$revno/rev.000/cfg/isgri_context_$stamp.fits:\n@result" ) 
		if ($retval);	
	
	# Get current rev's noise maps, whatever exists.  
	# pass empty parameter if none exist:
	if (-e "$ENV{SCWDIR}/$revno/rev.000/idx/isgri_prp_noise_index.fits") {
		&ISDCPipeline::CollectIndex (
			"workname" => "working_noise_index.fits",
			"template" => "ISGR-NOIS-CPR-IDX.tpl",
			"index"    => "$ENV{SCWDIR}/$revno/rev.000/idx/isgri_prp_noise_index.fits",
			);
	}      
	
	$prev_nois_index = "working_noise_index.fits[GROUPING]" if (-e "working_noise_index.fits");
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - copy old context to new",
		"program_name" => "$mycp cfg/isgri_context_$stamp.fits cfg/isgri_context_new_$stamp.fits",
		);
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - make writeable",
		"program_name" => "$mychmod +w cfg/isgri_context_new_$stamp.fits",
		);
	
	&ISDCPipeline::FindIndex (
		"index"    => "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits",				
		"select"   => "REVOL == $revno",
		"workname" => "working_prpscws_index.fits",
		"sort"     => "OBTEND",
		"subdir"   => "$workdir",
		"required" => 0,
		) if (-e "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits");				
	
	my $scw_index = "";
	if (-e "working_prpscws_index.fits") {
		$scw_index = "working_prpscws_index.fits[GROUPING]";
	} else {
		&Message ( "WARNING:  no index of prepared science window groups found;  skipping low threshold analysis" );
		return;
	}

	my $luts = &ISDCPipeline::GetICFile (
		"structure" => "ISGR-OFFS-MOD",
		"filematch" => "cfg/isgri_context_new_$stamp.fits[GROUPING]",
		"keymatch"  => "VSTART",
		);
	
	my $checkmode = 2;			
	
	#  Turns off checking which will lead to error if not enough data,
	#    allowing partial testing with small datasets in pipeline unit test. 
	$checkmode = 0 if ( (defined $ENV{ILT_UNITTEST}) && ($ENV{ILT_UNITTEST} == 1));
	
	my $isgr_dead_cfg_idx_DOL = "";
	if ( -e "$ENV{OUTPATH}/idx/rev/ISGR-DEAD-CFG-IDX.fits" ) {
		&ISDCPipeline::FindIndex (
			"index"    => "$ENV{OUTPATH}/idx/rev/ISGR-DEAD-CFG-IDX.fits",
			"select"   => "REVOL < $revno",
			"workname" => "working_isgridead_index.fits",
			"subdir"   => "$workdir",
			);																	
		$isgr_dead_cfg_idx_DOL = "working_isgridead_index.fits[GROUPING]";			
	}
	
	my $isgr_dead_cfg     = "cfg/isgri_context_dead_$stamp.fits";
	my $isgr_dead_cfg_DOL = "cfg/isgri_context_dead_$stamp.fits[ISGR-DEAD-CFG]";
	
	($retval, @result) = &ISDCPipeline::PipelineStep (
		"step" => "$proc - IBIS ibis_isgr_low_thres",
		"program_name"     => "ibis_isgr_low_thres",
		"par_chatter"      => "4",
		"par_checkMode"    => "$checkmode",
		"par_cutCount"     => "1000",
		"par_dead_out"     => "$isgr_dead_cfg",
		"par_evt_struct"   => "ISGR-EVTS-ALL",
		"par_maxRT"        => "128.0",
		"par_minRT"        => "6.0",
		"par_maxLT"        => "38.0",
		"par_minLT"        => "14.7",
 		"par_maxDead"      => "650",
		"par_maxNewDead"   => "9",
		"par_maxLTM"       => "99",
		"par_max_step_inc" => "12",
		"par_maxStep"      => "6",
		"par_meanExpect"   => "16.0",
		"par_meanREVon"    => "6.7",
		"par_ratioDEAD"    => "$ENV{ILT_RATIO_DEAD}",
		"par_ratioPeak"    => "$ENV{ILT_RATIO_PEAK}",
		"par_ratioEffic"   => "$ENV{ILT_RATIO_EFFIC}",
		"par_revolWait"    => "$ENV{ILT_REVOL_WAIT}",
		"par_whichLUT1"    => "2",
		"par_DOLisgriCtxt" => "cfg/isgri_context_new_$stamp.fits[GROUPING]",
		"par_DOLidx_S1"    => "$scw_index",
		"par_DOLidxNoisy"  => "$prev_nois_index",
		"par_DOLic_LUT1"   => "$luts",
		"par_DOLdead_in"   => "$isgr_dead_cfg_idx_DOL",
		"stoponerror"      => "0",
		);

	my $DoNotAttachIndex = "";
	if ( $retval ) {
		if (  ( $retval == 144910 )
			|| ( $retval == 144911 ) ) {
			#		If exit status is not ISDC_OK but strictly below -144909 (in fact -144910 or -144911)
			#		The context is OK and can be sent (next step is the program ibis_isgr_ctxt2obsms)
			#		BUT the created ISGR-DEAD-CFG has problem and must NOT be attached to the index.
			&Message ( "WARNING: ibis_isgr_low_thres returned $retval; problem with isgr_dead_cfg_DOL; continuing." );
			$DoNotAttachIndex .= "$isgr_dead_cfg ";		#	do NOT attach this isgri_context_dead to index
		} else {
			if (`$myls $workdir/*alert* 2> /dev/null`) {
				&Message ( "WARNING: found alert from ibis_isgr_low_thres; copying." );
				&ISDCPipeline::PipelineStep (
					"step"           => "$proc - am_cp",
					"program_name"   => "am_cp",
					"par_OutDir"     => "$ENV{ALERTS}",
					"par_OutDir2"    => "$ENV{SCWDIR}/$revno/rev.000/logs/",
					"par_Subsystem"  => "REV",
					"par_DataStream" => "realtime",
					"subdir"         => "$workdir",
					"par_ScWIndex"   => "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits[1]",
					);
			}
			&Error ( "Return status of $retval from ibis_isgr_low_thres is not allowed" );
		}
	}
	
	if ( -r $isgr_dead_cfg ) {			
		my $rows = &ISDCLIB::RowsIn ( $isgr_dead_cfg_DOL );
		print ">>>>>>>     There are $rows rows in $isgr_dead_cfg_DOL;  ";
		if ($rows == 0) {
			print "deleting it.\n";
			unlink "$isgr_dead_cfg";
		} elsif ($rows <= 0) {
			&Error ( "ERROR examining $isgr_dead_cfg_DOL; "
				."cannot determine number of rows in $isgr_dead_cfg_DOL" );
		} else {
			print "NOT deleting it.\n";
		}
	}
	
	&ISDCPipeline::PipelineStep (
		"step"                 => "$proc - IBIS convert ISGRI context to OBSMS",
		"program_name"         => "ibis_isgr_ctxt2obsms",
		"par_groundCtxtDOL"    => "cfg/isgri_context_new_$stamp.fits[GROUPING]",
		"par_obsmsName"        => "cfg/",
		"par_obsmsPhase"       => "V",
		"par_obsmsRelease"     => "",
		"par_obsmsOrderNumber" => "$iltvers",
		"par_details"          => "YES",
		);
	@result = sort(glob("cfg/*TPF"));
	$result[0] =~ /.*_(\d{14})_/;
	my $date = $1;
	
	&Error ( "Cannot parse TPF names like $result[0]" ) unless ($date);
	#  Tar file name should be:  IIMG_SDCMOC_Dyyyyddmmhhmmss_vvvvv.INT
	#   where vvvvv always starts 0000.  (This may change if obsmsOrderNumber
	#   goes away;  it's currently only one digit.)
	my $tar_file_name = "IIMG_SDCMOC_D${date}_0000${iltvers}.INT";
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - create tar file of TPFs",
		"program_name" => "$mytar cvf $tar_file_name *TPF", 
		"subdir"       => "$workdir/cfg",
		);
	
	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{TPF_DIR}" ) unless (-d "$ENV{TPF_DIR}");
	&Error ( "Cannot find directory $ENV{TPF_DIR}" ) unless (-d "$ENV{TPF_DIR}");
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - send tar file to IFTS outbox.tmp",
		"program_name" => "$mycp $tar_file_name $ENV{TPF_DIR}",
		"subdir"       => "$workdir/cfg",
		);

	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{IFTS_OUTBOX}" ) unless (-d "$ENV{IFTS_OUTBOX}");
	&Error ( "Cannot find directory $ENV{IFTS_OUTBOX}" ) unless (-d "$ENV{IFTS_OUTBOX}");
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - mv tar file to IFTS outbox",
		"program_name" => "$mymv $tar_file_name $ENV{IFTS_OUTBOX}",
		"subdir"       => "$ENV{TPF_DIR}",
		);
	
	
	if (defined($ENV{ILT_ALERT})) {

		&ISDCPipeline::WriteAlert (
			"step"    => "$proc - sending ILT alert",
			"message" => "TPF v$iltvers SENT for rev $revno dump $stamp",
			"subdir"  => "$workdir",
			"level"   => $ENV{ILT_ALERT},
			"id"      => "3045",
			);
	}
	
	return ( $DoNotAttachIndex );		
} # end of ICAiltNRT


##########################################################################

=item B<ICAidpHK>

This function performs the Inst.Config.Anal. for PICsIT faulty pixels using the HK data in the science windows just before archiving rev.  ( arc_prep trigger) both for NRT and CONS.

=cut

sub ICAidpHK {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my ($retval,@result);
	##
	#  Have to remember here that the dataset is the arc_prep trigger!  We are
	#   doing all faulty pixel checking here at once.  
	##
	
	#
	#  select science windows
	#	
	&ISDCPipeline::FindIndex (
		"index"    => "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits",				
		"select"   => "REVOL == $revno",
		"workname" => "working_prpscws_index.fits",
		"sort"     => "OBTEND",
		"subdir"   => "$workdir",
		);
	
	&Error ( "Index selection resulted in no members." ) 
		unless (-e "$workdir/working_prpscws_index.fits");
	
	# latest picsit fault list:
	chdir("$workdir") or &Error ( "Cannot chdir into working dir $workdir!" );	
	
	my @flt_lists;
	my $rev;
	my $flt_list = "";
	print "*******       Looking for PICsIT fault list starting in revolution $revno\n";
	for ($rev = $revno; $rev >= 0; $rev--) {
		$rev = sprintf("%04d",$rev);
		$rev = &ISDCLIB::FindDirVers ( "$ENV{SCWDIR}/$rev/rev" );
		@flt_lists = sort(glob("$rev/cfg/picsit_fault_list*.fits"));
		$flt_list = $flt_lists[$#flt_lists]."[PICS-FALT-STA]" if (@flt_lists);
		print "*******     Found $flt_list\n" if ($flt_list);
		last if ($flt_list);
	}
	
	#  run ibis_pics_fault_check.... 
	#
	
	&ISDCPipeline::PipelineStep (
		"step"           => "$proc - IBIS ibis_pics_fault_check",
		"program_name"   => "ibis_pics_fault_check",
		"par_ctxtDOL"    => "",
		"par_idxSwgDOL"  => "working_prpscws_index.fits[GROUPING]",
		"par_faultyDOL"  => "$flt_list",
		"par_faultyName" => "cfg/picsit_fault_list_$stamp.fits",
		"subdir"         => "$workdir",
		);
	
	&ISDCPipeline::PutAttribute ( "cfg/picsit_fault_list_$stamp.fits[1]", "REVOL",
		"$revno", "DAL_INT", "Revolution number (set by pipeline)" ) 
		if (-e "cfg/picsit_fault_list_$stamp.fits");
	
} # end of ICAidpHK


##########################################################################

=item B<DPprc>

This function performs the Data Preparation for picsit_raw_cal files:

The executable B<dp_obt_calc> is run on this file to write the on board time into the raw file.  The result is write protected and added to an index kept in the work directory.  

=cut

sub DPprc {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - IBIS ibis_csih_obt_calc",
		"program_name" => "ibis_csih_obt_calc",
		"par_csiHist"  => "raw/$dataset"."[GROUPING]",
		"par_accuracy" => "ANY",
		);
	
	return;
}  # end of DPprc



##########################################################################

1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrvdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

