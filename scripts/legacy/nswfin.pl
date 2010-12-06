#!perl

=head1 NAME

nswfin.pl - NRT Science Window Pipeline Finish

=head1 SYNOPSIS

I<nswfin.pl> - Run from within B<OPUS>.  This is the last step of a six stage pipeline which processes science windows.  This finishes the processing of the science window data.

=head1 DESCRIPTION

This process triggers when an OSF shows that the B<nswosm> step has completed for a science window.  It performs some miscellaneous tasks for finishing, and a few executables run here because they must only happen one at a time.  The PMG should only allow one instance of this process to run at a time, because it writes to files not contained within the science window.  

First, the HK averages are updated.  If the ouput file, in the revolution "prp" directory, does not exist, it is created using the GNRL_AVRG.ftpl.  Then B<dp_average> is run to add a row for the current science window containing the average values of selected HK parameters.

The science window of prepared data is then cleaned using B<swg_clean> to remove any empty data structures and detach them from the group.

The interactive OSM needs an index of all prepared science windows, so the next step is to add the current window to the index in REP_BASE_PROD/idx/obs_group.  A working copy is updated first using B<idx_add> and then renamed to be the next version in that directory.  (The first instance is created if none is yet there.)  The table is then sorted using an isdcroot call to DALtableSortRows.  

The resulting prepared and OSM data are write protected, and a trigger for archive ingest is created.  Lastly, the original science window trigger is renamed to "_done".  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location of the repository, i.e. REP_BASE_PROD usually.

=item B<WORKDIR>

This is set to the B<nrt_work> entry in the path file.  It is the location of the working directory, i.e. OPUS_WORK.

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the scw part of the repository.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the location of all log files seen by OPUS.  The real files are located in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the location of the pipeline parameter files.  

=item B<ARC_TRIG>

This is the directory in which to write triggers for archive ingest;  it is set to the B<arcingest> entry in the path file, usually into a subdirectory OPUS_WORK/nrtscw/arcingest.  

=item B<ALERTS>

This is the centralized alerts repository.  This is set to the B<nrt_alerts> entry in the path file, usually /isdc/alert/ntr.

=cut

use strict;
use File::Basename;
use File::Copy;
use lib "$ENV{ISDC_OPUS}/pipeline_lib/";
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

my ($retval,@result);

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","ARC_TRIG","SCW_INPUT","ALERTS");

#########              set processing type:  NRT or CONS
my $proc = &ISDCLIB::Initialize();
#my $proc = &ProcStep();
my $stream = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "consolidated" : "realTime";
my $inst   = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "cons"         : "nrt";				#	Why $inst?  This is misleading.  It has nothing to do with instruments

&Message ( "STARTING" );

#	&ISDCPipeline::PipelineStep(
#		"step"            => "$proc - quick dal_list to check swg.fits",
#		"program_name"    => "dal_list",
#		"par_dol"         => "swg.fits+1",
#		"par_extname"     => "",
#		"par_exact"       => "no",
#		"par_longlisting" => "yes",
#		"par_fulldols"    => "no",
#		"par_mode"        => "ql",
#		);

#########

my $scwid   = $ENV{OSF_DATASET};
my $revno   = &ISDCPipeline::RevNo("$scwid");
my $osf_dir = "$ENV{SCWDIR}/$revno/$scwid.000";		

#
#  Get raw list here and access it throughout this script   - 040206 - Jake - SCREW 1386
#
my @raw_list = &ISDCLIB::ParseConfigFile ( "GNRL_SCWG_RAW.cfg" );
my @grp_list = &ISDCLIB::ParseConfigFile ( "GNRL_SCWG_GRP.cfg" );
my $gziplist = "";			#	primarily a list of .gz files
my $fitslist = "";			#	primarily a list of .fits files

#	because spi_gain_corr overwrites the correct ISDCLEVL ...
&ISDCPipeline::PutAttribute("swg.fits[GROUPING]","ISDCLEVL","DEAD");

########################################################################
# update files of averaged housekeeping HK values for this revolution

#  This will error if version 000 and another exist;  so if NNN>0 exists,
#   should be write protected.  
my $revfiledir = &ISDCLIB::FindDirVers("../rev");

if ($revfiledir =~ /.*\.000/) {
	
	my $avgfile = "../rev.000/osm/hk_averages.fits";
	print "*******     avgfile is $avgfile\n";

	&ISDCLIB::DoOrDie ( "$mymkdir -p ../rev.000/osm" ) unless ( -d "../rev.000/osm" );
	
	#########
	#   Create first HK averages group, if necessary
	#########
	
	if (!(-e "$avgfile") && (-w "../rev.000/")) {
		print "*******     $avgfile does not exist\n" if (!(-e "$avgfile"));
		print "*******     ../rev.000/ is writeable\n" if (-w "../rev.000/");
		# create the group of average structures
		&ISDCPipeline::PipelineStep(
			"step"              => "$proc - create average group",
			"program_name"      => "swg_create",
			"par_SWGroup"       => "hk_averages",
			"par_Config"        => "$ENV{CFITSIO_INCLUDE_FILES}/GNRL_AVRG_GRP.cfg",
			"par_Level"         => "AVR",
			"par_BaseDir"       => "./",
			"par_InGroup"       => "",
			"par_KeywordError"  => "yes",	
			"par_AttachInGroup" => "no",	
			"subdir"            => "../rev.000/osm",
			);
		chdir "../../$ENV{OSF_DATASET}.000";
	}
	
	if ( -w "$avgfile"){
		#########
		#    Average the current science wondow
		#########
		
		#  dp_average will use the swg group and the avg grp to find everything 
		#   it needs to fill the average tables attached to the averages group 
		#   in ../rev.000/osm

		&ISDCPipeline::PipelineStep(
			"step"                => "$proc - dp_average",
			"program_name"        => "dp_average",
			"par_OutSWGroup"      => "swg.fits[GROUPING]",					
			"par_InSWGroup"       => "",
			"par_InData"          => "",
			"par_OutAveragedData" => "$avgfile"."[GROUPING]",
			"par_Log"             => "no",
			) if ( -r "swg.fits" );
		
	} # end of if hk_averages writeable
	
	else {
		&Message ( "WARNING:  HK averages locked;  skipping dp_average" );
	}
	
}  # end if rev.NNN is 000
else {
	&Message ( "WARNING:  rev dir is not version 000;  skipping dp_average" );
}

########################################################################


########################################################################
#
#   Add science windows to workspace indices for Rev pipeline and OSM
# 

&ISDCPipeline::PipelineStep(
	"step"          => "$proc - Extract groups",
	"program_name"  => "dal_grp_extract",
	"par_oDOL"      => "swg.fits[1]",
	"par_iDOL"      => "",
	"par_verbosity" => "3",		
	);


########################################################################
#
# OSM Group write protection and Addition
#
########################################################################
#
#  annoyingly, gzip returns with nonzero status if anything in a list 
#   of inputs isn't found

$fitslist = "";
$gziplist = "";
foreach my $grp ( @grp_list ) {                       
#	if ( -r "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/$grp.fits" ) {
	if ( -r "$osf_dir/$grp.fits" ) {
		$fitslist .= "$grp.fits ";
		$gziplist .= "$grp.fits.gz ";
	}
}

&ISDCPipeline::PipelineStep(												
	"step"         => "$proc - compress OSM data",
	"program_name" => "$mygzip $fitslist",
	) if ( $fitslist );

#$gziplist .= "swg.fits " if ( -r "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/swg.fits" );
$gziplist .= "swg.fits " if ( -r "$osf_dir/swg.fits" );

# since there may not be alerts, only put into write protect command if 
#  they exist;  else ugly error in log.  
#
$gziplist .= " *alert " if (`$myls $osf_dir/*alert 2> /dev/null`);
#$gziplist .= " *alert " if (`$myls $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*alert 2> /dev/null`);

&ISDCPipeline::PipelineStep(												
	"step"         => "$proc - write protect OSM results",
	"program_name" => "$mychmod -R -w $gziplist", 
	) if ( $gziplist );

&ISDCPipeline::MakeIndex(
	"root"     => "GNRL-SCWG-GRP-IDX",
	"subdir"   => "$ENV{OUTPATH}/idx/scw",					
	"add"      => "1",
	"osfname"  => "$ENV{OSF_DATASET}",
	"type"     => "scw",
	"files"    => "swg.fits",											
	"filedir"  => "../../scw/$revno/$ENV{OSF_DATASET}.000/",		
	"ext"      => "[GROUPING]",
	"template" => "GNRL-SCWG-GRP-IDX.tpl",
	"sort"     => "TSTART",
	);

&ISDCPipeline::LinkUpdate(
	"root"    => "GNRL-SCWG-GRP-IDX",
	"ext"     => ".fits",
	"subdir"  => "$ENV{REP_BASE_PROD}/idx/scw",			
	"type"    => "scw",
	"logfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log",
	);

#  End of Indexing
########################################################################



########################################################################

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ALERTS}" ) unless ( -d "$ENV{ALERTS}" );

my $scw_osm_index = "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits[GROUPING]" 
	if (-e "$ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits");

&ISDCPipeline::PipelineStep(
	"step"           => "$proc - copy nrt alerts to $ENV{ALERTS}",
	"program_name"   => "am_cp",
	"par_OutDir"     => "$ENV{ALERTS}",
	"par_OutDir2"    => "",
	"par_Subsystem"  => "SCW",
	"par_DataStream" => "$stream",
	"par_ScWIndex"   => $scw_osm_index,
	"subdir"         => "$osf_dir",
#	"subdir"         => "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000",	
	) if ( $ENV{PATH_FILE_NAME} =~ /nrt/ );	

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - chmod +w cons alerts",
	"program_name" => "$mychmod +w *alert*",
	) if ( (`$myls $osf_dir/*alert* 2> /dev/null`) && ( $ENV{PATH_FILE_NAME} =~ /cons/ ) );	
#	) if ( (`$myls $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*alert* 2> /dev/null`) && ( $ENV{PATH_FILE_NAME} =~ /cons/ ) );	

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - remove cons alerts",
	"program_name" => "$myrm *alert*",
	) if ( (`$myls $osf_dir/*alert* 2> /dev/null`) && ( $ENV{PATH_FILE_NAME} =~ /cons/ ) );	
#	) if ( (`$myls $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*alert* 2> /dev/null`) && ( $ENV{PATH_FILE_NAME} =~ /cons/ ) );	

#  End of Alert copying
########################################################################




########################################################################
#
# Check contents of ScW dir
#
print "*******     Checking contents of ScW directory\n";
#my @contents  = sort ( glob ("$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*" ));
#push @contents, sort ( glob ("$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*/*" ));
#push @contents, sort ( glob ("$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/*/*/*" ));
my @contents  = sort ( glob ("$osf_dir/*" ));
push @contents, sort ( glob ("$osf_dir/*/*" ));
push @contents, sort ( glob ("$osf_dir/*/*/*" ));
foreach my $one (@contents) {
	print "*******     File:  $one\n";
	my ($root,$path,$suffix) = &File::Basename::fileparse($one,'\..*');
	print "*******      Root is $root, suffix is $suffix\n";
	
	next if (($root =~ /^swg$/ ) && ($suffix =~ /fits/)) ;							
	next if (($root =~ /^swg_raw$/ ) && ($suffix =~ /fits/)) ;							
	
	next if (($root =~ /(_inp|_scw)/) && ($suffix =~ /txt/));

	next if (($root =~ /L\d{1}_.+/) && ($suffix =~ /alert/));

	# next level contents:  all data files should be in one of the cfg files:
	if ( grep /$root/, @grp_list ){	
		print "*****     Found in GRP configuration file\n";
		next;
	}
	if ( grep /$root/, @raw_list ){	
		print "*****     Found in RAW configuration file\n";
		next;
	}
	
	if ($root =~ /^core$/) {
		print "*******     WARNING:  removing core file\n";
		unlink "$one";
		next;
	}
	
	next if ($root =~ /picsit_evts_lcr/);
	
    next if (($root =~ /_scp/ ) && ($suffix =~ /fits/)) ;                          
	
	# if you got to here, you're a junk file: 
	print "*******     ERROR:  Found junk file $one\n";
	
	&Error ( "ERROR:  junk file found; Found junk file $one" );
	
} # end of contents check
########################################################################


&ISDCPipeline::PipelineStep(
	"step"            => "$proc - quick dal_list to check swg.fits",
	"program_name"    => "dal_list",
	"par_dol"         => "swg.fits+1",
	"par_extname"     => "",
	"par_exact"       => "no",
	"par_longlisting" => "yes",
	"par_fulldols"    => "no",
	"par_mode"        => "ql",
	);

########################################################################
#
#  Write protection, archive triggering, last steps...
#

&Message ( "write protect all and trigger archive ingest" );

##  Now recursively write protect, and hereafter log  only to process log
($retval,@result) = &ISDCPipeline::RunProgram("$mychmod -R -w $osf_dir");
#($retval,@result) = &ISDCPipeline::RunProgram("$mychmod -R -w $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/");

die "*******  ERROR:  cannot write protect $osf_dir:\n@result" if ($retval);
#die "*******  ERROR:  cannot write protect $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000:\n@result" if ($retval);

# write trigger file for archive ingest
&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ARC_TRIG}" ) unless ( -d "$ENV{ARC_TRIG}" );

open(AIT,">$ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger_temp") or 
die "*******     ERROR:  cannot open trigger file $ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger_temp";

print AIT "$ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger SCW $ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000\n";
close(AIT);

($retval,@result) = &ISDCPipeline::RunProgram(
	"$mymv $ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger_temp $ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger");
die "******     ERROR:  Cannot make trigger $ENV{ARC_TRIG}/scw_$ENV{OSF_DATASET}0000.trigger:\n@result" if ($retval);

#  trigger QLA (NRT case only):
#if ($inst =~ /nrt/) {
if ( ($inst =~ /nrt/) && ($scwid =~ /0$/) ){ #  071121 - Jake - SPR 4761 - added pointing check
	($retval,@result) = &ISDCPipeline::RunProgram("$mytouch $ENV{OPUS_WORK}/nrtqla/input/$scwid.trigger");
	die "******     ERROR:  Cannot make trigger $ENV{OPUS_WORK}/nrtqla/input/$scwid.trigger:\n@result" if ($retval);
}

($retval,@result) = &ISDCPipeline::RunProgram(
	"$mymv $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_processing $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_done") 
		if (-e "$ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_processing"); 
die "******     ERROR:  Cannot move trigger file $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_processing to done:\n@result" if ($retval);

# if it had an error during processing, was fixed and reset by hand,
#  then this needs to find the "_bad" trigger file instead.  
($retval,@result) = &ISDCPipeline::RunProgram(
	"$mymv $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_bad $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_done") 
		if (-e "$ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_bad"); 
die "******     ERROR:  Cannot move trigger file $ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger_bad to done:\n@result" if ($retval);

my $logfile = "$osf_dir/$ENV{OSF_DATASET}_inp.txt";
if ( -e $logfile ) {
print "Removing input log file:\n --> $logfile\n";
	`$mychmod +w $osf_dir`;
	`$mychmod +w $logfile`;
	`$myrm -f $logfile`;
}
`$mychmod -w $osf_dir`;	#	060503 - was accidentally left here as +w

exit 0;

########################################################################
##
##            DONE
##
########################################################################






__END__ 

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

