#!perl

=head1 NAME

nswdp.pl - NRT Science Window Data Preparation

=head1 SYNOPSIS

I<nswdp.pl> - Run from within B<OPUS>.  This is the second step of a six stage pipeline which processes science windows.  This performs the Data Preparation of the science window data.

=head1 DESCRIPTION

This process triggers when an OSF shows that the B<nswst> step has completed for a science window.  It first cleans up any files possibly created in an earlier attempt.  It then creates the science window group for prepared data using the executable B<swg_create>, which also creates all of the output data structures needed the pipeline following the SCWG_GNRL_PRP.cfg configuration file.  To this group are attached the necessary auxiliary data files (attitude, orbit, time correlation, pointing definition.) Then it finds all the "ic" repository files it will need (namely housekeeping conversion curves), and will quit immediately if any are not found.  Now each instrument DP processing is ready to be run.  In all cases, the first step is the housekeeping conversion with B<dp_hkc> and a conversion curve found in OUTPATH/ic/<inst>/cnv/. 

Note that most executables are called simply giving an input science window group.  If any script intelligence is required to determine other parameters, it will be described.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location of the repository, i.e. REP_BASE_PROD usually.

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
use warnings;

use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

my ($retval,@results);

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","CFG_DIR","LOG_FILES","PARFILES","ALERTS", "REV_INPUT");
&ISDCPipeline::PipelineEnvVars();

my $proc = &ISDCLIB::Initialize();
my $revno = &ISDCPipeline::RevNo($ENV{OSF_DATASET});

my $osf_dir = "$ENV{REP_BASE_PROD}/scw/$revno/$ENV{OSF_DATASET}.000/";
`$mychmod +w $osf_dir`;
chdir "$osf_dir" or &Error ( "cannot chdir to $osf_dir" );

#  Must use relative paths here.
my $revnodir = &ISDCLIB::FindDirVers("../../../aux/adp/$revno");

my $input_log_file = "$osf_dir/$ENV{OSF_DATASET}_inp.txt";	
my $scw_log_file   = "$osf_dir/$ENV{OSF_DATASET}_scw.txt";	

#  Remove a previous log file.  (There will always be one, either initialized 
#   in startup, or in a previous DP run.)

if ( -e $input_log_file ) {
	unlink $scw_log_file ;
	`$mycp $input_log_file $scw_log_file`;	
}

`$mychmod +w $scw_log_file`;				

#  Then re-initialize it.  The reason we do this is so that:
#    a)  after startup, something appears, just a place holder until here
#    b)  DP then puts in the pipeline info again 
#    c)  re-runs have the same thing, but only one run in the log.
$retval = &ISDCPipeline::PipelineStart(
	"pipeline" => "$proc ScW Start", 
	"logonly"  => 1, # OSF already there
	"dataset"  => "$ENV{OSF_DATASET}",
	);
&Error ( "Cannot start pipeline" ) if ($retval);

&Message ( "STARTING" );	

my $grpdol   = "swg.fits[GROUPING]";					
my $grpname  = "swg.fits";								
my @raw_list = &ISDCLIB::ParseConfigFile ( "GNRL_SCWG_RAW.cfg" );
my @grp_list = &ISDCLIB::ParseConfigFile ( "GNRL_SCWG_GRP.cfg" );
my $gziplist = "";			
my $fitslist = "";			


########################################################################
#########              cleanup old 
########################################################################

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - rm the old $grpname",
	"program_name" => "$mychmod u+w $grpname;$myrm $grpname",
	) if (-e  "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/$grpname");

foreach my $grp ( @grp_list ) {
	$fitslist .= "$grp.fits "    if ( -r "$osf_dir/$grp.fits" );
	$gziplist .= "$grp.fits.gz " if ( -r "$osf_dir/$grp.fits.gz" );
}

&ISDCPipeline::PipelineStep(							
	"step"         => "$proc - chmod the raw directory",
	"program_name" => "$mychmod u+w raw",
	);

&ISDCPipeline::PipelineStep(							
	"step"         => "$proc - chmod the old data",
	"program_name" => "$mychmod u+w $fitslist $gziplist",
	) if ( "$fitslist$gziplist" );		

&ISDCPipeline::PipelineStep(							
	"step"         => "$proc - rm the old data",
	"program_name" => "$myrm -rf $fitslist $gziplist",
	) if ( "$fitslist$gziplist" );		

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - rm the old alerts",
	"program_name" => "$mychmod u+w *.alert;$myrm -f *.alert*",
	) if (`$myls $osf_dir/*.alert* 2> /dev/null`);

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - rm the old core dump",
	"program_name" => "$mychmod u+w core;$myrm -f core",
	) if (`$myls $osf_dir/core 2> /dev/null`);

########################################################################
#########              create SWG 
########################################################################

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - swg_create",
	"program_name" => "swg_create",
	"par_SWGroup"  => "swg",										
	"par_Level"    => "DEAD",										
	"par_Config"   => "$ENV{CFG_DIR}/GNRL_SCWG_GRP.cfg",
	"par_BaseDir"  => "./",
	"par_InGroup"  => "swg_raw.fits[GROUPING]",
	"par_KeywordError"  => "yes",
	"par_AttachInGroup" => "no",								
	);

########################################################################
#########              find and attach AUX data 
#########                (see details below)
########################################################################

&attNRT()  if ( $proc =~ /NRT/  );
&attCONS() if ( $proc =~ /CONS/ );

########################################################################
#########             find required conversion curves
########################################################################

my $missing;
my $inst;
my $struct;
my %cc;
my @result;
my $list;
my $dpe; 
my $apid;

foreach $inst ( "intl", "ibis", "spi", "omc", "jmx1", "jmx2" ) 
{
	$struct = $inst."-CONV-MOD";
	$struct = uc($struct);
	$struct =~ s/(OMC|SPI)/$1\./;
	$dpe = "";
	if ($inst =~ /spi|ibis/) {
		my ( $raw_hk_file ) = grep /${inst}_raw_hk/, @raw_list;

		&Error ( "Did not find ${inst}_raw_hk match" ) unless ( $raw_hk_file );		

		if ( -r $raw_hk_file or -r "$raw_hk_file.gz" ) {	

			$apid = &ISDCPipeline::GetAttribute("$raw_hk_file"."[SPI.-DPE.-HRW]","APID") 
				if ($inst =~ /spi/);		
			$apid = &ISDCPipeline::GetAttribute("$raw_hk_file"."[IBIS-DPE.-HRW]","APID") 
				if ($inst =~ /ibis/);	

			#  Two SPI values
			if ($apid =~ /1024/) {
				$dpe = "DPE == 1";
			}
			elsif ($apid =~ /1152/) {
				$dpe = "DPE == 2";
			}
			# Two IBIS values
			elsif ($apid =~ /1280/) {
				$dpe = "DPE == 1";
			}
			elsif ($apid =~ /1408/) {
				$dpe = "DPE == 2";
			}
		} else {
			&Message ( "WARNING:  ${inst}_raw_hk file not readable\n" );		
		}
	}  # if inst is SPI or IBIS
	
	
	#  Call GetICFile without error;  check afterwards so pipeline can
	#   see all which missing at once.  
	#
	#  Note that unlike most IC files, new conversion curves remain
	#   valid for old data.  So no time selection necessary, only sort.
	#  (Furthermore, TSTART not set in science window until end of DP.)

	@result = &ISDCPipeline::GetICFile (
		"structure" => "$struct",
		"error"     => 0,
		"sort"      => "VSTART",
		"select"    => "$dpe",
		);

	if (@result) {
		$cc{$inst} = $result[$#result];
		$list .= "-----   ".$cc{$inst}."\n";
	}
	else{
		print "*****     Missing curve for $inst\n";
		$missing .= "$struct";
	}
}


&Error ( "cannot find the following IC structures \n$missing\n" ) 
	if ($missing);	

&Message ( "CONVERSION CURVES:\n$list-----   " );	

########################################################################
#########             call each the spacecraft DP
########################################################################


&scDP();

########################################################################
#########             generic early steps
########################################################################

($retval,@results) = &ISDCPipeline::PipelineStep(
	"step"           => "$proc - dp_aux_attr",
	"program_name"   => "dp_aux_attr",
	"par_OutSWGroup" => "$grpdol",
	"par_InSWGroup"  => "",
	"stoponerror"    => 0,
	);
# check exact return status;  if 11452  (no PDEF, or PDEF or attitude don't
# contain matching data) and we are in NRT, then don't stop, but send alert.
# All other errors, die as usual. 
if ($retval) {
	#	061113 - Jake - Set these default messages 
	my $warning  = "WARNING:  dp_aux_attr missing AUX data in NRT pipeline;  sending alert and continuing.";
	my $alertmsg = "Missing AUX data in NRT processing of SWID $ENV{OSF_DATASET}";
	my $alertid  = "504";

	
	#  In NRT case, all "special" errors 11452, 11455, 11456 are allowed to
	#   pass with a logged warning and an alert:
	if (  ( ( $retval =~ /1145[256]/) && ( $ENV{PATH_FILE_NAME} =~ /nrt/ ) )
		|| ( ( $retval == 11455 )      && ( $ENV{PV_ALLOW_AUX_ERR} =~ /TRUE/ ) )
		|| ( ( $retval == 11456 )      && ( $ENV{PV_ALLOW_AUX_ERR} =~ /TRUE/ ) ) ) {
		#
		#	nothing to really do here apparently
		#
	}
	elsif ( ( $retval == 11452 ) && ( $ENV{PV_ALLOW_AUX_ERR} =~ /TRUE/ ) ) {
		my @scwdirs =  grep /\d{12}\.\d{3}/, glob ( "$ENV{SCWDIR}/$revno/*" );
		if ( ( $scwdirs[$#scwdirs] =~ /$ENV{OSF_DATASET}/ )
			&& ( $ENV{OSF_DATASET} =~ /1$/ ) ) {
			my ($sec,$min,$hr,$dom,$mon,$yr,$dow,$doy,$dlst) = localtime(time);
			$yr+=1900;
			my @months = qw/Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/;
			open  IGN_ERRORS, ">> $ENV{OPUS_HOME_DIR}/ignored_errors.cfg";
			print IGN_ERRORS  "$ENV{OSF_DATASET}            dp_aux_attr             11452           added by cswdp on $dom-$months[$mon]-$yr\n";
			close IGN_ERRORS;
		} else {
			print "*******     ERROR:  return status of $retval from dp_aux_attr is not allowed for $ENV{OSF_DATASET}\n";
			exit 1;
		}
	}
	else {
		print "*******     ERROR:  return status of $retval from dp_aux_attr is not allowed\n";
		exit 1;
	}
	
	&Message ( "$warning" );	
	
	&ISDCPipeline::WriteAlert(
		"message" => "$alertmsg",
		"level"   => 1,
		"id"      => "$alertid",
		);
}

########################################################################
#########             call each instrument subroutine
#########                (see details below)
########################################################################

&spiDP();
&ibisDP();
&jmxDP( 1 );
&jmxDP( 2 );
&omcDP();

########################################################################
#########             generic last steps
########################################################################

my $path = &ISDCPipeline::FindScw("$ENV{OSF_DATASET}");

#  Sounds wrong, but stop=0 means do NOT revert errors to ISDC_OK (Cons),
#   stop=1 means DO revert errors to ISDC_OK (NRT).  I.e. stop=0 means
#   do stop, stop=1 means don't stop.  Blame LL.
#	my $stop = 0;
#	$stop = 1 if ($ENV{PATH_FILE_NAME} =~ /nrt/);
my $stop = ($ENV{PATH_FILE_NAME} =~ /nrt/) ? 1 : 0;

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - dp_aux_derive",
	"program_name" => "dp_aux_derive",
	"par_inSWG"    => "",
	"par_outSWG"   => "$grpdol",
	"par_outAttitude"      => "",
	"par_stopDal3auxAlert" => "$stop",
	);

my $thisswg  = "$path/swg_raw.fits[GROUPING]";
my $prevswid = &ISDCPipeline::FindPrev("$thisswg");
print "*******     Previous science window is $prevswid.\n";

# check for fist science window, only in testing when PP writes prevswid as
#   00000000000 and dp_status_gen errors off.  
if ( $prevswid =~ /0{11}/ ) {
	$prevswid = "";
	print "*******     WARNING:  Resetting prevswid to blank string\n";  
}
&ISDCPipeline::PipelineStep(
	"step"             => "$proc - dp_status_gen",
	"program_name"     => "dp_status_gen",
	"par_ThisScW"      => "$grpdol",
	"par_InScW"        => "",
	"par_PrevScW"      => "$prevswid", 
	"par_StatusFile"   => "",
	"par_TimeFraction" => "0.9",
	);

&Message ( "done" );	

exit 0;

########################################################################
#########            done with main
########################################################################



########################################################################

=item B<attNRT> ( )

Attach Attitude for NRT 

=cut

sub attNRT {
	
	my $file;
	my $attsna;
	my $orbit;
	my @progs;
	my $prog;
	my @pdefs;
	my $pdef;
	my @missing;
	my @attpres;
	my $attpre;
	my $timecor;
	my @files;
	my @children;
	my $ool;
	my @tcors;
	my $rev;
	
	####
	#  Predicted attitude:  required
	####
	@attpres = sort(glob("$revnodir/attitude_predicted*.fits*"));		
	if (@attpres) {
		$attpre = $attpres[$#attpres];
		$attpre =~ s/\.gz$//;														
		$attpre .= "[AUXL-ATTI-PRE]";
	} else {
		push @missing, "attitude_predicted";
	}
	
	####
	# Snapshot attitude required if predicted exists.  (It may be empty.)
	#  If neither exist, get from rev 0000.
	####
	$attsna = "$revnodir/attitude_snapshot.fits[AUXL-ATTI-SNA]" 
		if ( ( -e "$revnodir/attitude_snapshot.fits" ) || ( -e "$revnodir/attitude_snapshot.fits.gz" ) );		
	# die immediately if predicted exists without snapshot;  should never be
	
	&Error ( "predicted attitude exists but no snapshot found!\n" ) 
		if (($attpre) && !($attsna));
	push @missing, "attitude_snapshot" unless ($attsna);
	
	####
	#  Time correlation required;  recursive search for previous version.
	####
	$timecor = "$revnodir/time_correlation.fits";
	if ( ( ! -e "$timecor" ) && ( ! -e "$timecor.gz" ) ){															
		print "********     WARNING:  time correlation not present for this revolution;  looking backwards.\n";
		for ($rev = $revno; $rev >= 0; $rev--) {
			$rev = sprintf("%04d",$rev);
			$timecor = &ISDCLIB::FindDirVers("../../../aux/adp/$rev")."/time_correlation.fits";
			print "*******       Looking for $timecor (.gz) \n";															
			last if ( ( -e "$timecor" ) || ( -e "$timecor.gz" ) );														
		}
		print "*******     Found $timecor\n" if (-e "$timecor");
		push(@missing,"time_correlation") unless (-e "$timecor");
	}  # end recursive search
	$timecor .= "[AUXL-TCOR-HIS]";
	
	####
	# Required predicted orbit 
	####
	$orbit = $revnodir."/orbit_predicted.fits[AUXL-ORBI-PRE]" 
		if ( ( -e "$revnodir/orbit_predicted.fits" ) || ( -e "$revnodir/orbit_predicted.fits.gz" ) );	
	push(@missing,"orbit_predicted") unless ($orbit);#required
	
	####
	# Optional predicted program
	####
	@progs = sort(glob("$revnodir/timeline_summary*.fits*"));													
	if ($progs[$#progs]) {
		$prog = $progs[$#progs];
		$prog =~ s/\.gz$//;														
		$prog .= "[AUXL-PROG-PRE]";
	}
	
	####
	# Optional predicted pointing definition
	####
	@pdefs = sort(glob("$revnodir/pointing_definition_predicted_*.fits*"));								
	if (@pdefs) {
		$pdef = $pdefs[$#pdefs];
		$pdef =~ s/\.gz$//;														
		$pdef .= "[AUXL-PDEF-PRE]";
	}
	
	####
	# if it exists, also the MOC Out Of Limits 
	####
	$ool = glob("$revnodir/moc_out_of_limits.fits*");																
	if ( -e "$ool") { 
		$ool =~ s/\.gz$//;														
		$ool .= "[AUXL-OOL.-HIS]";
	} else { $ool = ""; }
	
	########################################################
	# if anything required is missing, look in revolution 0000
	########################################################
	print "*****     WARNING:  missing files ",join(' ',@missing),
		"\n*****                  Looking in revolution 0000\n" if (@missing);
	
	# need to attach using relative path, but need the file test absolute
	foreach $file (@missing) {
		$file = "../../../aux/adp/0000.000/$file*.fits*";												
		print "*****     Looking for: $file\n"; 
		@files = sort(glob("$file"));
		if (@files) {
			my $tmpfile = $files[$#files];
			$tmpfile =~ s/\.gz$//;														
			print "*****     FOUND:\n",join("\n",@files),"\n";
			$orbit =   $tmpfile."[AUXL-ORBI-PRE]" if ($file =~ /orbit/);
			$timecor = $tmpfile."[AUXL-TCOR-HIS]" if ($file =~ /time_cor/);
			$attpre =  $tmpfile."[AUXL-ATTI-PRE]" if ($file =~ /attitude_predicted/);
			$attsna =  $tmpfile."[AUXL-ATTI-SNA]" if ($file =~ /attitude_snapshot/);
		}
		else {
			# if anything required is STILL missing, error  
			#	040820 - Jake - SCREW 1533
			&Error ( "missing AUX data: $file;\nNot in either "
				."$ENV{OUTPATH}/aux/adp/$revno.000/ nor in rev 0000.000\n" );
		} # end else still missing
	} # end if (@missing)
	########################################################
	
	
	########################################################
	#         Finally, dal_attach call(s)
	# 
	# For NRT processing, want to pass blank strings to dal_attach call
	#   for those which optional and missing.  
	#
	# Unfortunately, can't pass blank child to dal_attach with filled paramter
	#  to follow;  ignores any after one blank.  So have to be a bit annoying 
	#  here.
	
	foreach $file ( $ool, $prog, $pdef, $timecor, $attsna, $attpre, $orbit ) {
		push @children, $file if ( $file );
	}
	&ISDCLIB::QuickDalAttach ( $grpdol, @children );
	
} # end of sub attNRT
########################################################################



########################################################################

=item B<attCONS> ( )

Attach Attitude data for Consoldated 

=cut

sub attCONS {
	my $file;
	my $atthis;
	my $orbit;
	my @progs;
	my $prog;
	my @pdefs;
	my $pdef;
	my @missing;
	my @attpres;
	my $attpre;
	my $timecor;
	my $ool;
	my @children;
	
	####
	#  Time correlation required 
	####
	$timecor = $revnodir."/time_correlation.fits";
	push(@missing,"time_correlation ") 
		unless ( ( -e "$timecor" ) || ( -e "$timecor.gz" ) );		
	
	####
	# Required Historic attitude
	####
	$atthis = $revnodir."/attitude_historic.fits";
	push(@missing,"attitude_historic ") 
		unless ( ( -e "$atthis" ) || ( -e "$atthis.gz" ) );			
	
	####
	# Required historic orbit
	####
	$orbit = $revnodir."/orbit_historic.fits";
	push(@missing,"orbit_historic ") 
		unless ( ( -e "$orbit" ) || ( -e "$orbit.gz" ) );			
	
	####
	# Required observation log (probram historic)
	####
	$prog = $revnodir."/observation_log.fits";
	push(@missing,"observation_log ") 
		unless ( ( -e "$prog" ) || ( -e "$prog.gz" ) );				
	
	####
	# Required pointing definition predicted;  
	#  SCREW 1098:  no longer use historic even in Cons.
	####
	@pdefs = sort(glob("$revnodir/pointing_definition_predicted*fits*"));		
	if (@pdefs) {
		$pdef = $pdefs[$#pdefs]; # don't add ext here yet
		$pdef =~ s/\.gz$//;														
	}
	push(@missing,"pointing_definition_predicted ") 					
		unless ( ( -e "$pdef" ) || ( -e "$pdef.gz" ) );				
	
	####
	#  Predicted attitude NOT required in CONS
	####
	@attpres = sort(glob("$revnodir/attitude_predicted*.fits*"));		
	if (@attpres) {
		$attpre = $attpres[$#attpres];
		$attpre =~ s/\.gz$//;														
		$attpre .= "[AUXL-ATTI-PRE]";
	}
	
	####
	# if it exists, also MOC Out Of Limits
	####
	$ool = glob("$revnodir/moc_out_of_limits.fits*");		
	if ( -e "$ool") { 
		$ool =~ s/\.gz$//;														
		$ool .= "[AUXL-OOL.-HIS]";
	} else { $ool = ""; }
	
	########################################################
	# Check that all required exist
	# Yes, $file is a reference to the $att, $orbit, etc. variables in Perl
	foreach $file ($timecor,$atthis,$orbit,$prog,$pdef) { 
		$file .= "[AUXL-ATTI-HIS]" if ($file =~ /attitude/);    
		$file .= "[AUXL-ORBI-HIS]" if ($file =~ /orbit/);
		$file .= "[AUXL-PROG-HIS]" if ($file =~ /observation_log/);
		$file .= "[AUXL-PDEF-PRE]" if ($file =~ /pointing_def/);
		$file .= "[AUXL-TCOR-HIS]" if ($file =~ /time_cor/);
	}
	
	if (@missing) {
		#  For CONS, error off immediately if any missing from correct revno
		&Error ( "missing AUX data files:\n".join("\n",@missing)."\n" );
	} # end if (@missing)    
	########################################################
	
	
	########################################################
	#         Finally, dal_attach call(s)
	# 
	# For CONS processing, want to pass blank strings to dal_attach call
	#   for those two which optional and missing.  
	
	foreach $file ( $attpre, $ool, $prog, $pdef, $timecor, $atthis, $orbit ) {
		push @children, $file if ( $file );
	}
	&ISDCLIB::QuickDalAttach ( $grpdol, @children );
	
}  # end of sub attCONS


########################################################################

=item B<scDP> ( )

The SpaceCraft Data Preparation simply consists of converting the SC housekeeping data as above.

=cut

sub scDP {
	&Message( "SC starting" );	
	&QuickDPHKC ( "intl" );
}

########################################################################

=item B<ibisDP> ( )

For IBIS, after the HK conversion, the on board time is calculated for several IBIS data types using the executables B<ibis_evts_obt_calc> and B<ibis_spih_obt_calc>.  Deadtime is then calculated for ISGRI, PICSIT, and Compton using B<ibis_isgr_deadtime>, B<ibis_pics_deadtime>, and B<ibis_comp_deadtime> respectively.  

=cut

sub ibisDP {
	
	&Message( "IBIS starting" );	
	&QuickDPHKC ( "ibis" );

	&ISDCPipeline::PipelineStep(
		"step"            => "$proc - IBIS ibis_evts_obt_calc",
		"program_name"    => "ibis_evts_obt_calc",
		"par_inswg"       => "",
		"par_outswg"      => "$grpdol",
		"par_clobber"     => "yes",
		"par_inpackraw"   => "",			
		"par_inraw"       => "",			
		"par_insecraw"    => "",			
		"par_outprep"     => "",			
		);
	
	$gziplist = "";
	$fitslist = "";
	for ( my $i=0; $i <= $#raw_list; $i++ ) {			
		if (( $raw_list[$i] =~ /picsit_raw_histo_si/ ) && ( -r "$raw_list[$i].fits.gz" )) {
			$gziplist .= "$raw_list[$i].fits.gz ";			
			$fitslist .= "$raw_list[$i].fits " ;			
		}
	}

	&ISDCPipeline::PipelineStep(							
		"step"         => "$proc - unlock the raw data",
		"program_name" => "$mychmod u+w $gziplist",
		) if ( "$gziplist" );		#	only call this if there is something to do

	&ISDCPipeline::PipelineStep(							
		"step"         => "$proc - gunzip the raw data",
		"program_name" => "$mygunzip $gziplist",
		) if ( "$gziplist" );		#	only call this if there is something to do
	
	&ISDCPipeline::PipelineStep(						
		"step"         => "$proc - ibis_prp_check_histo",
		"program_name" => "ibis_prp_check_histo",
		"par_inswg"    => "",
		"par_outswg"   => "$grpdol",
		"par_inraw"    => "",
		"par_inpackraw" => "",
		"par_insecraw" => "",
		"par_outprep"  => "",
		"par_clobber"  => "yes",
		);

	&ISDCPipeline::PipelineStep(
		"step"            => "$proc - IBIS ibis_spih_obt_calc",
		"program_name"    => "ibis_spih_obt_calc",
		"par_inGRP"       => "",
		"par_outGRP"      => "$grpdol",
		"par_sgleHistIdx" => "",
		"par_muleHistIdx" => "",
		"par_polhHistIdx" => "",
		"par_accuracy"    => "ANY",							
		); 

	&ISDCPipeline::PipelineStep(							
		"step"         => "$proc - re-gzip the raw data",
		"program_name" => "$mygzip $fitslist",
		) if ( "$fitslist" );		#	only call this if there is something to do
	
	&ISDCPipeline::PipelineStep(							
		"step"         => "$proc - re-lock the raw data",
		"program_name" => "$mychmod u-w $gziplist",
		) if ( "$gziplist" );		#	only call this if there is something to do
	
	&ISDCPipeline::PipelineStep(
		"step"            => "$proc - ibis_diag_obt_calc",
		"program_name"    => "ibis_diag_obt_calc",
		"par_inSWG"       => "",
		"par_outSWG"      => "$grpdol",
		"par_inpicsDOL"   => "",
		"par_inisgrDOL"   => "",
		"par_outpicsDOL"  => "",
		"par_outisgrDOL"  => "",
		"par_clobber"     => "yes",
		"par_inibisDOL"   => "",			
		"par_outibisDOL"  => "",			
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - ibis_spti_obt_calc",
		"program_name" => "ibis_spti_obt_calc",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_clobber"  => "yes",
		"par_doAlert"  => "yes",		
		"structures"   => "PICS-SPTI-RAW",
		);
}


########################################################################

=item B<jmxDP> ( $jemxnum )

For each JemX detector, the processing is the same.  After the HK conversion, the on board times are calculated for several the data modes using B<j_prp_evts_obt>, B<j_prp_spec_obt>, and B<j_prp_rate_obt>.  The instrument status table is filled by B<j_prp_status_table>.  This executable will error off if its required data is missing, so a check is done first and the executable not run if the right raw data are not there.  

=cut

sub jmxDP {
	my ( $jemxnum ) = @_;

	&Message( "JMX$jemxnum starting" );	
	&QuickDPHKC ( "jmx$jemxnum" );

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JMX$jemxnum j_prp_evts_obt",
		"program_name" => "j_prp_evts_obt",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_clobber"  => "y",
		"par_jemxNum"  => "$jemxnum",
		);

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JMX$jemxnum j_prp_spec_obt",
		"program_name" => "j_prp_spec_obt",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_clobber"  => "y",
		"par_jemxNum"  => "$jemxnum",
		);

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JMX$jemxnum j_prp_rate_obt",
		"program_name" => "j_prp_rate_obt",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_clobber"  => "y",
		"par_jemxNum"  => "$jemxnum",
		);

	&ISDCPipeline::PipelineStep(
		"step"                 => "$proc - JMX$jemxnum DIAG dp_obt_calc",
		"program_name"         => "dp_obt_calc",
		"par_InSWGroup"        => "",
		"par_OutSWGroup"       => "$grpdol",
		"par_RawData"          => "",
		"par_ConvertedData"    => "",
		"par_AttributeData"    => "",
		"par_TimeInfo"         => "",
		"par_IN_STRUCT_NAME"   => "JMX$jemxnum-DIAG-CRW",
		"par_OUT_STRUCT_NAME"  => "JMX$jemxnum-DIAG-CPR",
		"par_ATT_STRUCT_NAME"  => "",
		"par_LOBT_2X4_NAMES"   => "",
		"par_LOBT_1X8_NAMES"   => "LOBT_FIRST OB_TIME",
		"par_PKT_NAMES"        => "",
		"par_LOBT_ATTR"        => "",
		"par_PKT_ATTR"         => "",
		"par_OBT_TYPE"         => "JEMX",
		"structures"           => "JMX$jemxnum-DIAG-CRW",
		);

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JMX$jemxnum j_prp_status_table",
		"program_name" => "j_prp_status_table",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_instStat" => "",
		"par_fullSrw" => "",
		"par_fullPrw" => "",
		"par_fullPrp" => "",
		"par_restSrw" => "",
		"par_restPrp" => "",
		"par_sptiPrw" => "",
		"par_sptiPrp" => "",
		"par_sptiSrw" => "",
		"par_specRaw" => "",
		"par_specPrp" => "",
		"par_timePrp" => "",
		"par_timePrw" => "",
		"par_timeSrw" => "",
		"par_rateRaw" => "",
		"par_ratePrp" => "",
		"par_ratePrw" => "",
		"par_jemxNum" => "$jemxnum",
		"par_clobber" => "y",
		"par_chatter" => "1",
		);

	# Moved from osm to dp as j_prp_verify now writes something to fits files 
	# starting in version 4.0 (23 Oct 2003)
	my $imodgrp;

	@result = &ISDCPipeline::GetICFile(
		"structure" => "JMX$jemxnum-IMOD-GRP",
		"filematch" => "$grpdol",
		);

	if (@result) {
		$imodgrp = $result[$#result];
	}
	else {
		#	040820 - Jake - SCREW 1533
		&Error ( "missing JMX IC file JMX$jemxnum-IMOD-GRP" );
	}

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - JMX$jemxnum j_prp_verify",
		"program_name" => "j_prp_verify",
		"par_inSWG"    => "",
		"par_outSWG"   => "$grpdol",
		"par_instMod"  => "$imodgrp",
		"par_anodHis"  => "",
		"par_jemxNum"  => "$jemxnum",
		"par_alrtLevel" => "$ENV{JMX_ALERT_LEVEL}",
		"par_chatter"  => "1",
		"par_clobber"  => "n",
		);
}


########################################################################

=item B<omcDP> ( )

After the OMC HK conversion, the OBT calculation is done for the "trig" data mode using B<o_prp_trig_obt>.  The pipeline then searches for the corresponding shot and box plans in the auxiliary data repository and attaches them to the science window group.  (These are named for revolution and sequence, as is the science window.)  If they are not found, the omcDP is not run further.  The shot OBT calculation is done with B<o_prp_shot_obt>, and then B<o_prp_shot_plan>, B<o_prp_box_plan>, and B<o_prp_box_fluxes> check that the data correspond to the planning.  

=cut

sub omcDP {
	
	&Message( "OMC starting" );	
	&QuickDPHKC ( "omc" );
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_shot_obt",
		"program_name" => "o_prp_shot_obt",
		"par_outswg"   => "$grpdol",
		"par_inswg"    => "",
		"par_clobber"  => "y",
		);

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_trig_obt",
		"program_name" => "o_prp_trig_obt",
		"par_inswg"    => "",
		"par_rawtrig"  => "",
		"par_outswg"   => "$grpdol",
		"par_prptrig"  => "",
		"par_clobber"  => "no",
		"par_mode"     => "",			
		);
	
	
	#  find corresponding shot and box plans and attach.
	#	shotplan_01000092_0002.fits.gz
	#	shotplan_(bcppid)_(podversion).fits.gz

	my $bcppid;
	($retval,$bcppid) = &ISDCPipeline::GetAttribute("$grpdol","BCPPID","DAL_CHAR");
	
	print "looking for shotplans\n";
	my @shotplans = sort(glob(&ISDCLIB::FindDirVers("../../../aux/adp/$revno")."/shotplan_$bcppid*.fits*"));
	
	print "looking for boxplans\n";
	my @boxplans = sort(glob(&ISDCLIB::FindDirVers("../../../aux/adp/$revno")."/boxplan_$bcppid*.fits*"));
	
	if (!((@shotplans) && (@boxplans))) {
		&Message( "WARNING:  OMC shot and box plans not found;  skipping OMC DP" );	
		return;
	}
	
	my $shotplan = $shotplans[$#shotplans];	# not always correct
	$shotplan =~ s/\.gz$//;														
	$shotplan .= "[OMC.-SHOT-REF]";
	
	my $boxplan = $boxplans[$#boxplans];		# not always correct
	$boxplan =~ s/\.gz$//;														
	$boxplan .= "[OMC.-BOXS-REF]";

	if ( ( $shotplan ) or ( $boxplan ) ) { # 050323 - Jake - this if really isn't necessary
		print "*******     Found $shotplan and $boxplan;  attaching\n";
		&ISDCLIB::QuickDalAttach ( $grpdol, ( $boxplan, $shotplan ) );
	}

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_box_plan",
		"program_name" => "o_prp_box_plan",
		"par_inswg"    => "",
		"par_rawshots" => "",
		"par_boxplan"  => "",
		"par_outswg"   => "$grpdol",
		"par_prpshots" => "",
		"par_osmpars"  => "",
		"par_clobber"  => "y",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_shot_plan",
		"program_name" => "o_prp_shot_plan",
		"par_inswg"    => "",
		"par_outswg"   => "$grpdol",
		"par_rawshots" => "",
		"par_shotplan" => "",
		"par_prpshots" => "",
		"par_osmpars"  => "",
		"par_chatty"   => "1",
		"par_clobber"  => "y",
		"par_alertlevel" => $ENV{ALERT_LEVEL},
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - OMC o_prp_box_fluxes",
		"program_name" => "o_prp_box_fluxes",
		"par_inswg"    => "",
		"par_outswg"   => "$grpdol",
		"par_rawshots" => "",
		"par_prpshots" => "",
		"par_osmpars"  => "",
		"par_pixsatur" => "4095",
		"par_pixblank" => "150",			
		"par_boxsatur" => "4000",
		"par_boxblank" => "210",			
		"par_chatty"   => "1",
		"par_clobber"  => "y",			
		);
} 



########################################################################

=item B<spiDP> ( )

After the HK conversion for SPI, the executable B<spi_evts_obt_calc> is run to calculate the OBT for the SPI event data.  

=cut

sub spiDP { 
	
	&Message( "SPI starting" );	
	&QuickDPHKC ( "spi" );
	
	&ISDCPipeline::PipelineStep( 
		"step"              => "$proc - SPI spi_evts_obt_calc",
		"program_name"      => "spi_evts_obt_calc",
		"par_inswg"         => "",
		"par_outswg"        => "$grpdol",
		"par_SgleRawEvt"    => "",
		"par_PsdeRawEvt"    => "",
		"par_Me02RawEvt"    => "",
		"par_Me03RawEvt"    => "",
		"par_Me04RawEvt"    => "",
		"par_Me05RawEvt"    => "",
		"par_HimultRawEvt"  => "",
		"par_SglePrepEvt"   => "",
		"par_PsdePrepEvt"   => "",
		"par_Me02PrepEvt"   => "",
		"par_Me03PrepEvt"   => "",
		"par_Me04PrepEvt"   => "",
		"par_Me05PrepEvt"   => "",
		"par_HimultPrepEvt" => "",
		"par_clobber"       => "yes",
		"par_SpiMode"       => "-1",
		"par_EvtType"       => "-1", 
		"par_chatter"       => "1",
		"par_PrepSchk"      => "",				
		"par_RawSchk"       => "",				
		);
	
	my @result = &ISDCPipeline::GetICFile(
		"sort"      => "VSTART",
		"structure" => "SPI.-ALGO-PSD",
		);
	my $algopsd = $result[$#result];

	@result = &ISDCPipeline::GetICFile(
		"sort"      => "VSTART",
		"structure" => "SPI.-LIB.-PSD",
		);
	my $libpsd = $result[$#result];
	
	&ISDCPipeline::PipelineStep(
		"step"           => "$proc - dp_spi_psd",
		"program_name"   => "dp_spi_psd",
		"par_swgdol"     => "$grpdol",
		"par_algopardol" => "$algopsd",
		"par_libdol"     => "$libpsd",
		"par_clobber"    => "no",
		"par_mode"       => "ql",			
		);
	
	&ISDCPipeline::PipelineStep(
		"step"           => "$proc - spi_dp_derived_param",
		"program_name"   => "spi_dp_derived_param",
		"par_InSWGroup"  => "",
		"par_OutSWGroup" => "$grpdol",
		"par_DFEE_BTime" => "600",
		"par_DFEE_CTime" => "60",
		"par_DFEE_Periodicty" => "60",
		"par_Block_BTime"     => "4800",
		"par_Block_CTime"     => "480",
		"par_Block_Periodicty" => "60",
		"par_ACS_BTime"        => "40",
		"par_ACS_CTime"        => "10",
		"par_ACS_Periodicty"   => "960",
		"par_OverAllACS_BTime" => "6000",
		"par_OverAllACS_CTime" => "200",
		"par_OverAllACS_Periodicty" => "10",
		"par_PSD_BTime"             => "7",
		"par_PSD_CTime"             => "1",
		"par_PSD_Periodicty"        => "64",
		"par_MaxIntegrationTime"    => "5000",
		"par_ConsCheckIntegrationTime" => "120",
		);
	
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - spi_spec_obt_calc",
		"program_name" => "spi_spec_obt_calc",
		"par_swgDOL"   => "$grpdol",
		"par_accuracy" => "$ENV{SPI_SPEC_OBT_ACCURACY}",
		);
	
} # end sub spiDP


########################################################################

=item B<QuickDPHKC> ( $inst )

dp_hkc wrapper

=cut

sub QuickDPHKC {
	my ( $inst ) = @_;
	my $LOBT_2X4_NAMES = ( $inst =~ /spi/ ) ? "P__AS__MS_MOBT__L P__AS__LS_MOBT__L P__AS_MOBT__L" : "";
	
	&ISDCPipeline::PipelineStep(
		"step"                => "$proc - $inst dp_hkc",
		"program_name"        => "dp_hkc",
		"par_OutSWGroup"      => "$grpdol",
		"par_InSWGroup"       => "",
		"par_RawData"         => "",
		"par_ConvertedData"   => "",
		"par_ConversionCurve" => "$cc{$inst}",
		"par_LOBT_2X4_NAMES"  => "$LOBT_2X4_NAMES",
		"par_LOBT_1X8_NAMES"  => "",
		"par_TimeInfo"        => "",
		);
}

########################################################################

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

