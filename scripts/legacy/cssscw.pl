#!perl

=head1 NAME

I<cssscw.pl> - consssa SCW step script

=head1 SYNOPSIS

I<cssscw.pl> - Run from within B<OPUS>.  This is the second, and primary processing, step in the consssa pipeline.

=head1 DESCRIPTION

=over

=cut

use strict;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use OPUSLIB;
use IBISLIB;
use JMXLIB;
use SPILIB;
use OMCLIB;
use SSALIB;
use CorLIB;
use ISDC::Level;

print "\n========================================================================\n";

&ISDCPipeline::EnvStretch("LOG_FILES", "OUTPATH", "WORKDIR", "INPUT","IC_ALIAS");

=item Parse OSF

=cut
	
my ($retval,@result);
my ( $scwid, $revno, $og, $inst, $INST, $instdir, $OG_DATAID, $OBSDIR ) = &SSALIB::ParseOSF;
my $proc = &ProcStep()." $INST";

&Message ( "$proc - STARTING" );

$ENV{PARFILES} = "$ENV{OPUS_WORK}/consssa/scratch/$ENV{OSF_DATASET}/pfiles";

&ISDCLIB::DoOrDie ( "mkdir -p $ENV{PARFILES}" ) unless ( -e $ENV{PARFILES} );

print "*******     ObsID is $ENV{OSF_DATASET};  Instrument is $INST;  group is $og.\n";

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
#					Check that this should be done.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

if ( ( $ENV{REDO_CORRECTION} ) && ( $INST=~/SPI|OMC|PICSIT/ ) ) {
	&Message ( "$proc - Not ReRunning Correction for $INST $ENV{OSF_DATASET}." );
	exit 0;
}


=item Previous-run-cleanup

Cleanup is done in the primary location, as well as any data which may be stored on other machines when using the "USELOCALDISKS" option.

=cut

if (( -e "$OBSDIR") ||		#	using -e bc may be link or directory
	(  -l "$OBSDIR" )) {		#	need -l in case of dead link
	#  Clean up previous run:
	
	if ( $ENV{OG_TOOL} ne 'og_clean' ) {

		#  Move log back to central dir (assuming it exists and the previous
		#   run had gotten that far.)
		#
		#	The filename is OG_DATAID in the actual dir, but OSF_DATASET
		#	in the OPUS logs and obs dirs. (The only difference may be the 
		#	removed 0000 if it is a multiple revolution mosaic.)
		&ISDCPipeline::MoveLog(										#	move log from OBSDIR to OPUS_WORK
			"$OBSDIR/logs/${OG_DATAID}_css.txt",				#	current location
			"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_css.txt",		#	new location
			"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"			#	link location
			) if ( -e "$OBSDIR/logs/${OG_DATAID}_css.txt" );
	
		#	If the log file doesn't exist, this will fail.
		#	The log file will probably exist at the beginning, but then it has been deleted.
		#	This same thing continues below with the &Error calls.
		($retval,@result) = &ISDCPipeline::PipelineStep (
			"step"         => "$proc - clean up previous main",
			"program_name" => "$myrm -rf $OBSDIR",
			"subdir"       => "$ENV{WORKDIR}",
			);
		&Error ( "Didn't remove $OBSDIR" )
			if (( -e "$OBSDIR" ) || ( -l "$OBSDIR" ));


	} else {   # $ENV{OG_TOOL} eq 'og_clean' ) {

		&ISDCPipeline::PipelineStep(
			"step"         => "$proc - dummy step to set COMMONLOGFILE variable",
			"program_name" => "$myecho",
		);
		&Message ( "Checking the results of STAGE 1 and prepping for STAGE 2" );
		&Error ( "og $OBSDIR/$og does not exist" )   unless ( -e "$OBSDIR/$og" );
		&ISDCLIB::DoOrDie ( "$mychmod -R +w $OBSDIR" );
		&ISDCPipeline::MoveLog(										#	move log from OBSDIR to OPUS_WORK
			"$OBSDIR/logs/${OG_DATAID}_css.txt",				#	current location
			"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_css.txt",		#	new location
			"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"			#	link location
			) if ( -e "$OBSDIR/logs/${OG_DATAID}_css.txt" );
#		&ISDCLIB::DoOrDie ( "$mygunzip $OBSDIR/*gz" )       if ( glob ( "$OBSDIR/*gz" ) );
#		&ISDCLIB::DoOrDie ( "$mygzip $OBSDIR/energy_bands.fits" ) if ( -e "$OBSDIR/energy_bands.fits" );
#		&ISDCLIB::DoOrDie ( "$mygunzip $OBSDIR/scw/*/*gz" ) if ( glob ( "$OBSDIR/scw/*/*gz" ) );
		&UnixLIB::Gunzip ( "$OBSDIR/*gz" );
		&UnixLIB::Gzip   ( "$OBSDIR/energy_bands.fits" );
		&UnixLIB::Gunzip ( "$OBSDIR/scw/*/*gz" );


#		the endLevel should actually be 1 less that the given $ENV{ISGRI_STARTLEVEL}

		my $level = new ISDC::Level('isgri',$ENV{ISGRI_STARTLEVEL});
		
		&ISDCPipeline::PipelineStep(
			"step"         => "$proc - clean og to level ".$level->previous,
			"program_name" => "og_clean",
			"par_ogDOL"    => "$og",
			"par_endLevel" => $level->previous,
			"par_chatter"  => "3",
			"subdir"       => $OBSDIR,
		);

	} # if ( $ENV{OG_TOOL} ne 'og_clean' ) {
}
	
#	
#	NOTE:  Both above and below, the clean-up procedures DO NOT chmod 
#			the data.  This is a precautionary procedure.  It will only
#			be write-protected if it finished.  If the user wishes to
#			start over, (s)he will need to chmod manually.
#
	
if ( ( $ENV{USELOCALDISKS} ) && ( $ENV{OG_TOOL} ne 'og_clean' ) ) {
	chomp ( my $hostname = `hostname` );
	my $localdir        = "/reproc/$hostname/cons/ops_sa/$instdir/$OG_DATAID";
	my $allpossibledirs = "/reproc/anaB?/cons/ops_sa/$instdir/$OG_DATAID";		#	this NEEDS to have a wildcard in it!

	my @dirs = glob ( "$allpossibledirs" );
	if ( @dirs ) {
		foreach my $dir ( @dirs ) {
			( $retval, @result ) = &ISDCPipeline::PipelineStep(
				"step"         => "$proc - clean up previous local $dir",
				"program_name" => "$myrm -rf $dir",
				);
		}       
	}

	@dirs = glob ( "$allpossibledirs" );
	&Error ( "Didn't remove all possible dirs\n @dirs" )
		if ( @dirs );

	&ISDCLIB::DoOrDie ( "$mymkdir -p $localdir" );
	&Error ( "Didn't make dir $localdir." ) if ( ! -d "$localdir");
	symlink "$localdir", "$OBSDIR"
		or &Error ( "Couldn't symlink dir $localdir : $!" );
	&Error ( "Didn't symlink dir $localdir : $!" )
		unless ( -l "$OBSDIR" );
}

=item Create directory and logs.

=cut

&ISDCLIB::DoOrDie ( "$mymkdir -p $OBSDIR/logs" ) 
	unless ( -d "$OBSDIR/logs" );
&Error ( "Didn't make dir $OBSDIR/logs." ) 
	unless ( -d "$OBSDIR/logs" );
	
&ISDCPipeline::MoveLog(										#	move log from OPUS_WORK to OBSDIR
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_css.txt",		#	current location
	"$OBSDIR/logs/${OG_DATAID}_css.txt",				#	new location
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"			#	link location
	) if ( -e "$ENV{LOG_FILES}/$ENV{OSF_DATASET}_css.txt" );

&Message ( "$proc - STARTING" );

$ENV{PARFILES} = "$ENV{OPUS_WORK}/consssa/scratch/$ENV{OSF_DATASET}/pfiles";

&ISDCLIB::DoOrDie ( "mkdir -p $ENV{PARFILES}" ) unless ( -e $ENV{PARFILES} );

=item Create OG and Parse trigger file

=cut

chomp ( my $trigger = `$myls $ENV{INPUT}/${scwid}_${inst}.trigger*` );

my $proctype = ( -z "$trigger" ) ? "scw" : "mosaic";
&Message ( "Using Proctype of (either scw or mosaic) : $proctype" );

my $idx2og;
my $spiPoint = "";

chdir "$ENV{REP_BASE_PROD}";
	
if ( $proctype =~ /mosaic/ ) {
	$idx2og   = "$OBSDIR/logs/$OG_DATAID.idx2og";			#	this file is written in ParseTrigger;
	$spiPoint = &SSALIB::ParseTrigger ( "$trigger", "$inst", "$idx2og" );
} else {
	$idx2og  = &ISDCLIB::FindDirVers("./scw/$revno/$scwid");
	&Error ( "Version of $ENV{REP_BASE_PROD}/scw/$revno/$scwid not found!?!?!?" ) unless ( $idx2og );
	$idx2og .= "/swg.fits[GROUPING]";
}


#	add third possible option to continue processing in the same directory

if ( $ENV{OG_TOOL} eq 'og_copy' ) {
	&Error ( "\$ENV{OG_READ} and \$ENV{OG_WRITE} must be different!" ) unless ( $ENV{OG_WRITE} ne $ENV{OG_READ} );

	( my $source = $instdir ) =~ s/$ENV{OG_WRITE}/$ENV{OG_READ}/;

	($retval,@result) = &ISDCPipeline::PipelineStep(
		"step"           => "$proc - copy OG",
		"program_name"   => "og_copy",
		"par_inOgDir"    => "./$source/$OG_DATAID/",
		"par_instrument" => "$inst",
		"par_levels"     => "",
		"par_baseDir"    => "./",
		"par_obsDir"     => "$instdir",
		"par_ogid"       => "$OG_DATAID",
		"par_obs_id"     => "",
		"par_purpose"    => "CSS",
		"par_versioning" => "0",
		"subdir"         => "$ENV{REP_BASE_PROD}",
		);

} elsif ( ( $ENV{OG_TOOL} eq 'og_create' ) || ( $ENV{OG_TOOL} eq '' ) ){
	&ISDCLIB::DoOrDie ( "$mycp $trigger $OBSDIR/logs/$OG_DATAID.trigger" ) if ( $proctype =~ /mosaic/ );
	
	($retval,@result) = &ISDCPipeline::PipelineStep(
		"step"           => "$proc - create OG",
		"program_name"   => "og_create",
		"par_idxSwg"     => "$idx2og",
		"par_instrument" => "$inst",
		"par_ogid"       => "$OG_DATAID",
		"par_baseDir"    => "./",
		"par_obs_id"     => "",
		"par_purpose"    => "CSS",
		"par_versioning" => "0",
		"par_obsDir"     => "$instdir",
		"par_scwVer"     => "",			#	050708 - Jake - testing SPR 4258 change
		"par_swgName"    => "swg",
		"par_keep"       => "",			#	060424
		"par_verbosity"  => "3",
		"subdir"         => "$ENV{REP_BASE_PROD}",
		);
}

chdir ( "$OBSDIR" ) or &Error ( "Cannot chdir to $OBSDIR" );
print "*******     Current directory is $OBSDIR \n";

my $IC_Group = "../../../idx/ic/ic_master_file.fits[1]";
#my $IC_Group = ( $revno =~ /0000/ ) ? "../../idx/ic/ic_master_file.fits[1]" : "../../../idx/ic/ic_master_file.fits[1]";

############################################################################

=item Call associated instrument script to begin processing

=cut

if ($INST =~ /ISGRI/) {
	&IBISLIB::ISA (
		"IC_Group"       => "$IC_Group",
		"proctype"       => "$proctype",
		"INST"           => "$INST",
		"instdir"        => "$instdir",
		);
	&CorLIB::IBEP (
		"disableIsgri"  => "no",
		"disablePICsIT" => "yes",
		"INST"          => "$INST",
		) if ( ( $ENV{REDO_CORRECTION} ) && ( $proctype =~ /scw/ ) && ( $ENV{CREATE_REV_3} ) );
}
elsif ( ( $INST =~ /PICSIT/ ) && ( $proctype =~ /scw/ ) ) {
	&IBISLIB::ISA (
		"IC_Group"       => "$IC_Group",
		"proctype"       => "$proctype",
		"INST"           => "$INST",
		"instdir"        => "$instdir",
		);
	&CorLIB::IBEP (
		"disableIsgri"  => "yes",
		"disablePICsIT" => "no",
		"INST"          => "$INST",
		) if ( ( $ENV{REDO_CORRECTION} ) && ( $proctype =~ /scw/ ) && ( $ENV{CREATE_REV_3} ) );
}
elsif ( ( $INST =~ /PICSIT/ ) && ( $proctype =~ /mosaic/ ) ){
	&IBISLIB::IPMosaic ();
}
elsif ($INST =~ /JMX(\d)/) {
	my $jemxnum = $1;
	&JMXLIB::JSA(
		"jemxnum"  => "$jemxnum",
		"IC_Group" => "$IC_Group",
		"proctype" => "$proctype",
		);
	&CorLIB::JXEP (
		"jemxnum" => "$jemxnum",
		) if ( ( $ENV{REDO_CORRECTION} ) && ( $proctype =~ /scw/ ) && ( $ENV{CREATE_REV_3} ) );
}
elsif ($INST =~ /OMC/) {
	&OMCLIB::OSA (
		"IC_Group" => "$IC_Group",
		"proctype" => "$proctype",
		);
}
elsif ($INST =~ /SPI/) {
	&SPILIB::SSA (
		"IC_Group" => "$IC_Group",
		"revno"    => "$revno",
		"proctype" => "$proctype",
		);
}
else {
	&Error ( "Made it to the end of cssscw.pl and did not match +$INST+" );
}

&ISDCLIB::DoOrDie ( "$myrm -rf $ENV{PARFILES}" ) if ( -e "$ENV{PARFILES}" );

exit 0;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

#	last line
