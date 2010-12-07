package CleanLIB;

=head1 NAME

I<CleanLIB.pm> - library used by cleanup.pl

=head1 SYNOPSIS

use I<CleanLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use Cwd;
use File::Basename;
use ISDCPipeline;
use UnixLIB;
use OPUSLIB;
use ISDCLIB;
use TimeLIB;
use SSALIB;
use Datasets;

my $hashline  = "#####################################################################################";
my $dashline  = "-------------------------------------------------------------------------------------";
my $equalline = "=====================================================================================";
my $grtrline  = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";
my $halfdashline = "----------------------------------------------";
	
my $DEBUG = 0;
my ( $retval, @result );

$| = 1;

#############################################################################

=item B<SetDEBUG > ( $DEBUG )

The $DEBUG variable is used as a switch for some print statements.

=cut

sub SetDEBUG {
	$DEBUG = $_[0];
	return;
}

#############################################################################

=item B<CleanData> ( %pars )

Wrapper around the individual cleaning routines.

=cut

sub CleanData {
	my %pars = @_;

	&dprint ( "$prefix1\n$prefix1 Processing dataset $pars{dataset}\n" ) if ( $DEBUG );

	if ( $pars{level} =~ /raw/ ) {
		print "$grtrline\n$grtrline\n";
		if ( $pars{path} =~ /revol_aux/ ) {
			&MoveData ( "$ENV{REP_BASE_PROD}/aux/adp/$pars{dataset}.000", "$ENV{REP_BASE_PROD}/cleanup/aux/adp/" );
			&MoveData ( "$ENV{REP_BASE_PROD}/aux/org/$pars{dataset}",     "$ENV{REP_BASE_PROD}/cleanup/aux/org/" );
		} elsif ( $pars{path} =~ /revol_scw/ ) {
			&MoveData ( "$ENV{REP_BASE_PROD}/scw/$pars{dataset}",       "$ENV{REP_BASE_PROD}/cleanup/scw/" );
			&MoveData ( "$ENV{REP_BASE_PROD}/obs/qs??_$pars{dataset}*", "$ENV{REP_BASE_PROD}/cleanup/obs/" );
			&CleanIndices();
		} elsif ( $pars{path} =~ /revol_ssa|consssa/ ) {
			&CleanConsssaData  ( %pars );
			&CleanIndices();
		} elsif ( $pars{path} =~ /conssa/ ) {
			&CleanConssaData   ( $pars{onedataset} );
			&CleanIndices();
		} elsif ( $pars{path} =~ /revol_qla/ ) {
			&MoveData ( "$ENV{REP_BASE_PROD}/obs/qs??_$pars{dataset}*", "$ENV{REP_BASE_PROD}/cleanup/obs/" );
			&CleanIndices();
		} elsif ( $pars{path} =~ /nrtqla/ ) {
		} elsif ( $pars{path} =~ /rev/ ) {
		} elsif ( $pars{path} =~ /scw/ ) {
		} elsif ( $pars{path} =~ /input/ ) {
		} elsif ( $pars{path} =~ /adp/ ) {
		}
	}
	return;
}


#############################################################################

=item B<GetOPUSLogLink> ( $path, $dataset, $quiet )

Returns the log link(s) in the opus directory as an array if it/they exist(s).

=cut

sub GetOPUSLogLink {
	my ( $path, $dataset, $quiet ) = @_;
	&dprint ( "$prefix1 \n$prefix1 Running GetOPUSLogLink ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );

	my $logfile = "$dataset.log";

	#  Look for log to clean:  added a trailing * to catch log_1, log_2, etc.
	&dprint ( "$prefix1 Looking for $ENV{OPUS_WORK}/$path/logs/$logfile*\n" ) if ( $DEBUG );
	my @files = glob( "$ENV{OPUS_WORK}/$path/logs/$logfile*" );
	&dprint ( "$prefix1 Found: @files\n" ) if ( $DEBUG );
	return @files;
}


#############################################################################

=item B<GetOPUSLog> ( $path, $dataset, $quiet )

Returns actual log file(s) in the opus directory as an array if it/they still exist(s).

=cut

sub GetOPUSLog {
	my ( $path, $dataset, $quiet ) = @_;
	&dprint ( "$prefix1 \n$prefix1 Running GetOPUSLog ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );

	my ($logfile,$type,$revno,$prevrev,$nexrev,$use);

	#	051124 - Jake - ( commented out by 050829 - Jake - SPR 4307 I think )
	#	It is possible that if a rev file didn't completely start, 
	#	that the actual log file and its link are still in the logs dir.
	#	This will remove the actual log file if it is there.
	#	I think that rev is the only path with a special log name
	if ($path =~ /rev$/) {
		($logfile,$type,$revno,$prevrev,$nexrev,$use) = &Datasets::RevDataset("$dataset");
		$logfile =~ s/\.fits$/_log\.txt/;
	}
	elsif ( $path =~ /adp/ ) {
		#	-rw-r--r--    1 isdc_int obs_gen      2057 Jan 19 12:09 THF_020816_0030.DAT_log.txt
		#	lrwxrwxrwx    1 isdc_int obs_gen       105 Jan 19 12:09 THF_020816_0030_DAT.log -> 
		( my $temp = $dataset ) =~ s/(.*)(_)(.*?)$/$1\.$3/g;
		$logfile = "${temp}_log.txt";
	}
	else {
		$logfile = "${dataset}_log.txt";
	}

	#	No trailing * cause there should be only 1
	&dprint ( "$prefix1 Looking for $ENV{OPUS_WORK}/$path/logs/$logfile\n" ) if ( $DEBUG );
	my @files = "$ENV{OPUS_WORK}/$path/logs/$logfile" 
		if ( -e "$ENV{OPUS_WORK}/$path/logs/$logfile" );
	&dprint ( "$prefix1 Found: @files\n" ) if ( $DEBUG );
	return @files;
}


#############################################################################

=item B<GetOPUSTrigger> ( $path, $dataset, $quiet )

Returns trigger file(s) in the opus directory as an array if it/they still exist(s).

=cut

sub GetOPUSTrigger {
	my ( $path, $dataset, $quiet ) = @_;
	&dprint ( "$prefix1 \n$prefix1 Running GetOPUSTrigger ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );

	my @files;
	my $trigger;

	#	for nrtqla 1 trigger file = 3 osf ( 1 for each inst, usually qsib, qsj1, and qsj2 )
	#	therefore, only delete the trigger when it is the last osf.
	if ( $path =~ /nrtqla/ ) {		
		unless ( $dataset =~ /^qm/ ) {
			#	qsj1_055500010010
			#	qmib_0557_04100010011_0007
			( $trigger = $dataset ) =~ s/^qs\w{2}_(\d{12})$/$1/
				or die "$prefix1 ERROR 012:  path nrtqla but dataset $dataset not format expected!";
		}
	} # end if QLA

	#	for conssa 1 trigger file per inst = multiple osf ( 1 for main and 1 for each scw )
	#	therefore, only delete the trigger after all scws and then the main osf
	elsif ( $path =~ /conssa/) {			#	050817 - Jake - SPR 4297
		#	dataset : sosp_03201020001_001_SPI	(SPI, IBIS, JMX1, JMX2, OMC)
		#	          soom_02201330003_003_OMC_030701120020
		#	logs    : sosp_03201020001_001_SPI
		#	          soom_02201330003_003_OMC_030701120020
		#	trigger : sosp_03201020001_001
		#	obs dir : sosp_03201020001_001.000

		( $trigger = $dataset ) =~ s/_[\w\d]{3,4}$//;	#	only matches for the og's osf ( not scw's which don't have triggers )
	}

	#	for consssa 1 trigger file = 1 osf
	#	therefore, remove the trigger each time
	elsif ( $path =~ /consssa/) {
		$trigger = &SSALIB::OSF2Trigger ( $dataset );
	}
	else {		#	NOTE: adp files do not have trigger files
		$trigger = $dataset;
	}

	if ( defined $trigger ) {
		&dprint ( "$prefix1 Looking for $ENV{OPUS_WORK}/$path/input/$trigger*\n" ) if ( $DEBUG );
		@files = glob( "$ENV{OPUS_WORK}/$path/input/$trigger*" );
		&dprint ( "$prefix1 Found: @files\n" ) if ( $DEBUG );
	}
	return @files;
}


#############################################################################

=item B<GetOPUSScratch> ( $path, $dataset, $quiet )

Returns the scratch directory(s) in the opus directory as an array if it/they still exist(s).

=cut

sub GetOPUSScratch {
	my ( $path, $dataset, $quiet ) = @_;
	&dprint ( "$prefix1 \n$prefix1 Running GetOPUSScratch ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );
	my @files;

	#	DO NOT DELETE THE CONSSA SCRATCH STUFF!!!!!
	unless ( $path =~ /conssa/ ) {
		&dprint ( "$prefix1 Looking for $ENV{OPUS_WORK}/$path/scratch/$dataset\n" ) if ( $DEBUG );
		@files = "$ENV{OPUS_WORK}/$path/scratch/$dataset"
			if ( -e "$ENV{OPUS_WORK}/$path/scratch/$dataset" );	#	because no wildcard, glob will return this even if it doesn't exist
		&dprint ( "$prefix1 Found: @files\n" ) if ( $DEBUG );
	}
	return @files;
}



#############################################################################

=item B<CleanOPUS> ( $path, $dataset, $quiet )

Function for cleaning OPUS workspace of all files and data to do with a particular OSF;  
runs from cleanup.pl script on command line or through OPUS cleanosf process.  

E.g.  CleanOPUS ( "adp", "orbita_20011120" );

Optional "quiet" option;  if third parameter defined, no printing except on error.

=cut

sub CleanOPUS {
	
	my ( $path, $dataset, $quiet ) = @_;		#	$quiet is usually undefined

	&dprint ( "$prefix1 \n$prefix1 Running CleanOPUS ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );
	&dprint ( "$prefix1 with 'quiet' option\n" ) if ( defined( $quiet ) && ( $DEBUG ) );
	
	#	conssa ogs have 1 trigger, but multiple osfs and logs
	if ( $path =~ /conssa/ ) {
		my @others = &ISDCPipeline::BBUpdate(
			"match"  => "$dataset",
			"return" => 1,
			"path"   => "$path",
			);
		if ( ( $#others == 0 ) && ( $others[0] =~ /$dataset/ ) ) {
			print "$prefix1 Only one (or possibly none) osf matches current $dataset;  deleting trigger.\n" unless ($quiet);
		} 

#	FIX - I think we need an elsif in here.
#		I don't think that this causes any problems, but is misleading
#		I wish I knew exactly how I got this snippet below


#	>>>>>>>     
#	>>>>>>>     Found more than 1 matching osf: 
#	> > > > > >    
#	>>>>>>>    
#	>>>>>>>     
#	>>>>>>>     Done cleaning others
#	>>>>>>>    


		else {
			#	Clean up the conssa scws first
			&dprint ( "$prefix1 \n$prefix1 Found more than 1 matching osf: \n$prefix4".join("\n$prefix4",@others)."\n$prefix1\n" );
			foreach my $other ( @others ) {		#	others includes self
				next if ( $other eq $dataset );	#	otherwise your in an infinite loop.
				&CleanOPUS ( $path, $other, $quiet );
			}
			&dprint ( "$prefix1 \n$prefix1 Done cleaning others\n$prefix1\n" );
		} 
	}



#	does this include ADP??

	my @delete;
	push @delete, &GetOPUSLog     ( $path, $dataset, $quiet );	#	usually doesn't exist
	push @delete, &GetOPUSLogLink ( $path, $dataset, $quiet );
	push @delete, &GetOPUSTrigger ( $path, $dataset, $quiet );
	push @delete, &GetOPUSScratch ( $path, $dataset, $quiet );	#	DO NOT CLEAN SCRATCH AREA FOR CONSSA!!!!!

	if ( @delete ) {
		&dprint ( "$prefix1 \n$prefix1 Will delete... \n$prefix4".join("\n$prefix4",@delete)."\n" ) unless ($quiet);

		foreach my $one (@delete) {
			&dprint ( "$prefix1 Removing $one\n" ) unless ( $quiet );
	
			#  because log is a link, no chmod.
			if ($one !~ /log/) {
				`$mychmod -R +w $one` if (!-w "$one");
				die "$prefix1 ERROR 015:  cannot chmod on $one: $!" if ($?);
			}
			else {
				print "$prefix1 $one is a link;  no chmod\n" unless ($quiet);
			}
			`$myrm -rf $one`;
		
			die "$prefix1 ERROR 016:  cannot remove $one: $!" if ($?);
		
		} # foreach delete 
	}
	else { 
		&dprint ( "$prefix1 \n$prefix1 Found nothing to delete for $path $dataset\n" ) unless ($quiet);
	}
	
	#  Can't do this here if under OPUS;  causes much BB confusion if the OSF 
	#   vanishes.  (It may appear to work, but if you try to trigger 
	#   that dataset again, there's a lock file which will mess it all up. 
	
	if (!defined($ENV{PATH_FILE_NAME})) {
		if ( `osf_test -p $path.path -f $dataset` ) {
			&dprint ( "$prefix1 Removing OSF for $dataset\n" ) unless ($quiet);
			`osf_delete -p $path.path -f $dataset`;
			die "$prefix1 ERROR 017:  cannot remove OSF $dataset" if ($?);
		} else {
			&dprint ( "$prefix1 OSF for $dataset not found.\n" ) unless ($quiet);
		}
	}
	&dprint ( "$prefix1\n" ) unless ($quiet);
	return;
}  # end sub CleanOPUS


##############################################################################

=item B<CleanRAW> ( $path, $dataset )

Function for cleaning repository for data related to an OSF; dangerous, from command line only using cleanup.pl script.

=cut

sub CleanRAW {
	#  TO BE FIXED:  Maybe move other Clean calls above?
	
	my ($path,$dataset) = @_;
	
	&dprint ( "$prefix1 \n$prefix1 Running CleanRAW ( path=$path, dataset=$dataset )\n" ) if ( $DEBUG );
	#  If cleaning raw for science windows, first have to clean prp.
	#   Given input path, have to clean prp for scw path.
	
	my $subdir;
	my ($rawfile,$type,$revno,$prevrev,$nexrev,$use);
	my $log;
	my $list;
	
	die "$prefix1 ERROR 025:  Unfortunately, I don't know quite how to do this yet."
		if ( $path =~ /aux|adp/ );
	
	print "$prefix1 Cleaning path $path dataset $dataset at RAW level\n";
	
	#  What's left to do is just remove raw data (and detach in case of
	#   raw ScWs index.)
	
	
	$dataset =~ /^(\d{4})/;
	$revno = $1;
	
	my $chmodopts = "";
	my $rmopts    = "";
	if (($path =~ /(nrt|cons)input/) || ($path =~ /(nrt|cons)scw/)) {
		#
		# Detach from raw index:
		# 
		if ( (-e "$ENV{REP_BASE_PROD}/idx/scw/raw/GNRL-SCWG-GRP-IDX.fits") && 
				(-e "$ENV{REP_BASE_PROD}/scw/$revno/$dataset.000/swg_raw.fits")) {
			&ISDCPipeline::IndexDetach(
				"root"   => "GNRL-SCWG-GRP-IDX",
				"subdir" => "$ENV{REP_BASE_PROD}/idx/scw/raw",
				"child"  => "../../../scw/$revno/$dataset.000/swg_raw.fits[GROUPING]",
				"delete" => "no"
				);
		}
		else {
			print "$prefix1 WARNING:  either index or child does not exist;  skipping index updating.\n";
		}
		
		chdir "$ENV{REP_BASE_PROD}/scw/$revno" or die "$prefix1 ERROR 026:  cannot chdir to $ENV{REP_BASE_PROD}/scw/$revno";
		$list = " $dataset.000" if (-d "$dataset.000");
		$chmodopts = " -R +w ";
		$rmopts    = " -R ";
	}  # end of if scw
	
	
	############
	#   For Rev Files:
	############
	elsif ($path =~ /^(nrt|cons)rev$/) {
		
		($rawfile,$type,$revno,$prevrev,$nexrev,$use) = &Datasets::RevDataset($dataset);
		
		#  First, check if directories are writeable
		foreach  ("","/raw","/prp","/aca","/cfg","/idx","/logs","/osm") {
			$subdir = "$ENV{REP_BASE_PROD}/scw/$revno/rev.000".$_;
			print "$prefix1 Checking permissions on $subdir\n";
			if ( (-d "$subdir") && (!-w "$subdir") ) {
				($retval,@result) = &ISDCPipeline::RunProgram("$mychmod +w $subdir");
				die "$prefix1 ERROR 028:  cannot $mychmod +w $subdir:\n@result" if ($retval);
			} # end if not writeable
			else {
				print "$prefix1 $subdir looks writeable\n";
			}
		} # end foreach subdir
		
		$log = $rawfile;
		$log =~ s/\.fits/_log\.txt/;
		$list = "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/raw/$rawfile" 
			if (-e "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/raw/$rawfile");
		$list .= " $ENV{REP_BASE_PROD}/scw/$revno/rev.000/logs/$log" 
			if (-e "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/logs/$log");
		$chmodopts = " +w ";
		$rmopts    = " ";
	} # end if rev
	
	############
	#  otherwise, error.
	############
	
	else {
		die "$prefix1 ERROR 029:  don't know how to clean $path to RAW level.";
	}
	
	#
	#  Now, just chmod and delete raw file/dir
	#
	#  (The "-R" is part of $list, put in for science windows only)
	#	050301 - Jake - not anymore
	#	Why can't the -R be used for rev?  It seems that they are just files.
	#	They wouldn't need it, but it would have been easier.

#	FIX - This is where the data is actually deleted?  Should we use &MoveData() instead?

#	if ( $list ) {	#	060208 - in light of prev problem using syntax like this ...
	if ( defined $list ) {
		($retval,@result) = &ISDCPipeline::RunProgram("$mychmod $chmodopts $list");
		die "$prefix1 ERROR 030:  cannot $mychmod $chmodopts $list:\n@result" if ($retval);
		
		($retval,@result) = &ISDCPipeline::RunProgram("$myrm $rmopts $list");
		die "$prefix1 ERROR 031:  cannot $myrm $rmopts $list\n@result" if ($retval);
	}
	else {
		print "$prefix1 WARNING:  nothing to clean\n";
	}
	
	#  Lastly, clean up opus stuff:
	&CleanOPUS ("$path","$dataset");
	return;
	
}  # end sub CleanRAW

###############################################################################

=item B<SetEnvVariable> ( %pars )

Evaluates what's installed in order to determine the path.

The issue of determining which "environment" we are in is complicated by the fact that consssa and nrtrev are always installed now because pipeline_lib/cleanup.pl sorta needs them.  It may be better to have set the $system variable, but this is not the case.  It could have also been parsed from the $ISDC_ENV variable, but this may not always be true either.  $USER is also a possibility.

In my opinion, the way this works now is a bad idea.  $env is only used as a prefix for input, scw and rev, therefore it only needs to be nrt or cons.

Modified cleanup.pl to only warn when missing SSALIB.pm
Modified cleanup.pl to only warn when missing Datasets.pm

Returns $env ( nrt,cons, adp, arc )

=cut

sub SetEnvVariable {
	my %pars = @_;
	my $installed = join(' ',`$myls $ENV{ISDC_OPUS} 2> /dev/null`);
	my $env = "nrt";	#	set default to nrt

	#  First of all, is our path installed?
	if ($pars{path} =~ /revol_aux/) {
		die "$prefix1  ERROR 032:  ADP not installed under $ENV{ISDC_OPUS}" 
			unless ($installed =~ /adp/);
	}
	elsif ($pars{path} =~ /revol_scw/) {
		foreach ("input","scw","rev") {
			die "$prefix1 ERROR 033:  $_ pipeline not installed under $ENV{ISDC_OPUS}" 
				unless ($installed =~ /$_/);
		}
	}
	elsif ($pars{path} =~ /revol_ssa/) {
		die "$prefix1  ERROR 034:  Consssa not installed under $ENV{ISDC_OPUS}" 
			unless ($installed =~ /consssa/);
	}
	elsif ($pars{path} =~ /revol_qla/) {
		die "$prefix1  ERROR 035:  nrtqla not installed under $ENV{ISDC_OPUS}" 
			unless ($installed =~ /nrtqla/);
	}

	if    ( $installed =~ /cons/    ) { $env = "cons"; }
	elsif ( $installed =~ /adp/     ) { $env = "adp";  }
	elsif ( $installed =~ /arcdd/   ) { $env = "arc";  }
	else  { $env = "XXX"; }
#	else { die "$prefix1 ERROR 035:  OPUS installation not recognized!\n"; }

	return $env;
}


###############################################################################

=item B<ValidateRequest> ( %pars )

Check that what was requested is allowed.

=cut

sub ValidateRequest {
	my %pars = @_;

	my $answer;

	#  For ADP, only OPUS level cleaning allowed (or raw for revol_aux)
	die "$prefix1 ERROR 039:  clean up for level $pars{level} in path ADP not allowed." 
		if ( ( $pars{level} !~ /opus/ ) && ( $pars{path} =~ /adp/ ) );

	#  For revol_aux, OPUS level or raw allowed, only prp forbidden:  
	die "$prefix1 ERROR 040:  clean up of prp data is not allowed for ADP!" 
		if ( ( $pars{level} =~ /prp/ ) && ( $pars{path} =~ /revol_aux/ ) );

	#  Shouldn't be possible through OMG, but just in case:
	die "$prefix1 ERROR 041:  clean up to $pars{level} not allowed for Input!" 
		if ( ( $pars{level} =~ /prp/ ) && ( $pars{path} =~ /input/ ) );

	die "$prefix1 ERROR 042:  Cannot clean revol_ssa for revolution 0000!"
		if ( ( $pars{path} =~ /revol_ssa/ ) && ( $pars{dataset} =~ /0000/ ) );

	if ( ( ( $pars{level} =~ /raw/ ) || ( $pars{path} =~ /revol_/ ) ) && !( $pars{"do_not_confirm"} ) ) {
		#  Ask if the operator is sure:
		print STDOUT "$prefix1 CONFIRM:  do you really want to remove $pars{dataset} "
			."in path $pars{path} down to and including level $pars{level}?\n"
			."$prefix1 If so, please type \`yes\`:  ";
		while( <STDIN> ) { $answer = $_; chomp $answer; last; }
		if ( $answer !~ /^yes$/ ) {
			print STDOUT "$prefix1 You did not answer \`yes\`;  quitting\n";

#	FIX: LOG is opened in cleanup.pl, so I can't close it here.
#		Do I really need to?
#			if ($pars{log}){  close(LOG); }
			exit;
		}
	}
	return;
}



###############################################################################

=item B<StartCleanup> ( %pars )

Mostly a wrapper for CleanOPUS, but also can call CleanRAW for paths *input, *rev and *scw.

=cut

sub StartCleanup {
	my %pars = @_;

	&dprint ( "$prefix1 \n$prefix1 Running StartCleanup for $pars{onedataset}\n$prefix1\n" ) if ( $pars{DEBUG} );

	#    print "\n\n$grtrline\n";
	#    print "$grtrline\n";
	print "$prefix1 ".&TimeLIB::MyTime()."  CLEANING dataset $pars{onedataset} in path $pars{onepath} to level $pars{level}\n";
	print STDOUT "$prefix1 ".&TimeLIB::MyTime()."  CLEANING dataset $pars{onedataset} in path $pars{onepath} to level $pars{level}\n" if ( $pars{DEBUG} );
	#    print "$grtrline\n";
	#    print "$grtrline\n";

	#  The datasets have returns and spaces as given by osf_test:
	#$pars{onedataset} =~ s/\s+//g;

	########################################################################
	#            Check the OSF status first:  must not be processing.
	#
	print STDOUT "$prefix2 &CheckOSF  ( $pars{onepath} , $pars{onedataset} ); from MAIN\n" if ( $pars{DEBUG} );
	&OPUSLIB::CheckOSF ($pars{onepath},$pars{onedataset});

	if ($pars{level} =~ /opus/i) {
		&CleanOPUS ( $pars{onepath}, $pars{onedataset} ) 
	}
	elsif ($pars{level} =~ /raw/i) {
		#  Here too, since we're cleaning up indices, we must make sure that everything has been stopped.  
		&OPUSLIB::CheckProcs ( $pars{FORCE} );

		#  If running revol_scw, onepath is input but scw must be cleaned too;
		#   Then, the actuall raw cleaning is done below, since it's easy.

		if ( $pars{path} =~ /revol_scw/ ) { 
			&CleanOPUS ("$pars{env}scw", $pars{onedataset} ) if ($pars{onepath} =~ /^$pars{env}input$/);
			&CleanOPUS ( $pars{onepath}, $pars{onedataset} );
		}
		elsif ( $pars{path} =~ /(revol_ssa|consssa|conssa)/ ) { 
			&CleanOPUS ( $pars{onepath}, $pars{onedataset} );
		}
		elsif ( $pars{path} =~ /(revol_qla|nrtqla)/ ) { 
		}





#		FIX what about adp and nrtqla????





		else {		#	*input,*rev,*scw,  (adp?)
			#  For ScWs, this function calls clean PRP first, then cleans RAW and
			#   OPUS levels as well.
			&CleanRAW ("$pars{onepath}", "$pars{onedataset}");
		}

	}  #  end of level raw

	else {
		die "$prefix1 ERROR 046:  don't recognize level $pars{level}";
	}

	return;
}


################################################################################

=item B<CleanIndices> ( )

No arguments.  Just clean.  All of them.

=cut

sub CleanIndices {

	print "$grtrline\n$grtrline\n";
	print "$prefix1 Cleaning the indices\n";
	print "$grtrline\n$grtrline\n";
	#  The science window indices
	my @indices;  
	push @indices, "scw/raw/GNRL-SCWG-GRP-IDX";
	push @indices, "scw/GNRL-SCWG-GRP-IDX";

	#  Get all the rev global indices
	foreach my $type ( keys( %Datasets::IndicesGlobal ) ) {
		foreach my $root ( keys( %{ $Datasets::IndicesGlobal{$type} } ) ) {
#			my $temp = "rev/";
#			$temp .= $Datasets::IndicesGlobal{$type}{$root};
#			$temp .= "-IDX";
#			push @indices, $temp;
			push @indices, "rev/$Datasets::IndicesGlobal{$type}{$root}-IDX";
		}
	}

	#  And lastly, the OG indices:
	push @indices, "obs/GNRL-OBSG-GRP-IDX";

	chdir "$ENV{REP_BASE_PROD}/idx" 
		or die "$prefix1 ERROR 058:  cannot chdir to $ENV{REP_BASE_PROD}/idx";

	foreach my $index ( @indices ) {
		my $root = &File::Basename::basename ( $index );
		my $subdir = $ENV{REP_BASE_PROD}."/idx/".&File::Basename::dirname( $index );
		&ISDCPipeline::CleanGroup (
			"root"    => "$root", 
			"subdir"  => "$subdir", 
			"descend" => "0");
	} # foreach index
	return;
}


###############################################################################

=item B<CleanConsssaData> ( %pars )

Find and move all matching data from the obs* directory(s) to the cleanup directory.

=cut

sub CleanConsssaData {
	my %pars = @_;
	my $dataset = ( exists ( $pars{onedataset} ) ) ? $pars{onedataset} : $pars{dataset};
	#	$pars{onedataset} is used for cleaning consssa
	#	$pars{dataset}    is used for cleaning revol_ssa ( single or multiple )

	#	Possible datasets
	#	dataset sssp_002500010010
	#	dataset 002500010010
	#	dataset 00250001  ??????
	#	dataset 0025      ??????
	#	dataset 002       ??????
	#	dataset 0         ??????

	&dprint ( "$prefix1 \n$prefix1 Running CleanConsssaData ( dataset=$dataset )\n" ) if ( $DEBUG );
	&dprint ( "$prefix1 Preparing to move consssa data for $dataset to $ENV{REP_BASE_PROD}/cleanup/\n" );

	my $revno;
	my @obsdirs;
	foreach ( split ",", $pars{inst} ) {
		push @obsdirs, &SSALIB::inst2instdir( $_ );
	}
	#  FIX : This may/will be a problem because of 2 instruments for jemx
	#  No easy way to differentiate between the 2 without doing completely different

	my $StartDir = `pwd`;

	if ( $dataset =~ /^[\w12]{4}_/ ) {		#	just a single instrument dataset
		( $revno ) = ( $dataset =~ /^[\w12]{4}_(\d{4})/ );
		my $instr = &SSALIB::OSF2Trigger ( $dataset );
		$instr =~ s/^.+_([omcisgripicsitspijmx]+)\d*$/$1/;
		@obsdirs = ( &SSALIB::inst2instdir( $instr ) );
		#@obsdirs = ( "obs_$instr" );
	} elsif ( $dataset =~ /^(\d{1,4})$/ ) {	#	An entire revolution	#	060203 - changed from 4 to 2,4	#	changed to 1,4
		$revno = $1;
	} elsif ( $dataset =~ /^(\d{4})/ ) {	#	portion of a revolution or single scw, all instruments
		$revno = $1;
		$dataset =~ s/^/*_/;
	} else {
		die "Unexpected result from parsing the dataset $dataset";
	}

	push my @rep_base_prods, $ENV{REP_BASE_PROD};
	if ( $ENV{REP_BASE_PROD} =~ /cons/ ) {
		my $TMP_BASE_PROD = $ENV{REP_BASE_PROD};		#	expecting /reproc/cons/ops_sa
		$TMP_BASE_PROD =~ s/cons/anaB\*\/cons/;		#	/reproc/anaB*/cons/ops_sa
		push @rep_base_prods, glob ( $TMP_BASE_PROD );
	}

	foreach my $rep_base_prod ( @rep_base_prods ) {	#	SPR 4390
		&dprint ( "$prefix2 Currently working rep_base_prod : $rep_base_prod\n" );

		foreach my $obsdir ( @obsdirs ) {
			my $fullobs   = "$rep_base_prod/$obsdir";
			my $fullclean = "$rep_base_prod/cleanup/$obsdir";
	
			print STDOUT "$prefix2 Working $obsdir now.\n" if ( $DEBUG );
			&dprint ( "$prefix2 In obsdir $obsdir\n" );
	
			chdir $fullobs;
	
			unless ( $revno =~ /0000/ ) {
				foreach my $revdir ( glob ( "$revno*.000" ) ) {		# NEED the .000 bc during processing we link the .001 or .002 or whatever!
					if ( -d $revdir ) {
						if ( $dataset =~ /^(\d{1,4})$/ ) {	#	060203 - changed from 4 to 2,4	#	changed to 1,4
							&MoveData ( "$fullobs/$revdir", "$fullclean/" );
						} else {
							&MoveData ( "$fullobs/$revdir/$dataset", "$fullclean/$revdir" );		#	FIX - Should there be a * after $dataset?????	Before????
							&RemoveDirsIfEmpty ( $revdir );
						}
					} else {
						&dprint ( "$prefix1 No dirs found matching revno $revno*.000\n" );
					}
				}
			} else { # revno is 0000	#	I don't think that this is really used yet, so its not well tested.
#	071203 - SCREW 2058 - Don't strip this out
#				$dataset =~ s/_0000//;
				&MoveData ( "$fullobs/$dataset", "$fullclean" );	#	FIX - Should there be a * after $dataset?????	Before????
			}

			if ( -e "$rep_base_prod/scratch/" ) {
				chdir "$rep_base_prod/scratch/";
				unless ( $revno =~ /0000/ ) {
					foreach my $revdir ( glob ( "$revno*" ) ) {
						if ( -d $revdir ) {
							if ( $dataset =~ /^(\d{1,4})$/ ) {	#	060203 - changed from 4 to 2,4	#	changed to 1,4
								&MoveData ( "$rep_base_prod/scratch/$revdir", "$rep_base_prod/cleanup/scratch/" );
							} else {
								&MoveData ( "$rep_base_prod/scratch/$revdir/$dataset*", "$rep_base_prod/cleanup/scratch/$revdir/" );
								&RemoveDirsIfEmpty ( $revdir );
							}
						} else {
							&dprint ( "$prefix1 No dirs found matching revno $revno*\n" );
						}
					}
				}
			}	#	if -e scratch
		}	# foreach $obsdir
	}	#	foreach $rep_base_prod

	chdir $StartDir;
	return;
}

###############################################################################

=item B<CleanConssaData> ( $dataset )

Potentially merge this function with CleanConsssaData.

=cut

sub CleanConssaData {
	my ( $dataset ) = @_;

	#	dataset : sosp_03201020001_001_SPI
	#	logs    : sosp_03201020001_001_SPI
	#	trigger : sosp_03201020001_001
	#	obs dir : sosp_03201020001_001.000

	&dprint ( "$prefix1 \n$prefix1 Running CleanConssaData ( dataset=$dataset )\n" ) if ( $DEBUG );
	#	if you don't anchor at the end, cleaning the first scw of the obs will move the data
#	my ($obsdir) = ( $dataset =~ /(so[\w\d]{2}_\d{11})_\d{3}_[\w\d]{3,4}/ )
	my ($obsdir) = ( $dataset =~ /(so[\w\d]{2}_\d{11})_\d{3}_[\w\d]{3,4}$/ )
		or &dprint ( "$prefix1 WARNING 060: path conssa but dataset $dataset (like sosp_03201020001_001_SPI) not format expected!\n" );

	$obsdir      = "$ENV{REP_BASE_PROD}/obs/$obsdir";
	my $obsclean = "$ENV{REP_BASE_PROD}/cleanup/obs";
	print STDOUT "$prefix1 Preparing to move conssa data for $dataset to $obsclean/\n";
	
	my @obsdirs = glob ( "$obsdir*" );
	if ( @obsdirs ) {
		foreach ( @obsdirs ) {
			chomp;
			next unless ( $_ );
			&MoveData ( "$_", "$obsclean" );
		}
	} else {
		&dprint ( "$prefix1 No $obsdir* exists, so skipping this part of cleanup.\n" );
	}
	return;
}

###############################################################################

=item B<MoveData> ( $sources, $target )

This function does a relatively simple task.  So simple that it almost need not be written.  The little details like mkdir, chmod and some error checking make it a bit handy though.  And now it seems a bit complicated.

=cut

sub MoveData {
	my ( $sources, $target ) = @_;

	&dprint ( "$prefix1 \n$prefix1 Running MoveData ( sources=$sources, \n$prefix3 target=$target )\n" ) if ( $DEBUG );

	foreach my $source ( glob ( $sources ) ) {
		&dprint ( "$prefix1 Checking $source \n" ) if ( $DEBUG );

		if ( ( -e $source ) || ( -l $source ) ) {	#	dead links are -l, but not -e
	
			&dprint ( "$prefix1 Moving $source \n" ) if ( $DEBUG );
			#
			#	FIX
			#	Why don't I just see if the dir is writeable or if the runner is the owner?
			#	Why do a chmod at all?  Just move it and worry about the chmod later.
			#
	
			my $firstfile = `$myfind $source -type f | $myhead -1 2> /dev/null`;
	
			if ( $firstfile ) {
				chomp $firstfile;
				my @filestats = stat ( $firstfile );
		
				#	user archive is uid 4866
				if ( $filestats[4] !~ /4866/ ) {
					&dprint ( "$prefix1 File in $source not owned by archive(4866), therefore trying chmod\n" );
					@result = `$mychmod -R +w $source`;
					die "$prefix1 ERROR 061:  cannot $mychmod -R +w $source" if ($?);
	
				} else {
					&dprint ( "$prefix1 File in $source owned by archive(4866), therefore NOT trying chmod\n" );
				}
			} else {
				&dprint ( "$prefix1 Cannot $myfind files in $source\n" );
			}
	
			@result = `$mymkdir -p $target` unless ( -d "$target" );
			die "$prefix1 ERROR 062:  cannot $mymkdir -p $target" if ($?);

			#	060210 - This is effectively going to rename the $source in the cleanup area.
			my $basename = &File::Basename::basename ( $source );
			if ( -e "$target/$basename" ) {
				my $ext = 0;
				while ( -e "$target/$basename.$ext" ) { $ext++;	}	#	append a number if already exists
				$target .= "$basename.$ext";
				&dprint ( "$prefix1 Source already exists in initial target.  Using new target of $target.\n" );
			}
	
			&dprint ( "$prefix1 Moving $source \n$prefix4 to $target\n" ) if ( $DEBUG );
			@result = `$mymv $source $target`;
			die "$prefix1 ERROR 063:  cannot $mymv $source $target" if ($?);
	
		} else {
			&dprint ( "$prefix1 $source does not exist so can't move\n" );
		}
	}
	return;
}


####################################################################################################

=item B<BuildPathsHashTable> ( %pars )

Determines the paths and osfs that the user has requested be cleaned.

returns %paths

=cut

sub BuildPathsHashTable {
	my %pars = @_;
	my %paths;

	&dprint ( "$prefix1 \n$prefix1 Running BuildPathsHashTable ( dataset=$pars{dataset}, path=$pars{path}, level=$pars{level} )\n" ) if ( $DEBUG );

	#$pars{env} = "cons" unless ( $pars{env} );
	#        
	#    Set up how much data we're dealing with:
	#        

	if ( $pars{path} =~ /revol_(aux|scw|ssa|qla)/) {
		if  ($1 =~ /aux/) {
			%paths = ( "adp" => [ `osf_test -p adp.path -pr dataset | grep "^$pars{dataset}"` ]);
		}

		#  To clean up scw repository, use the Input blackboard to start with, since
		#   it's likely more complete than the ScW.  
		elsif ($1 =~ /scw/){
			#  Always include Rev:  
			$paths{"$pars{env}rev"} =  [ `osf_test -p $pars{env}rev.path -pr dataset | grep "^$pars{dataset}"` ];

			$paths{"$pars{env}input"} = [ `osf_test -p $pars{env}input.path -pr dataset | grep "^$pars{dataset}"` ];
			#  Do ScW also unless level RAW (since in that case, we'll use the input
			#   blackboard which is more complete.)
			$paths{"$pars{env}scw"} = [ `osf_test -p $pars{env}scw.path -pr dataset | grep "^$pars{dataset}"` ]
				unless ($pars{level} =~ /raw/);
			#   die "$prefix1 ERROR 044:  problem reading blackboard $ENV{OPUS_WORK}/$pars{env}scw/obs\n" if ($?);   
		}
		elsif ($1 =~ /ssa/){
			#  Example output from osf_test: ssii_002400050010


			#	FIX - 060203 - NOTE: THIS INCLUDES MOSAICS.  Is this what we want?  Perhaps add revol_sma?
			foreach ( split ",", $pars{inst} ) {
				my $in = &ISDCLIB::inst2in ( $_ );
				push @{$paths{consssa}}, `osf_test -p consssa.path -pr dataset | grep "${in}_$pars{dataset}"`;
			}
		}

		elsif ($1 =~ /qla/){
			foreach ( split ",", $pars{inst} ) {
				my $in = &ISDCLIB::inst2in ( $_ );
				push @{$paths{nrtqla}}, `osf_test -p nrtqla.path -pr dataset | grep "qs${in}_$pars{dataset}"`;
			}
		}





		else {
			die "$prefix1 ERROR 045:  don't recognize path  $pars{path}";
		}
	} # end of revol_ cases

	elsif ( $pars{bydate} ){
		$paths{$pars{path}} = [ `osf_test -p $pars{path}.path -pr dataset` ];
	}

	elsif ( $pars{match} ){
		$paths{$pars{path}} = [ `osf_test -p $pars{path}.path -pr dataset | grep "$pars{dataset}"` ];
	}
	else {	#	conssa, consssa, *scw, *input, *rev
		#  Otherwise, you have only one dataset, so a hash of one path and one osf 
		%paths = ( "$pars{path}" => [ $pars{dataset} ]);
	}

	if ( $pars{bydate} ) {
		foreach my $onepath ( keys ( %paths ) ) {
			@{ $paths{$onepath} } = &OPUSLIB::RemoveDatasetsAfterDate (
				"path"           => $onepath,
				"datasets"       => [@{ $paths{$onepath}}],
				"date"           => $pars{dataset},
				#	"do_not_confirm" => "yes",
				);
		}
	}

	return %paths;
}


#############################################################################

=item B<GetPars> ( )

Really should've just used the perl C<-s> option instead.

returns %params

=cut

sub GetPars {
	my ($key,$value);
	my %params;
	if ( $ENV{PATH_FILE_NAME} ) {
		( $params{"path"}  = $ENV{PATH_FILE_NAME} ) =~ s/\.path//;  # have to take that off for the moment
		$params{"dataset"} = $ENV{OSF_DATASET};
		$params{"level"}   = "opus" if ($ENV{PROCESS_NAME} =~ /cleanopus/);
	} elsif (@ARGV) {
		foreach (@ARGV) {
			($key,$value) = split('=',$_);
			$key =~ s/^--//;
			if ( defined ( $value ) )  {	
				$params{$key} = $value;
			} else {
				$params{$key}++;
			}
		}
	}

	#	check for mistyped and/or extra params
	foreach ( keys(%params) ) {
		next if ( /^path$/ );
		next if ( /^level$/ );
		next if ( /^dataset$/ );
		next if ( /^inst$/ );
		next if ( /^DEBUG$/ );
		next if ( /^FORCE$/ );
		next if ( /^match$/ );
		next if ( /^dryrun$/ );
		next if ( /^do_not_confirm$/ );
		die "Unknown parameter given : $_";
	}

	$params{inst} = "ibis,isgri,picsit,jemx1,jemx2,omc,spi" unless ( $params{inst} );

	die "$prefix1 ERROR 008:  cannot read all parameters;  you must give path, dataset, and level."
		unless ( exists ($params{"path"}) && exists ( $params{"dataset"} ) && exists ($params{"level"} ) );
	return %params;
}

############################################################################## 

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
