package ISDCLIB;

=head1 NAME

I<ISDCLIB.pm> - some generic ISDC perl functions

=head1 SYNOPSIS

use I<ISDCLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

#	This function does not and should not ever need any other ISDC perl package or module.
#	This way, it will remain simple and easily maintainable without any crossover.

use UnixLIB;	#	everybody is gonna need UnixLIB

#	next 3 statements required for use without using ISDCLIB:: to prefix
#	cannot use with "use strict;"
use Exporter();
@ISA = qw ( Exporter );
@EXPORT = qw ( 
	$prefix1 
	$prefix2 
	$prefix3 
	$prefix4 
	&dprint 
	&Message 
	&Error 
	&RemoveDirsIfEmpty 
	&ProcStep
	);

sub ISDCLIB::Initialize;
sub ISDCLIB::dprint;
sub ISDCLIB::Message;
sub ISDCLIB::Error;
sub ISDCLIB::RemoveDirsIfEmpty;
sub ISDCLIB::RowsIn;
sub ISDCLIB::ChildrenIn;
sub ISDCLIB::ProcStep;
sub ISDCLIB::FindDirVers;
sub ISDCLIB::GetColumn;
sub ISDCLIB::QuickClean;
sub ISDCLIB::QuickDalAttach;
sub ISDCLIB::QuickDalDetach;
sub ISDCLIB::QuickDalDelete;
sub ISDCLIB::QuickDalCopy;
sub ISDCLIB::QuickDalClean;
sub ISDCLIB::ParseConfigFile;
sub ISDCLIB::DoOrDie;
sub ISDCLIB::inst2in;
sub ISDCLIB::in2inst;

$| = 1;

$prefix1   = ">>>>>>>    ";
$prefix2   = "> > > >    ";
$prefix3   = "> > > > >    ";
$prefix4   = "> > > > > >    ";

########################################################################### 

=item B<Initialize> ( )

Does basic routines for most all pipeline steps.

Requires that OSF_DATASET, PARFILES and LOG_FILES be set prior to execution.

=cut

sub Initialize {
	my $proc = &ProcStep ();
	chomp ( my $node = `$myuname -n` );
	chomp ( my $os   = `$myuname -s` );
	chomp ( my $ver  = `$myuname -r` );
	&Message ( "STARTING on $node running $os $ver" );
	$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
	$ENV{PFILES}        = "$ENV{PARFILES};$ENV{ISDC_ENV}/pfiles";

	return ( $proc );
}  


########################################################################### 

=item B<dprint> ( @message )

Prints to STDOUT and whatever is currently "selected".

=cut

sub dprint {
#	#	try to not print twice bc usually in the ST step there may not be a logfile selected yet
	my $CURRENT = select STDOUT;
	print          "@_"; #	STDOUT
	print $CURRENT "@_" if ( $CURRENT !~ /STDOUT/ );	#	main::STDOUT
	select $CURRENT;
#	print        "@_";
#	print STDOUT "@_";
}  

###########################################################################

=item B<Error> ( @message )

Prints given message and the "Death Stack" to the OSF logfile, then die's.

After stumbling upon it, I believe that this is very similar to Carp::confess except that this prints to the log and STDOUT.

=cut

sub Error {
	my @message = @_;
	my $proc = &ProcStep();
	my $logfile = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
	my $oldlog;

	if ( -e $logfile ) {
		open LOG, ">> $logfile";
		$oldlog = select LOG;
	}
#	print "$prefix1\n";
#	#	the array usually adds a trailing \n so may not need it
#	print "$prefix1 $proc - ERROR : @message\n";
#	print "$prefix1\n";
#	if ( -e $logfile ) {
#		close LOG;
#		select $oldlog;
#	}
	&dprint ( "\n\n$prefix1 ERROR : @message\n\n" );

	my $level=0;
	&dprint ( "\nDumping death stack\n" );
	&dprint ( "Death on line ".__LINE__." of ".__FILE__." ( ".__PACKAGE__." )\n" );		#	should be this line right here
	while ( my ($pkg,$file,$line,$sub) = caller($level++)) {
		if( $file =~ /^\(eval/ ) {
#			$oline = (caller($level))[2];
#			$oscope .= "the eval at line $oline of ";
#			$eval++;
		} else {
			&dprint ( "Death on line $line of $file ( $pkg )( $sub )\n" );
#			print "$eval,$line,$file,$oscope\n";
		}
	}

	if ( -e $logfile ) {
		close LOG;
		select $oldlog;
	}
	die;
	exit 1;
}

###########################################################################

=item B<Message> ( @message )

Prints given message the the OSF logfile.

=cut

sub Message {
	my @message = @_;
	my $proc = &ProcStep();
	my $logfile = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";
	my $oldlog;

	if ( -e $logfile ) {
		open LOG, ">> $logfile";
		$oldlog = select LOG;
	}
	print "$prefix1\n";
	#	the array usually adds a trailing \n so may not need it
	print "$prefix1 $proc : @message\n";
	print "$prefix1\n";
	if ( -e $logfile ) {
		close LOG;
		select $oldlog;
	}
	return;
}


########################################################################### 

=item B<RemoveDirsIfEmpty> ( @dirs )

This function takes a list of directories, checks to see if they are empty and then tries to remove them if they are.

=cut

sub RemoveDirsIfEmpty {
	my ( @dirs ) = @_;
   
	foreach my $dir ( @dirs ) {
		&dprint ( "$prefix1 Checking dir $dir for possible removal.\n" );
		if ( -d $dir ) {
			my @filelist = glob ( "$dir/*" );
			unless ( @filelist ) {
				&dprint ( "$prefix1 $dir appears to be empty so removing.\n" );
				print `rmdir $dir`;
			} else {
				&dprint ( "$prefix1 $dir appears to NOT be empty.\n" );
			}
		} elsif ( -e $dir ) {
			&dprint ( "$prefix1 WARNING: $dir is not a directory????\n" );
		} else {
			&dprint ( "$prefix1 \`pwd\` : ".`pwd` );
			&dprint ( "$prefix1 WARNING: $dir does not exist\n" );
		}
	}
}

########################################################################### 

=item B<ChildrenIn> ( $grpdol, $extension )

dal_list wrapper.

returns the number of Children specified by the dal_list.
returns -1 if dal_list produces no results to parse.

(Currently, no file existance checking is done.)
(I should probably modify this to return, say, -2 if the file doesn't exist.)

=cut

sub ChildrenIn {
	my ( $grpdol, $extension ) = @_;

	&Message ( "Running ChildrenIn ( $grpdol, $extension )" );
	my $initialcommonlogfile = $ENV{COMMONLOGFILE};
	$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} unless ( $ENV{COMMONLOGFILE} =~ /^\+/ );
	my @results = `dal_list dol=$grpdol extname=$extension exact=n longlisting=yes fulldols=no mode=h`;




	&Error ( "dal_list appears to have failed with $?" ) if ( $? );		#	070125 - Jake - added due to noted continuing after failure in OSA6 qla tests




	$ENV{COMMONLOGFILE} = $initialcommonlogfile;
	
	my $rows;                                               #       undefined variable
	my $cur_rows;
	my $cur_struct;
	foreach ( @results ) {           #       loop through all lines of output
		next unless /\s+GROUP\s+(\d+)\s+child\w*\s*/; 
		#next unless /\s+GROUP\s+(\d+)\s+children\s*/; 
		# next unless /Rows=\s+(\d+)\s*/; 

		$cur_rows = $1;
	
		#dal_list output format
		#
		#Ex. 1   (IDX has no Rows, but its children do and would be summed)
		#Log_1  : SPI.-GNRL-GTI-IDX    GROUP 4 children
		#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
		#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
		#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
		#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
		#
		#Ex. 2 (normal structure with Rows)
		#Log_1  : SPI.-OMP3-ALL        TABLE Cols=         8, Rows=      2086

		/^\s*Log_1\s+:\s+([\w\.-]{13})\s+/;
		$cur_struct = $1;
		
		#       Sum up all the Rows except if in $donotcountlist
		$rows += $cur_rows; # unless $donotcountlist =~ /$cur_struct/;
	}
	#       return -1 if no match or if grpdol does not exist
	$rows = -1 if !defined($rows);

	return $rows;
}	#	end ChildrenIn


##############################################################################

=item B<RowsIn> ( $grpdol, $extension, $donotcountlist )

dal_list wrapper.

returns the total number of all rows in $grpdol matching $extension specified by the dal_list.
returns -1 if dal_list produces no results to parse or if the file associated with grpdol does not exist.

(I should probably modify this to return, say, -2 if the file doesn't exist.)

=cut

sub RowsIn {         #  040623 - Jake - SPR 3725
	my ($grpdol, $extension, $donotcountlist) = @_;

	&Message ( "Running RowsIn ( $grpdol, $extension, $donotcountlist )" );

	#  ISDCPipeline::GetICFile will cause the "+" to be removed from $ENV{COMMONLOGFILE}
	#  ( or any call to PipelineStep with getstdout=0 )
	#  When the prior ISDCPipeline::PipelineStep runs, it would be put back.
	#  If the prior ISDCPipeline::PipelineStep fails, it will still be "missing".
	#     wrote SCREW 1437 to address this as the following step shouldn't be needed
	#$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} unless ( $ENV{COMMONLOGFILE} =~ /^\+/ );
	
	my ( $root, $path, $ext );

	if ( $grpdol =~ /\[/ ) {
		($root,$path,$ext) = &File::Basename::fileparse( $grpdol, '\[.*');
	} elsif ( $grpdol =~ /\+/ ) {
		($root,$path,$ext) = &File::Basename::fileparse( $grpdol, '\+.*');
	} else {
		#	These are only used to see if the file exists
		$path = &File::Basename::dirname  ( $grpdol );
		$root = &File::Basename::basename ( $grpdol );
	}

	my $rows;                  #       undefined variable

# /isdc/arc/rev_2/scw/0044/004400120010.001/ibis_gti.fits[IBIS-GNRL-GTI,18,BINTABLE]

	if ( ( -e "$path/$root" ) || ( -e "$path/$root.gz" ) ) {
#		&Message ( "$path/$root(.gz) exists." );
#		&Message ( "COMMONLOGFILE is : $ENV{COMMONLOGFILE}" );
		my $initialcommonlogfile = $ENV{COMMONLOGFILE};
		$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} unless ( $ENV{COMMONLOGFILE} =~ /^\+/ );
		my @results = `dal_list dol=$grpdol extname=$extension exact=n longlisting=yes fulldols=no mode=h`;




	&Error ( "dal_list appears to have failed with $?" ) if ( $? );		#	070125 - Jake - added due to noted continuing after failure in OSA6 qla tests




		$ENV{COMMONLOGFILE} = $initialcommonlogfile;
		
		my $cur_rows;
		my $cur_struct;
		foreach ( @results ) {      #       loop through all lines of output
#			&Message ( "RowsIn Line: $_" );
			next unless /Rows=\s+(\d+)\s*/;
			$cur_rows = $1;
			#dal_list output format
			#
			#Ex. 1   (IDX has no Rows, but its children do and would be summed)
			#Log_1  : SPI.-GNRL-GTI-IDX    GROUP 4 children
			#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
			#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
			#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
			#Log_1  :     SPI.-GNRL-GTI        TABLE Cols=         6, Rows=         1
			#
			#Ex. 2 (normal structure with Rows)
			#Log_1  : SPI.-OMP3-ALL        TABLE Cols=         8, Rows=      2086
			/^\s*Log_1\s+:\s+([\w\.-]{13})\s+/;
			$cur_struct = $1;
			#  Sum up all the Rows except if in $donotcountlist
			$rows += $cur_rows unless $donotcountlist =~ /$cur_struct/;
		}
		#  return -1 if no match or if grpdol does not exist
		$rows = -1 if !defined($rows);
	} else {
#		&Message ( "$path/$root(.gz) does not exist." );
		$rows = -1;
	}
	
	return $rows;
}  #  end RowsIn


########################################################################### 

=item B<ProcStep> ( )

The function simply translates processes and paths into a tag to include in the logging information, e.g. "DP (NRT)", "IREM (Cons.)", etc.  It takes no arguments but simply looks at the OPUS environment variables.

returns $proc.

=cut

sub ProcStep {
	# In internal functions which don't have the step info, pars the
	#  Process Name and use defaults, i.e. turn nswdp to "SD (NRT)".  

	#	060109 - Jake
	#	I just noticed that none of the input resource files have a PROCESS_NAME.
	#	Why?
	#	Is there something different about the input pipelines here?
	#	I added them, but I don't know that it'll make any difference.

	my $proc;

	if ($ENV{PROCESS_NAME} =~ /^csa/)       { $proc = "SA" ; }
	if ($ENV{PROCESS_NAME} =~ /^css/)       { $proc = "SSA" ; }
	if ($ENV{PROCESS_NAME} =~ /^nql/)       { $proc = "QLA"; }
	if ($ENV{PROCESS_NAME} =~ /^adp/)       { $proc = "ADP";}
	if ($ENV{PROCESS_NAME} =~ /^\winp/)     { $proc = "INPUT"; }	#	ninput.resource ???
	if ($ENV{PROCESS_NAME} =~ /^\wsw/)      { $proc = "ScW"; }		#	cswosm, ...
	if ($ENV{PROCESS_NAME} =~ /^\wrv/)      { $proc = "Rev"; }		#	nrvfin, ...

	#	SA Possibilities:
	if ($ENV{PROCESS_NAME} =~ /^\w{3}sw\d$/) { $proc .= " (Scw)"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}ob\d$/) { $proc .= " (Obs)"; }

	#	SSA/QLA Possibilities:
	if ($ENV{PROCESS_NAME} =~ /^\w{3}scw$/)  { $proc .= " (Scw)"; }

	# ScW Possibilities:
	if ($ENV{PROCESS_NAME} =~ /^\w{3}dp$/)  { $proc .= " DP"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}cor$/) { $proc .= " COR"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}osm$/) { $proc .= " OSM"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}avg$/) { $proc .= " AVG"; }

	#  Rev possibilities:
	if ($ENV{PROCESS_NAME} =~ /^\w{3}ire/)  { $proc .= " IRE"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}irn/)  { $proc .= " IRN"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}irv/)  { $proc .= " IRV"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}irc/)  { $proc .= " IRC"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}idp/)  { $proc .= " IDP"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}prc/)  { $proc .= " PRC"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3}jmf/)  { $proc .= " JMF"; }# JMX? FRSS
	if ($ENV{PROCESS_NAME} =~ /^\w{3}gen/)  { $proc .= " GEN"; }# Generic
	if ($ENV{PROCESS_NAME} =~ /^\w{3}jme/)  { $proc .= " JME"; }# JMX? ECAL
	if ($ENV{PROCESS_NAME} =~ /^\w{3}ssp/)  { $proc .= " SPI"; }# SPI spectra

	#	additions
	if ($ENV{PROCESS_NAME} =~ /^n/)         { $proc .= " (NRT)"; }
	if ($ENV{PROCESS_NAME} =~ /^c/)         { $proc .= " (CONS)"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3,4}st$/)  { $proc .= " Start"; }
	if ($ENV{PROCESS_NAME} =~ /^\w{3,4}fin?$/) { $proc .= " Fin"; }

	if ($ENV{PROCESS_NAME} =~ /clean/)      { $proc  = "cleanup";}

	&ISDCLIB::Error (  "cannot parse PROCESS_NAME $ENV{PROCESS_NAME}" ) unless ($proc);	#	060524
#	die "*******     ERROR:  cannot parse PROCESS_NAME $ENV{PROCESS_NAME}" unless ($proc);

	return $proc;
} # end of ProcStep. 

##############################################################################

=item B<FindDirVers> ( $root )

This function takes a root directory and returns the lastest version of that, e.g. for aux/adp/0084.003 or scw/0084/rev.001, etc.  It will error if both "000" and any other version is present.

returns the "largest" or "last" version.

=cut

sub FindDirVers {
	my $root = $_[0];
	my @files;

	print "*******      Trying to find $root*\n";

	@files = glob("$root*");

	return "" unless (scalar(@files));

	print "*******      Found these files:\n";
	foreach my $file (@files) {
		print "*******      $file\n";
	}

	(scalar(@files) > 1)
		and print "*******      Found ".scalar(@files)." different versions;  choosing last, $files[$#files]\n";
#	die ">>>>>>>     ERROR:  not expecting both $files[0] and $files[$#files]!"



#	SPECIAL FOR CONSSSA MOSAICS

#	&ISDCLIB::Error ( "not expecting both $files[0] and $files[$#files]!" )	#	060524
#		if (($files[0] =~ /\.000$/) && ($#files > 0));




	return $files[$#files];
}

##############################################################################

=item B<GetColumn> ( $DOL, $column )

dal_dump wrapper.

returns the first, or an array of all, of the column.

=cut

sub GetColumn {

	my ( $DOL, $column ) = @_;

	my $initialcommonlogfile = $ENV{COMMONLOGFILE};

	#	This'll bite me in the ass sometime when I really want the output after a crash
	$ENV{COMMONLOGFILE} = "/dev/null";	#	don't want the output

	my @result = `dal_dump inDol=\"$DOL\" column=$column outFormat=0`;
	my $retval = $?;

	$ENV{COMMONLOGFILE} = $initialcommonlogfile;

	&Error ( "ERROR examining output; No value found for $column:  \n@result" ) 
		if ( $retval );

	return @result    if     (wantarray);
	return $result[0] unless (wantarray);
}


##############################################################################

=item B<QuickDalAttach> ( $Parent, @Children )

dal_attach wrapper.

=cut

sub QuickDalAttach {
	my ( $Parent, @Children ) = @_;

	&Message ( "About to QuickDalAttach @Children to $Parent" );

#       @Children returns the count, 
#       $#Children returns the greatest subscript (@Children - 1)
#       while ( $#Children >= 0 ) {                             OR
#       while ( @Children > 0) {                                        OR

	while ( @Children ) {
		my $command = "dal_attach";

		#	do all this chomping just in case one of the kids has a \n
		#	doesn't affect anything negatively so ...
		chomp ( $command .= " Parent=$Parent" );
		chomp ( $command .= " Child1=".pop(@Children) );
		chomp ( $command .= " Child2=".pop(@Children) );
		chomp ( $command .= " Child3=".pop(@Children) );
		chomp ( $command .= " Child4=".pop(@Children) );
		chomp ( $command .= " Child5=".pop(@Children) );
		&Message ( "Running $command" );
		`$command`;
	}
	&Message ( "Done with QuickDalAttach." );
}


####################################################################################################

=item B<QuickClean> ( @products )

QuickClean DELETES the files given and their .gz counterparts, if they exist.

=cut

sub QuickClean {
	&Message ( "About to QuickClean @_" );
	foreach my $fn ( @_ ) {
		chomp ( $fn );
		&Message ( "About to remove $fn(.gz)" );
		foreach my $ext ( "", ".gz" ) {
			if  ( -e "${fn}${ext}" ) {
				system ( "$mychmod +w ${fn}${ext}" ) 
					if ( ( -e "${fn}${ext}" ) && ( ! -l "${fn}${ext}" ) );	#	only if its not a link
				&Message ( "Running $myrm ${fn}${ext}" );
				system ( "$myrm ${fn}${ext}" );
			}
			else {
				&Message ( "$fn$ext does not exist." );
			}
		}
	}
	&Message ( "Done with QuickClean." );
}

####################################################################################################

=item B<QuickDalClean> ( @files )

dal_clean wrapper.

=cut

sub QuickDalClean {
	&Message ( "About to QuickDalClean @_" );

	my @list = @_;
#	again, I cannot do a chomp???
#	chomp ( @_ );
#	I get a "Modification of a read-only value attempted at /isdc/integration/isdc_int/SunOS/all_sw/prod/opus/pipeline_lib/ISDCLIB.pm line 476."
	foreach my $grpdol ( @list ) {
		#	I don't know why, but I'm not allowed to chomp here either.
		#chomp ( $file );	#	just in case.

		#	SPR 4423 - Jake - may need to add this as many URLs are relative to where you are running and not where the file is?
		#	Also, for -e test, can't have the [1] or +1 stuff
		# /isdc/integration/isdc_int/sw/dev/prod/opus/nrtrev/unit_test/opus_work/nrtrev/scratch//0025_20021227060629_00_ire/working_rawscws_index.fits[GROUPING]

		my ($root,$path,$ext);
		if ( $grpdol =~ /\[/ ) {
			($root,$path,$ext) = &File::Basename::fileparse( $grpdol, '\[.*');
		} elsif ( $grpdol =~ /\+/ ) {
			($root,$path,$ext) = &File::Basename::fileparse( $grpdol, '\+.*');
		} else {
			#	These are only used to see if the file exists
			$path = &File::Basename::dirname  ( $grpdol );
			$root = &File::Basename::basename ( $grpdol );
			$ext  = "";	#	to avoid "Use of uninitialized value ... "
		}


		if ( ( -e "$path/$root" ) || ( -e "$path/$root.gz" ) ) {
#		if ( -e "$file" ) {





#			my $dirname  = &File::Basename::dirname  ( $file );
#			my $filename = &File::Basename::basename ( $file );
			my $command = "cd $path; " if ( $path );



#	FIX 060306 - Do I REALLY want checkExt=1 here?!?!?!

#	FIX - this leaves a common_log.txt in $path.


			$command .= "dal_clean inDOL=$root$ext checkExt=0 backPtrs=1 checkSum=1 chatty=2";
#			$command .= "dal_clean inDOL=$root$ext checkExt=1 backPtrs=1 checkSum=1 chatty=2";
			&Message ( "Running $command" );
#			my $initialcommonlogfile = $ENV{COMMONLOGFILE};
#			$ENV{COMMONLOGFILE} = "/dev/null";	#	don't want the output
			`$command`;
#			$ENV{COMMONLOGFILE} = $initialcommonlogfile;
		}
	}
	&Message ( "Done with QuickDalClean." );
}

####################################################################################################

=item B<QuickDalCopy> ( $source, $target, @extensions )

dal_copy wrapper.

=cut

sub QuickDalCopy {
	my ( $source, $target, @extensions ) = @_;

	&Message ( "Running QuickDalCopy with source=$source, target=$target, extensions=@extensions" );

	foreach my $extension ( @extensions ) {
		chomp ( $extension ); #	just in case
		&Message ( "Running QuickDalCopy on extension=$extension" );

#	FIX - this is a toughy.  Do I copy empty extensions????

		if ( &ISDCLIB::RowsIn ( "$source", "$extension" ) > -1 ) {		#	copy if the extension exists (even if empty)
			if ( &ISDCLIB::RowsIn ( "$source", "$extension" ) > 0 ) {	#	copy only if there are rows
				my $command = "dal_copy inDol=$source outFile=$target extension=$extension";
				&Message ( "Running $command" );
				`$command`;
			} else {
				&Message ( "Apparently no rows in extension=$extension; not copying" );
			}
		} else {
			&Message ( "Apparently extension=$extension does not exist; not copying" );
		}
	}
	&Message ( "Done with QuickDalCopy." );
}

####################################################################################################

=item B<QuickDalDelete> ( $file, $extension )

dal_delete wrapper.

=cut

sub QuickDalDelete {
	my ( $filename, @extensions ) = @_;

	&Message ( "Running QuickDalDelete on filename=$filename, extensions=@extensions" );

	foreach my $extension ( @extensions ) {
		chomp ( $extension ); #	just in case
		&Message ( "Running QuickDalDelete on extension=$extension" );
		my $command = "dal_delete filename=$filename extension=$extension deleteAll=0 verbosity=3";
		&Message ( "Running $command" );
		`$command`;
	}
	&Message ( "Done with QuickDalDelete." );
}

####################################################################################################

=item B<QuickDalDetach> ( $file, $extension )

dal_detach wrapper.

=cut

sub QuickDalDetach {
	my ( $file, $extension, $delete ) = @_;
	$delete = "no" unless ( $delete );

	&Message ( "Running  QuickDalDetach on file=$file, extension=$extension, delete=$delete." );

	my $command = "dal_detach object=$file pattern=$extension child= delete=$delete recursive=no showonly=no reverse=no",;
	&Message ( "Running $command" );
	`$command`;
	&Message ( "Done with QuickDalDetach." );
}

####################################################################################################

=item B<ParseConfigFile> ( $config_file )

returns the filename list from a given $ISDC_ENV/templates/*cfg file.

=cut

sub ParseConfigFile {
	my ( $config_file ) = @_;

	my @list;
	open TEMPLATEFILE, "$ENV{CFITSIO_INCLUDE_FILES}/$config_file"
		or &ISDLIB::Error ( "Could not open $config_file" ); #	060524
#		or die "*******     ERROR:  Could not open $config_file";
	while (<TEMPLATEFILE>) {
		chomp;
		next if ( !/^\s*file\s+/ );
		#  file spi_raw_oper_tmp GNRL-FILE-GRP 
		s/^\s*file\s+//;
		s/\s+[\w-]+\s*$//;
		push @list, "$_";
	}
	close TEMPLATEFILE;

	return @list;
}

####################################################################################################

=item B<DoOrDie> ( $command )

As it sounds, DoOrDie executes the given command in `'s and returns the result, or it dies.

=cut

sub DoOrDie {
	my ( $command ) = @_;
	&ISDCLIB::dprint ( "$command\n" );
#	&ISDCLIB::Message ( "$command" );
#	`$command`;
#	die "*******    ERROR:  $command failed with $?" if ( $? );
	my @result = `$command`;
	&ISDCLIB::Error ( "$command failed with $?" ) if ( $? );
#	die "*******    ERROR:  $command failed with $?" if ( $? );
	return @result;
}


########################################################################

=item B<inst2in> ( $inst )

returns the lowercase 2 letter abbreviation for the given instrument.

=cut

sub inst2in {
	my ( $inst ) = @_;
	my $in;
	if    ( $inst =~ /isg/i )  { $in = "ii"; }	#	isgri
	elsif ( $inst =~ /pic/i )  { $in = "ip"; }	#	picsit
	elsif ( $inst =~ /ib/i )   { $in = "ib"; }	#	ibis
	elsif ( $inst =~ /o/i )    { $in = "om"; }	#	omc
	elsif ( $inst =~ /sp/i )   { $in = "sp"; }	#	spi
	elsif ( $inst =~ /j.*1/i ) { $in = "j1"; }	#	could be jx, jmx, jemx
	elsif ( $inst =~ /j.*2/i ) { $in = "j2"; }	#	could be jx, jmx, jemx
	else  { &ISDCLIB::Error ( "$inst does not match any expected instrument" ); }	#	060524
#	else  { die "$inst does not match any expected instrument"; }
	return $in;
}

########################################################################

=item B<in2inst> ( $in )

returns the lowercase instrument name for the given 2 letter abbreviation.

=cut

sub in2inst {
	my ( $in ) = @_;
	my $inst;
	if    ( $in =~ /ii/i ) { $inst = "isgri"; }
	elsif ( $in =~ /ip/i ) { $inst = "picsit"; }
	elsif ( $in =~ /ib/i ) { $inst = "ibis"; }
	elsif ( $in =~ /o/i )  { $inst = "omc"; }
	elsif ( $in =~ /s/i )  { $inst = "spi"; }
	elsif ( $in =~ /j1/i ) { $inst = "jmx1"; }
	elsif ( $in =~ /j2/i ) { $inst = "jmx2"; }
	else  { &ISDCLIB::Error ( "$in does not match any expected instrument abbreviation" ); }	#	060524
#	else  { die "$in does not match any expected instrument abbreviation"; }
	return $inst;
}

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
