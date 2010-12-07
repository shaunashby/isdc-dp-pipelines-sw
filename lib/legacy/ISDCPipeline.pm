package ISDCPipeline;

=head1 NAME

ISDCPipeline.pm - Perl Module for ISDC Pipelines 

=head1 SYNOPSIS

use I<ISDCPipeline.pm>;
contains all of the functions and subroutines generic to the Automatic Analysis Infrastructure.  

C<use ISDCPipeline;>

=head1 DESCRIPTION

This module is part of all ISDC pipeline scripts, and its functions and subroutines used therein as appropriate.  They are intended to be as generic as possible, though tailored to the particular environment and needs of the ISDC processing.  

=head1 SUBROUTINES

=over

=cut 

use strict;
use warnings;

use Carp qw(croak);
use File::Basename;
use Cwd;
use File::Copy;

use UnixLIB;
use TimeLIB;
use ISDCLIB;
use OPUSLIB;

#  This is the return value which RunProgram will pass up if a timeout
#   occured on the command run.  It will be interpreted as a system signal,
#   so it has to be < 256, but high enough to be well out of the way of
#   anything the system uses (e.g. max 64 on Linux, according to Jurek),
#   and 255=ff isn't a good idea.   So:
$ISDCPipeline::timeout_return = 254;  

$| = 1;	# disable output buffering

##############################################################################

=item B<PipelineStart> ( %att )

This subroutine is used in the pipeline startup processes only.  It begins each pipeline by initializing the log file, both the real version in the repository and the link in the OPUS_WORK directory, creating the OSF (the OPUS blackboard entry) which will trigger the next
pipeline step.  

It takes a hash as input and uses the following elements:

=over 5

=item B<dataset> -

the Science Window ID or more generally, the OPUS "observation" being processed. 

=item B<type> -

the OPUS type of the observation ["scw"]

=item B<state> -

the OSF state in which to set the observation ["cwwwww"]

=item B<logfile> -

the central link to the log file;  default is the OSF name followed by ".log", since that is what OPUS requires.

=item B<reallogfile> -

the location of the real log file as opposed to the link in the central OPUS log directory 

=item B<pipeline> -

the name of the pipeline used in logging ["NRT ScW"] 

=back 

=cut 

sub PipelineStart {
	croak( "PipelineStart: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	my $retval;
	my $output;
	my $lt;
	my $path;
	my $suffix;
	my $logdir;
	my $quiet;
	
	# Process the input parameters 
	
	($att{dataset}, $path, $suffix) = &File::Basename::fileparse($ENV{EVENT_NAME}, '\..*') unless defined $att{dataset};
	
	$att{type} = "scw" unless defined $att{type};
	$att{state} = $osf_stati{SCW_ST_C} unless defined $att{state};
	$att{dcf} = "000" unless defined $att{dcf};
	# this always has to be .log for OPUS to find it
	$att{logfile} = "$ENV{LOG_FILES}/$att{dataset}.log" unless defined $att{logfile};
	
	if (!defined($att{logextension})) {
		if (($att{type} eq "scw") || ($att{type} eq "inp")) {
			$att{logextension} = "_$att{type}.txt";
			#  Only pipelines where reallogfile can be found automatically
		}
		else {  # ADP
			$att{logextension} = ".txt";
		}
	}
	
	if ($ENV{PATH_FILE_NAME} =~ /scw|inp/) {
		$logdir = &FindScw("$att{dataset}");       
		&ISDCLIB::DoOrDie ( "$mymkdir -p $logdir" ) unless (-d "$logdir");	#	060524
		$att{reallogfile} = $logdir."/$att{dataset}$att{logextension}" 
			unless defined $att{reallogfile};
	} # if scw or inp
	
	&Error ( "no reallogfile defined!" ) unless ($att{reallogfile});	#	060525
	
	# start the process log
	$lt = &TimeLIB::MyTime();
	print "\n----- $lt:  Starting processing of type $att{type} in path "
		."$ENV{PATH_FILE_NAME} for dataset $att{dataset} -----\n";
	
	# start the observation log
	print "*******     Opening $att{reallogfile}\n";
	$ENV{COMMONLOGFILE} = $att{reallogfile};
	$ENV{COMMONSCRIPT} = "1";
	`$myrm -f $att{logfile}`; # remove the old one if it exists
#	`$myrm -f $att{logfile}` unless ( $ENV{PATH_FILE_NAME} =~ /conscor/ ); # remove the old one if it exists
	if (open OBSTRAILER, ">>$att{reallogfile}") {
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		print OBSTRAILER "-----   $lt:  Starting pipeline with:\n";
		print OBSTRAILER "-----   Path file         : $ENV{PATH_FILE_NAME}\n";
		print OBSTRAILER "-----   Input             : $att{dataset}\n";
		print OBSTRAILER "-----   Type              : $att{type}\n";
		print OBSTRAILER "-----   Inst              : $att{dcf}\n" unless ($att{dcf} =~ /000/);
	}
	else {
		&Error ( "cannot open $att{reallogfile} to write" ) unless ($att{state} =~ /x/);	#	060525
		#  In this case, we already know there's a problem, so don't worry if
		#   you can't write to the log;  need just to get the OSF up with x.
		print "*******     ERROR:  cannot open $att{reallogfile} to write\n" if ($att{state} =~ /x/);
		$quiet++;
		
	}
	if (!defined($att{logonly})) {

		my $sleep_time = 0;
		my $max_sleep_time = 80;
		do {
			sleep ( $sleep_time );
			$sleep_time += 30;
			#$sleep_time += 10;

#	050629 - Jake - This will cause a lot of extra info to show up in the opus log
#$ENV{MSG_REPORT_LEVEL} = "MSG_ALL";

			$output = `osf_create -p $ENV{PATH_FILE_NAME} -f $att{dataset} -t $att{type} -s $att{state} -n $att{dcf}`;

#$ENV{MSG_REPORT_LEVEL} = "MSG_INFO";

			$retval = $?;
			######testing
			print  "*******     Command \'osf_create -p $ENV{PATH_FILE_NAME} -f $att{dataset} -t $att{type} "
				."-s $att{state} -n $att{dcf}\'\n*******     ...had this result:\n$output";

			if ($retval) {

				return $retval if ( $sleep_time >= $max_sleep_time );
			}
		} while ( $retval ); 

	}					#	if (!defined($att{logonly})) {
	
	if ($quiet) { return;}
	
	print OBSTRAILER "-----   created Observation Status File\n";
	
	
	#  In these two pipelines, dump the Preproc settings into the log:
	if ( ( $ENV{PATH_FILE_NAME} =~ /rev|input/) && !( $att{dataset} =~ /arc|ilt/)) {
		#  (for Rev files and Input, but not for arc or ilt triggers in rev)
		my $trigger;
		my $pathname = $ENV{PATH_FILE_NAME};
		my @settings;
		$pathname =~ s/\.path//;
		print OBSTRAILER "-----   Preproc settings:\n";
		if (defined($ENV{EVENT_NAME}) && (-e "$ENV{EVENT_NAME}")) {
			print "*******     EVENT_NAME $ENV{EVENT_NAME} found.\n";
			$trigger = $ENV{EVENT_NAME};
		}
		else {
			print "*******     EVENT_NAME $ENV{EVENT_NAME} not found;  looking for dataset $att{dataset}\n";
			if (-e "$ENV{OPUS_WORK}/$pathname/input/$att{dataset}.trigger_processing") {
				$trigger = "$ENV{OPUS_WORK}/$pathname/input/$att{dataset}.trigger_processing";
			}
			elsif (-e "$ENV{OPUS_WORK}/$pathname/input/$att{dataset}.trigger_bad") {
				$trigger = "$ENV{OPUS_WORK}/$pathname/input/$att{dataset}.trigger_bad";
			}
			else {
				&Error ( "cannot find trigger file "
					."$ENV{OPUS_WORK}/$pathname/input/$att{dataset}.trigger_processing or _bad" );	#	060525
			}
			
			
		} # end if not event_name
		
		@settings = &ISDCLIB::DoOrDie ( "$mycat $trigger" );	#	060524
		print "*******     Settings are:  @settings\n";
		print OBSTRAILER @settings;
		
	}  # end if Input or Rev
	
	close OBSTRAILER;
	
	# put a symbolic link in so that the log file is in the right place,
	# AND, opus is happy.
	symlink $att{reallogfile}, $att{logfile};
	
	return ($?,$output);
	
}


##############################################################################

=item B<PipelineFinish> ( %att )

This subroutine is only used by the pipeline finish processes.  It simply prints a message to the log to the effect that the pipeline has finished.  It has the same parameters as PipelineStart with the exception of  reallogfile.   

=cut

sub PipelineFinish {
	croak( "PipelineFinish: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	my $lt;
	
	$att{dataset} = $ENV{OSF_DATASET} unless defined $att{dataset};
	$att{logfile} = "$ENV{LOG_FILES}/$att{dataset}.log" 
	unless defined $att{logfile};
	$att{type} = "$ENV{OSF_DATA_ID}" unless defined $att{type};
	
	$lt = &TimeLIB::MyTime();
	
	print "*******     Opening $att{logfile}\n";
	$ENV{COMMONLOGFILE} = $att{logfile};
	$ENV{COMMONSCRIPT} = "1";
	open OBSTRAILER, ">>$att{logfile}";
	
	print OBSTRAILER 
	"-----   $lt:  Finishing data type $att{type} in path $ENV{PATH_FILE_NAME} for $att{dataset}\n-----\n";
	close OBSTRAILER;
	
	print "$lt:  Finished processing for data type $att{type} in path "
		."$ENV{PATH_FILE_NAME} with dataset $att{dataset} \n\n";
	
}


##############################################################################

=item B<PipelineStep> ( %att )

This is the workhorse of all the pipelines.  This is how every executable and significant command is called.  It sets the PFILES and PATH, finds the directory to run in, checks  run conditions, constructs the command line, calls the command line, handles errors, and logs the results.  It also has alternate functions of copying a series of files, directly implementing an error exit, and logging a non-operational step.   

All parameters from PipelineFinish are used, and in addition the following:  

=over 5

=item B<environment> -

replace ISDC_ENV with another software repository? 

=item B<error> -

message to print when ERROR exit function used 

=item B<stoponerror> -

flag whether or not to halt pipeline on error;   
default is to stop ["1"];  set to 0 if the pipeline should continue. 

=item B<subdir> -

subdirectory to run command from; 

=item B<step> -

the description of the step, beginning with the process name 

=item B<program_name> -

the command to run, usually an executable name or a shell command;  special values NONE, ERROR, CHECK, and COPY: 

=over 5 

If program_name is NONE, one line is simply printed for the step; 

If program_name is ERROR, the error message given is printed to the log  file and the pipeline halts;  

If program_name is CHECK or COPY, the given files are copied from the  run directory to the given new directory, checking first to see if they already exist in the output dir. CHECK is done first, and if overwrite is not 1 or 2, an error will occur if the files already exist.  If overwrite has the value of 2, it will use a forced copy, so will replace even write-protected files (assuming the proper directory permissions.)  This is a careful way to copy many files from working space into the repository.  (Using CHECK first on a set of files means that none will be copied if even one already exists.  Using COPY only will error off on the first it gets to that is already there.)  The file is copied first to <filename>_temp and them moved atomically.  To read files which may be updated (or globbing), always specify the full extension, i.e. use "<path>/<fileroot>*.fits" never "<path>/<fileroot>*" to prevent reading a file before it is completely copied.  

If program_name is none of those special values, it is passed to the shell;   to the command line are added any parameters specified with the "par_" keys (e.g. "par_outfile" => "test.fits" will be turned into  "outfile=test.fits" on the command line.)  The command is called using the RunProgram function, and the results logged;  if a non-zero value is returned, the pipeline exits with status 1 (the general pipeline error trapped by each process)  unless  stoponerror=0.  If it returns,  the exit status and STDOUT are returned in that order to the calling program as an array.

=back 

=item B<overwrite> -

flag whether or not to overwrite repository files when using COPY function;  default "0", set to "1" to overwrite.  "2" to overwrite with force (C<-f>).

=item B<needfiles> -

flag wheter to error off when no files to copy exist when using COPY function;  default "1", set to "0" to allow none 

=item B<newdir> -

directory into which to copy files when using COPY 

=item B<filename> -

a file specifier, parsed by bsh, of files to copy when using COPY function; 

=item B<chkvers> -

give location of directory containing VERSION file for an executable, and this will print that version number to the log 

=back

=cut

sub PipelineStep {
	croak( "PipelineStep: Need even number of args" ) if ( @_ % 2 );
	my %att = @_;
	my @results;
	my $retval;
	
	my $lt;
	######################################################
	##     Set up and check input paramters
	######################################################

	$att{type} = ( defined ( $ENV{OSF_DATA_ID} ) ) ? "$ENV{OSF_DATA_ID}" : "" unless ($att{type});
	$att{dataset}      = $ENV{OSF_DATASET} unless ($att{dataset});
	$att{dcf}          = $ENV{OSF_DCF_NUM} unless defined $att{dcf};
	$att{logextension} = "txt" unless ($att{logextension});
	$att{logfile}      = "$ENV{LOG_FILES}/$att{dataset}.log" unless ($att{logfile});
	$att{step}         = "unknown!" unless ($att{step});
	$att{environment}  = $ENV{ISDC_ENV} unless ($att{environment});
	$att{error}        = "no error" unless ($att{error});
	# for these one only, a defined value of 0 is possible
	$att{overwrite}    = 0 unless defined $att{overwrite};
	$att{stoponerror}  = 1 unless  defined $att{stoponerror};
	$att{needfiles}    = 1 unless  defined $att{needfiles};
	$att{ckvers}       = 0 unless defined $att{ckvers};
	$att{getstdout} = 1 unless (defined $att{getstdout});
	
	print "\n=================================================================\n";
	print &TimeLIB::MyTime(),"     PIPELINESTEP $att{step} for $att{dataset}\n";
	print "=================================================================\n";
	print"*******     stoponerror is $att{stoponerror}\n";
	&Error ( "No program name" ) unless ($att{program_name});
	
	# set the current directory
	
	# to distinguish ScW pipeline from SAscw; this may change:
	if (($att{type} eq "scw") && ($att{dcf} eq "000")) {
		chdir &FindScw("$att{dataset}") or 
		&Error ( "Can't find science window $att{dataset}" );
	}
	
	if ($att{type} eq "inp") {
		chdir &FindScw("$att{dataset}") or 
		&Error ( "Can't find science window $att{dataset}" );
	}
	
	if ($att{subdir}) {
		chdir $att{subdir} or 
		&Error ( "Can't change to subdirectory $att{subdir}" );
	}
	
	&Error ( "PARFILES not set!" ) unless (defined($ENV{PARFILES}));
	$ENV{PFILES} = "$ENV{PARFILES};$att{environment}/pfiles";
	print "========     PFILES is now $ENV{PFILES}\n";

	my $par;
	my $programLine;
	
	$lt = &TimeLIB::MyTime();
	
	######################################################
	##     Start logging
	######################################################
	
	#####################################
	#  NFS problem hunting:
	#####################################
	my $openstatus = 0;
	my $i;
	my $j;
	my $path;
	my @dirs;
	#  We try to open the file;  if we can't, we
	#       1)  ls each directory backward from lowest to highest
	#       2)  'ps -efw' and store result
	#       3)  sleep N seconds
	#       4)  try again.
	#
	#  Sleep 1 second, then 2, then 4, then 8, then 16, then give up with error
	#
	
	open DEBUG, ">>$ENV{OPUS_HOME_DIR}/debug_fileopen.log" 
		or &Error ( "Cannot open $ENV{OPUS_HOME_DIR}/debug_fileopen.log to write;  $!" );	#	060525
	
	for ($i = 1; $i <= 16 ; $i *= 2) { 
		
		if (open OBSTRAILER, ">>$att{logfile}") {
			print "#######  $lt  DEBUG:  file opened succesfully.\n";
			$openstatus = 0;
			last; # break out of for loop
		}
		else {
			#  Print error as number (+0 forces it to that context) then as string:
			
			print "\n\n\n\n"
				."##################################################################\n"
				."##################################################################\n"
				."#######  $lt  DEBUG:  on host $ENV{HOST} and in process $ENV{PROCESS_NAME}, "
				."could not open $att{logfile}:  error was ".($! + 0)."==$!\n";

			print DEBUG "\n\n\n\n"
				."##################################################################\n"
				."##################################################################\n"
				."#######  $lt  DEBUG:  on host $ENV{HOST} and in process $ENV{PROCESS_NAME}, "
				."could not open $att{logfile}:  error was ".($! + 0)."==$!\n";
			$openstatus = $!;  
		}
		
		#  Otherwise, 1-3 above:
		
		#  ls all directories:
		$att{reallogfile} = readlink "$att{logfile}";
		
		@dirs = split('/+',$att{reallogfile});
		#  Note that there will be a "" in front of @dirs, so take it off:
		shift @dirs;
		
		DIRS: while ( pop(@dirs) ) {
			last unless ($dirs[0]);  # don't 'ls -l '
			$path = "";
			foreach (@dirs) { $path .= "/".$_}
			print DEBUG "#######     DEBUG:  running \'$myls -l $path\':\n";
			@results = `$myls -l $path`;
			print DEBUG "#######     DEBUG:  status $?;  results:\n@results\n";
			# stop listing if no error; only useful if ls fails on this dir
			last DIRS unless ($?); 
		}
		
		#  check processes
		print DEBUG "#######      DEBUG:  running \`$myps -ef\':\n";
		@results = `$myps -ef`; 
		print DEBUG "#######     DEBUG:  status $? ;  results:\n@results\n";
		
		#  check load
		print DEBUG "#######      DEBUG:  running \`$myw\' command:\n";
		@results = `$myw`; 
		print DEBUG "#######     DEBUG:  status $? ;  results:\n@results\n";
		
		
		#  then sleep
		print DEBUG "#######     DEBUG:  sleeping $i\n";
		sleep $i unless ($i > 16);  # ugly, but didn't see better way
		
	} # end foreach opening OBSTRAILER
	
	close DEBUG;  
	
	#  If after those loops, we still couldn't open the file, die:
	&Error ( "Cannot open $att{logfile} to write ( $openstatus )." ) if ($openstatus != 0);	#	060525
	#####################################
	#####################################
	
	if (($att{step} =~ /STARTING/) && ($att{program_name} =~ /NONE/)) {
		# WATCH IT:  this is case sensitive;  "starting" is for individual 
		#  instrument sections within DP, while "STARTING" is for DP.  Not ideal.
		#  Needs big, visible logging.
		#
		#  Try to check for previous version;  if so, add RE-:
		print OBSTRAILER "-----\n-----\n";
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		if (`$mygrep \"$att{step}\" $att{logfile}` =~ /STARTING/) {
			$att{step} =~ s/START/RE-START/;
		}
		print OBSTRAILER "-----   $lt:  STEP $att{step} on host $ENV{HOST}\n";
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		print OBSTRAILER "----------------------------------------------------------------------------------\n";
		print OBSTRAILER "-----\n-----\n";
	}
	else { 
		print OBSTRAILER "-----   $lt:  STEP $att{step}\n";
	}
	
	
	if ($att{program_name} =~ /NONE/) { 
		# just for prettier logging, check for keys with "par_";  this means
		#  the step is normally a program but is non-op, rather than just
		#  a logging step.  
		my $nonop;
		foreach (keys(%att)) { $nonop++ if (/par_/);}
		print OBSTRAILER "-----   WARNING:  step is currently non-operative.\n-----\n" if ($nonop);  
		print OBSTRAILER "-----\n";
		close OBSTRAILER;
#		$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
		return;
	}
	else {
		print OBSTRAILER "-----   Current directory is ".&Cwd::cwd()."\n";
	}
	######################################################
	# if a list of structures to check is given, run CheckStructs and
	#  only call executable if it passes.  
	######################################################
	
	if ($att{structures}) {
		
		$ENV{COMMONLOGFILE} = "$ENV{WORKDIR}/tmplog.txt";
		my @structs = split(' ',$att{structures});
		my ($sok, @missing);
		my $group = "swg.fits[GROUPING]" if ($att{type} eq "scw");
		$group = "swg_raw.fits[GROUPING]" if ($att{type} eq "inp");
		($sok,@missing) = CheckStructs("$group",@structs);
		if ($sok != 0) {
			print OBSTRAILER "-----   $lt:  WARNING:  Cannot run step:  missing required data structures ";
			print OBSTRAILER join(' ',@missing), "\n";
			print OBSTRAILER "-----\n";
			close OBSTRAILER;
#			$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
			return;
		}
	}
	
	#  want this after above check because we don't need to see the error stack
	#  from that run;  errors are relatively normal there. 
	delete $ENV{COMMONLOGFILE};
	$ENV{COMMONLOGFILE} = "+" if ($att{getstdout}); # SCREW 1099 requires always using this
	$ENV{COMMONLOGFILE} .= $att{logfile};
	
	$ENV{COMMONSCRIPT} = "1";
	
	# run the program
	######################################################
	##     Check what the command is
	######################################################
	
	if (!($att{program_name} =~ /NONE/)) {
		
		if ($att{program_name} =~ /ERROR/) {
			#######
			##     ERROR log and exit
			#######
			
			print OBSTRAILER "Error   $lt:  $att{error}\n";
			close OBSTRAILER;
			# try to move trigger file 
			&RunProgram("$mymv $ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_processing "
				."$ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_bad") 	
				if (-e "$ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_processing");
			# try to manually update OSF (only for startups which don't know one)
			if ($att{status}) {
				print "========     trying:\nosf_update -p $ENV{PATH_FILE_NAME} -f $ENV{OSF_DATASET} -s $att{status}\n";
				($retval,@results) = &RunProgram("osf_update -p $ENV{PATH_FILE_NAME} -f $att{dataset} -s $att{status}");
				
				print "========     retval was $retval\n";
			}
			print "*******      ERROR:  $att{error}\n";
			exit 1 if ($att{stoponerror});
#			$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
			return 1;
		} # end if ERROR 
		elsif (($att{program_name} =~ /^COPY$/) || 
			($att{program_name} =~ /^CHECK$/)) {
			#######
			##     File COPY and/or CHECK
			#######
			
			# this takes two parameters, $att{filename} and $att{newdir}.
			# this means campers that you MUST copy to a directory, and yes,
			# we're checking.  BTW, we make the directory if it doesn't exist.
			# we error if the file already exists in the output directory.
			# filename, of course, can be a wild card.
			&Error ( "Cannot find files when filename not defined!" ) 	#	060525
				unless ($att{filename});
			# does the output directory exist
			
			&ISDCLIB::DoOrDie ( "$mymkdir -p $att{newdir}" ) unless ( -d $att{newdir} );	#	060524
#	This is the most excessive mkdir-or-die I've ever seen
#			if (!(-e $att{newdir})) {
#				# doesn't exist, make it
#				`$mymkdir -p $att{newdir}`;
#				die "*******     ERROR:  cannot mkdir $att{newdir}." if ($?);
#			}
#			if (!(-e $att{newdir}) || !(-d $att{newdir})) {
#				&ISDCPipeline::PipelineStep(
#					"step"         => "error exit",
#					"error"        => "can not create dir $att{newdir}",
#					"program_name" => "ERROR",
#					"type"         => "$att{type}",
#					"logfile"      => "$att{logfile}",
#					);
#			}
			# directory now exists.
			
			# do the input file(s) exist?
			my @files = glob("$att{filename}");
			my $numFiles = scalar @files;
			if ($numFiles == 0 && $att{needfiles}) {
				&ISDCPipeline::PipelineStep(
					"step"         => "error exit",
					"error"        => "no files match $att{filename}",
					"program_name" => "ERROR",
					"type"         => "$att{type}",
					"logfile"      => "$att{logfile}",
					);
				
			}
			elsif ($numFiles == 0 && !($att{needfiles})) {
				
				print "*******     COPY:  no files matched, needfiles==0, returning.\n";
#				$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
				return;
			}
			# I'm going to do something here which looks a bit more complex
			# than it should have to be.  What I'm going to do is loop over the
			# list of files twice, once to see if they exist in the output 
			# directory and then once to copy.  Why?  That way if any exist
			# I stop without copying any of them rather than copying some of 
			# them.  This should make it easier to restart because one won't have
			# to delete a few files here or there.
			
			my $oneFile;
			my $newFile;
			print "#######     DEBUG:  overwrite is $att{overwrite}.\n";
			if (!$att{overwrite})  {
				foreach $oneFile (@files) {
					my $fname;
					my $fpath;
					my $fext;
					($fname, $fpath, $fext) = 
					&File::Basename::fileparse($oneFile,'\..*');
					$newFile = $att{newdir}."/".$fname.$fext;
					print OBSTRAILER "-----   checking to see if $newFile exists\n";
					if (-e $newFile) {
						&ISDCPipeline::PipelineStep(
							"step"         => "error exit",
							"error"        => "file $newFile exists already - from $att{newdir} and $att{filename}",
							"program_name" => "ERROR",
							"type"         => "$att{type}",
							"logfile"      => "$att{logfile}",
							);
						
					}
				}
			}
			#######
			##      End of CHECKing
			#######
			
			# now, finally, copy them
			if ($att{program_name} =~ /^COPY$/) {
				foreach $oneFile (@files) {
					my $fname;
					my $fpath;
					my $fext;
					($fname, $fpath, $fext) = &File::Basename::fileparse($oneFile,'\..*');
					$newFile = $att{newdir}."/".$fname.$fext;
					&ISDCLIB::DoOrDie ( "$mymkdir -p $att{newdir}" ) unless ( -d $att{newdir} );	#	060524
					
					#	SPR 3284
					if (!-r $oneFile) {
						print OBSTRAILER "-----   Copying $oneFile: input file does not exist\n";
					} else {
						print OBSTRAILER "-----   Copying $oneFile to $newFile\n";
						
						if ( -l $oneFile ) {
							print OBSTRAILER "----   File $oneFile is a link\n";
							my $onefileinfo = `$myls -l $oneFile`;
							print OBSTRAILER "----   File info : $onefileinfo";
							my $realfilename = `$myls -l $oneFile | $myawk '{print \$NF}'`;
							if ( -e $realfilename ) {
								my $realfiledata = `$myls -l $fpath/$realfilename 2> /dev/null`;
								print OBSTRAILER "----   $fpath/$realfiledata\n"; 
							} else {
								print OBSTRAILER "----   Appears to be a dead link.\n";
								print OBSTRAILER "----   $fpath/$realfilename does not exist\n";
							}
						}
						
						# first copy it to its destination but to temporary name;  
						#  takes time, and can't be reading at the same time;  
						#  then, do a mv to final name, essentially instantaneous
						($retval,@results) = &RunProgram("$mycp $oneFile ${newFile}_temp");
						&ISDCPipeline::PipelineStep(
							"step"         => "error exit",
							"error"        => "copying file $oneFile to ${newFile}_temp:  @results",
							"program_name" => "ERROR",
							"type"         => "$att{type}",
							"logfile"      => "$att{logfile}",
							) if ($retval);
						if ($att{overwrite} != 2) {
							#  Check permissions first.  Looks like mv doesn't care if the
							#   target already exists and is write protected.  It silently
							#   clobbers it even if -f isn't specified. 
							if ( (-e "$newFile") && (!-w "$newFile")) {
								&ISDCPipeline::PipelineStep(
									"step"         => "error exit",
									"error"        => "$newFile already exists and is write protected, "
										."and you didn't specify overwrite=2 to force the mv",
									"program_name" => "ERROR",
									"type"         => "$att{type}",
									"logfile"      => "$att{logfile}",
									);
							}
							
							($retval,@results) = &RunProgram("$mymv ${newFile}_temp $newFile");
							&ISDCPipeline::PipelineStep(
								"step"         => "error exit",
								"error"        => "moving ${newFile}_temp $newFile:  @results",
								"program_name" => "ERROR",
								"type"         => "$att{type}",
								"logfile"      => "$att{logfile}",
								) if ($retval);
							
						}
						#  ... but if you really mean it (overwrite == 2), use "mv -f"
						#  (even though I'm not sure it adds anything to the usual
						#  behavior.)
						else {
							($retval,@results) = &RunProgram("$mymv -f ${newFile}_temp $newFile");
							&ISDCPipeline::PipelineStep(
								"step"         => "error exit",
								"error"        => "error copying file $oneFile to $newFile:  @results",
								"program_name" => "ERROR",
								"type"         => "$att{type}",
								"logfile"      => "$att{logfile}",
								) if ($retval);		
						} # force overwrite
					} # else of if( !-r file )
				} # foreach file
			} # if COPY
		} # if CHECK
		
		#######
		##      End of COPY/CHECKing
		#######
		
		else {  # not CHECK, 
			######################################################
			##     Running a  real command line
			######################################################
			
			$programLine = $att{program_name}." ";
			#  Tack on parameters, if any:
			foreach $par (sort keys %att) {
				if ($par =~ s/^par_(.*)$/$1/) {
					if ( ($par =~ /^mode$/) && ($att{"mode"} !~ /^h$/) && ($att{program_name} !~ /^spi/) ) {
						print "-------     WARNING:  changing default mode $att{par_mode} to 'h'\n";
						$att{par_mode} = "h";
					}
					$programLine = "$programLine $par=\"".$att{"par_".$par}."\"";
				}
			}
			
			# run it
			print OBSTRAILER "-----   Running:  $att{program_name}\n";
			# check version number, if requested
			if ($att{ckvers}) {
				#my $ckvers = `$att{program_name} --v`;
				# Better to check the VERSION file;  developers might not change code
				my $vers = `$mycat $att{ckvers}/VERSION`;
				chomp $vers;
				print "========     Program $att{program_name} version:  $vers\n";
				print OBSTRAILER "-----   Program $att{program_name} version:  $vers\n";
			}
			
			close OBSTRAILER;
			#      PipelineEnvVars();
			
			#  Get some timing info:
			my $tstart = time;
			
			################
			#  RUN IT:
			($retval,@results) = &RunProgram("$programLine");
			################    
			
			my $tstop = time;
			my $tdiff = $tstop - $tstart;
			#my $load = `/usr/bin/w -u`;
			my $load = `$myw -u | $myhead -1`;		#	050412 - Jake - SCREW 1704

			chomp $load;
			$load =~ s/^.*load\saverage:\s(.*)$/$1/;
			print "*******     STATISTICS for $att{program_name} on $ENV{OSF_DATASET}:  "
				."execution time $tdiff elapsed seconds;  load averages $load.\n";
			
			#############################################################
			# TESTING of temporary junk left behind: 
			#	if ((glob("temporary*") or glob("*subset*")) and ($att{program_name} !~ /\s/)) {
			#	  my $temp = join('_',glob("temporary*"));
			#	  $temp .= join('_',glob("*subset*"));
			#	  system("touch NOTE_$att{program_name}_left_$temp");
			#	}
			#	if (-e "swg_osm.fits") {
			#          my ($sok,@missing) = CheckStructs("swg_osm.fits[1]","PRIMARY");
			#         if ($sok == 0) {
			#           print "*******     ERROR:  found PRIMARY array attached!  Command was $att{program_name} .\n";
			#           exit 1;
			#         }
			#      }
			#############################################################
			open OBSTRAILER, ">> $att{logfile}";
			print OBSTRAILER "-----   Command was:  $programLine\n";
			#  SCREW 1099 requires always getting STDOUT to parse it.  So can no longer
			#   trap this stuff separately.  No big deal.
			#      print OBSTRAILER "-----   results are \n@results\n" if ((@results) && !($att{getstdout}));
			print OBSTRAILER "-----   Execution time:  $tdiff seconds (approximate real "
				."elapsed time) for $att{program_name}\n";
			
			if ($retval == 0) {
				print OBSTRAILER "-----   Retval is 0\n";
			}
			else {
				
				
				######################################################################
				##  Figure out where the error comes from.
				##
				##   See cammel book, chap 3, p. 230, definition of system call 
				##   (Programming Perl, Wall et al., O'Reilly, 1996)
				##   But that's both more and less than we need here.  
				##   
				##  In summary, three cases:  
				##   - "normal" CommonExit 0xff00
				##   - "abnormal" executable or shell error, 0x??00
				##   - Unix signal exit, 0x00??  
				##  (since either the lower XOR upper 8 bits are set)
				
				
				$retval &= 0xffff;  
				printf "*******     return value was %#04x\n",$retval;
				
				#####################
				#  Fudge for ISDCRoot analysis scripts which return error in CommonFinish;  this
				#   tries to get the return status from the log even if the value returned wasn't
				#   0xff00;  it just means you don't get the hint that it core dumped in that case.
				#####################
				#	if ($retval == 0xff00) {

				#	040528 - Jake - SPR 3650
				#	scripts will not return a ($retval == 0xff00) and therefore must be
				#	dealt with another way, hence the $ENV{PATH_FILE_NAME} usage
				#	because of j_correction and ibis_correction possible OK errors, 
				#	nrtscw and consscw need to be added to this list
				#	To be quite frank, I don't know why this check is here at all
				#	and just do the ignored_errors.cfg check.
				if ( ($retval == 0xff00) || ($ENV{PATH_FILE_NAME} =~ /nrtqla|conssa|consssa|nrtscw|consscw/) ) {		#	040528 - Jake - SPR 3650
					################################################
					##  This is what you get if you do an exit(-1) in Perl or C,
					##   which in fact what CommonExit does instead of being able
					##   to really return the status printed.  (Comment in common.c
					##   says "exit only keeps the lower 256 bits" as explanation
					##   (which I think means really 8 bits, as 256 is big!).)  
					##
					##  I.e. this is our normal error exit.  
					print "*******     ERROR:  Status of $retval from $programLine\n";
					print OBSTRAILER "-----   Retval is -1 from the executable.\n" if ($retval == 0xff00);
					print OBSTRAILER "-----   Retval is $retval from the executable.\n" unless ($retval == 0xff00);
					
					#  find trigger file and move to _bad (if possible) on error
					#  (workdir is certain to be defined, though INPUT is not, so use
					#  it to try to find the trigger file)
					&RunProgram("$mymv $ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_processing "
						." $ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_bad",1
						) if ((-e "$ENV{WORKDIR}/../input/$ENV{OSF_DATASET}.trigger_processing") && ($att{stoponerror})); 
					
					if ($att{getstdout}) {
						#  Now look at the COMMONLOG output to see what it really is:
						my @tmp = split('status -',$results[$#results]);
						my $exit_status;
						# (split returns one array entry of original string if no match)
						if ($#tmp > 0) {
							$exit_status = $tmp[$#tmp];
							chomp $exit_status;
							print ">>>>>>>     Real exit status from CommonExit was $exit_status\n";
							print OBSTRAILER ">>>>>>>     Real exit status from CommonExit was $exit_status\n";
							$retval = $exit_status; # for those cases where we return it.
						}
						else {
							print ">>>>>>>     WARNING:  cannot parse last line:\n$results[$#results]\n";
							print ">>>>>>>     WARNING:  cannot determine real exit status, so must skip checking for ignored errors.\n";
							# 040824 - Jake - SCREW 1533
							print OBSTRAILER ">>>>>>>     WARNING:  cannot parse last line:\n$results[$#results]\n"; 
							print OBSTRAILER ">>>>>>>     WARNING:  cannot determine real exit status, so must skip checking for ignored errors.\n";
						}
						my @all;
						my @ignore_errors;
						my $match = 0;
						print ">>>>>>>     WARNING:  cannot find ignored_errors.cfg in $ENV{OPUS_HOME_DIR};  skipping check.\n" 
							unless (-e "$ENV{OPUS_HOME_DIR}/ignored_errors.cfg");
						print OBSTRAILER ">>>>>>>     WARNING:  cannot find ignored_errors.cfg in $ENV{OPUS_HOME_DIR};  skipping check.\n" 
							unless (-e "$ENV{OPUS_HOME_DIR}/ignored_errors.cfg"); # 040824 - Jake - SCREW 1533
						#####################################################################
						#  SCREW 1099:  check a configuration file for datasets and expected
						#   errors.  
						#
						#  TO BE FIXED:  where should this live?
						#
						if ( ($exit_status) && (-e "$ENV{OPUS_HOME_DIR}/ignored_errors.cfg") ) {
							#  Load it into memory so we can use Perl grep.  (sed removes comment lines)
							@all = `$mysed /^#/d $ENV{OPUS_HOME_DIR}/ignored_errors.cfg`;
							#  See if there are any entries for this dataset and program
							#  (TO BE FIXED:  someday, may allow that to be general, in which
							#   case will have to change this.)
							@ignore_errors = grep s/^\s*$ENV{OSF_DATASET}\s+$att{program_name}\s+(\d+).*$/$1/ , @all;
							
							# there may be multiple possibilities, so check all (though only
							#  expect one match
							foreach (@ignore_errors) {
								$match++ if ($_ == $exit_status);
							}
							print ">>>>>>>     No error file entries for $ENV{OSF_DATASET}.\n" unless (@ignore_errors);
							print OBSTRAILER ">>>>>>>     No error file entries for $ENV{OSF_DATASET}.\n" unless (@ignore_errors); 
							
						} # end if ignored_errors.cfg exists
						
						if ($match) {
							print ">>>>>>>     WARNING:  found ignored_errors.cfg entry for dataset $ENV{OSF_DATASET}, program"
								." $att{program_name}, and error $exit_status;  resetting return value to 0 and continuing.\n";
							print OBSTRAILER ">>>>>>>     WARNING:  found ignored_errors.cfg entry for dataset $ENV{OSF_DATASET}, program"
								." $att{program_name}, and error $exit_status;  resetting return value to 0 and continuing.\n"; 
							print OBSTRAILER "-----   Ignoring error $exit_status from $att{program_name}\n";
							$retval = 0;
							# defined error exit in most of our OPUS processes
						}
						else {
							print ">>>>>>>     Found no ignored errors entry for dataset $ENV{OSF_DATASET}, "
								."program $att{program_name}, and error $exit_status.\n";
							print OBSTRAILER ">>>>>>>     Found no ignored errors entry for dataset $ENV{OSF_DATASET}, "
								."program $att{program_name}, and error $exit_status.\n"; # 040824 - Jake - SCREW 1533
							exit 1 if ($att{stoponerror});  # otherwise, fall through
						}
						
					} # If getstdout defined
					else {
						print ">>>>>>>     WARNING:  getstdout==0, so can't check real exit status against ignored_errors.cfg.\n";
						print OBSTRAILER ">>>>>>>     WARNING:  getstdout==0, so can't check real exit status against ignored_errors.cfg.\n"; 
						exit 1 if ($att{stoponerror});  # otherwise, fall through	    
					}
					
				}  # end if normal error
				
				elsif ($retval > 0xff) {
					################################################
					##  i.e. something else in the 8 high order bits.  
					##
					##  This is what you get if the executable exits without
					##   using CommonExit, or if there was a shell error 
					##   like command not found, permission denied, etc.  
					
					$retval >>= 8;  # shift off lover 8 bits (zeros)
					
					print "*******     ERROR:  Status of $retval from $programLine (not CommonExit!)\n";
					print OBSTRAILER "-----   Retval is $retval from the command (not CommonExit!)\n";
					exit 1 if ($att{stoponerror});  # this is an XPOLL_ERROR for most ISDC processes
					
				}
				else {
					################################################
					##  If you get here, that means something in the lower 8 bits of
					##  the exit status was set, i.e. it was killed by the system,  
					##  e.g. somebody killed it, bus errors, etc.  It's not clear
					##  that our system will ever get here;  I think perl or shell ends
					##  up changing this into a system value above.  But in case...
					##	  
					print "*******     ERROR:  ";
					if ($retval == $ISDCPipeline::timeout_return) {
						print "pipeline-defined timeout after $ENV{TIMEOUT} seconds.";
						print OBSTRAILER "-----   Retval indicates a TIMEOUT!\n";
						close OBSTRAILER;
						exit 1;
					} #  If got a timeout
					if ($retval & 0x80) {
						#  this is supposed to spot a core dump, but doesn't
						#   seem to work on our system.  
						
						#  And this, I don't want to do;  it sets the retval back
						#   to zero.  Why do they do this?  
						#$retval &= ~0x80;
						print "coredump from ";
					} # if coredump?  
					print "signal $retval\n";
					print OBSTRAILER "-----   Retval is $retval from the system.\n"; 
					exit 1 if ($att{stoponerror});  #  so this is an XPOLL_ERROR for most ISDC processes
					
				}  # end if any wierd signal errors
				
			} # end if not status 0
			
			if (($retval) && !($att{stoponerror})) {
				print "*******     WARNING:  exit value non-zero, but continuing.\n";
				
			}
		} # if not CHECK
		
		print OBSTRAILER "-----\n";
		close OBSTRAILER;
		print "======================= end PIPELINESTEP ========================\n";
		#    Don't do this anymore;  it's now set to real CommonExit value 
		#      (if there was one)
		#    $retval = 1 if ($retval); # whatever it is, reset it to 1 before returning.
#		$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
		return ($retval,@results);
		
	}  # end if not NONE
#	$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
}



##############################################################################

=item B<FindScw> ( $scwid )

This function uses the SCWDIR environment variable (usually set through the path and resource files) and the given scwid (the only parameter) and searches the repository for that science window, as it may have an unknown version number.  It will error if it finds more than one.

=cut

sub FindScw {
	
	my $scwid = "@_";
	
	my @files;
	my $file;
	my $rev = $scwid;
	$rev =~ s/^(\d{4}).*/$1/;
	#  print "Trying to find $ENV{SCWDIR}/$rev/$scwid*\n";
	
	@files = glob("$ENV{SCWDIR}/$rev/$scwid*");
	
	#  print "Found these files\n";
	#  foreach $file (@files) {
	#    print "-- $file\n";
	#  }
	# if there is more than one then we have to die, for now
	(scalar(@files) > 1) and &Error ( "Too many science window ids @files" );	#	060525
	&Error ( "No science window $scwid found!" ) unless (@files);	#	060525
	return $files[0];
}



##############################################################################

=item B<FindPrev> ( $swg )

This uses the GetAttirubute function and the given science window group (the only parameter) and returns the value of the PREVSWID keyword.

=cut

sub FindPrev {
	#  get previous science window keyword
	
	my ($swg) = @_;
	my ($retval,$prevswid) = &GetAttribute("$swg","PREVSWID","DAL_CHAR");
	if ($retval) {
		# no previous science window
		print "*******      ERROR:  Could not check PREVSWID keyword:  $prevswid\n";
		exit 1;
	} 
	else {
		
		$prevswid =~ s:(\d{12}):\.\.\/$1\.000\/swg_raw\.fits\[1\]:;
		print "*******      PREVSWID from $swg is $prevswid\n";
	}
	
	return $prevswid;
	
}


##############################################################################

=item B<RunProgram> ( $command, $quiet )

This is the basic function to pass a command to a shell and parse the result.  It takes a command an an optional second parameter which, if defined, tells it not to print the return value automatically.  The command is passed to the shell and the output and errors trapped.  The function returns the scalar exit status and the array of all output. 

=cut

sub RunProgram {
	
#	my $initialcommonlogfile = $ENV{COMMONLOGFILE};		#	040408 - Jake - SCREW 1437

	#  Default of 1 hour timeout (seconds).
	$ENV{TIMEOUT} = 3600 unless (defined $ENV{TIMEOUT});
	print "#######    DEBUG:   TIMEOUT of $ENV{TIMEOUT} seconds will be used\n";
	
	# This runs the command given as a parameter, traps the output
	# returns the output or "ERROR FOR $$" if there is an error.
	# The error messages are written to STDERR unless the first parameter
	# ($command) begins with a '-' sign.
	my $command = $_[0];
	my $quiet = $_[1];
#	my $quiet = "";					#	040408 - Jake - SCREW 1437 - $_[1];
#	my $screen = "";					#	040408 - Jake - SCREW 1437
#	my @pars = @_;						#	040408 - Jake - SCREW 1437
#	if ( $#pars > 0 ) {				#  040408 - Jake - SCREW 1437
#		for ( my $n=1; $n <= $#pars; $n++ ) {
#			$quiet  = $pars[$n] if ( $pars[$n] =~ /quiet/ );	#	prints a few extra lines
##			$screen = $pars[$n] if ( $pars[$n] =~ /screen/ );	#	makes sure that a + precedes COMMONLOGFILE 
##	$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} unless ( ( $ENV{COMMONLOGFILE} =~ /^\+/ ) and !( $screen ) );    #  040408 - Jake - SCREW 1437
#		}
#	}

	my $ERRFIL;
	my @result;
	my $retval;
	my $pid;
	my @pids;
	my $lowest;
	my $indentcount;
	
	my $basecommand = $command;
	$basecommand =~ s/^(\S+)\s*.*/$1/;
	
	if ($ENV{PARFILES}) {
		$ENV{PFILES} = $ENV{PARFILES}.";".$ENV{ISDC_ENV}."/pfiles";    
		$ENV{PFILES} .= ":"."$ENV{LHEASOFT}/syspfiles" if ($ENV{LHEASOFT});
		print "*******     PFILES in RunProgram is now:  $ENV{PFILES}\n";
	}
	

	print "*****\n" unless ($quiet);
	print "*****         RUNNING:  $command\n";
	print "*****\n" unless ($quiet);
	
	
	#  if($command =~ s/^-// ) {
	#    my $suppress = 1;
	#  }
	
	##  
	##   In order to get proper retval for distinguishing system, signal, and
	##    executable errors, I can't use the redirect.  This means the STDERR
	##    goes to STDOUT and cannot be tacked onto the result.  In practice,
	##    this means that the pipelines will have this error only in the
	##    process log instead of in the obs log, annoying but necessary if
	##    we want to distinguish errors and how OPUS handles them.   
	##
	#  $ERRFIL = "/tmp/error$$";
	#  unlink $ERRFIL;
	#  $command .= " 2> $ERRFIL";
	
	#######
	#
	#   Timeout handling:  set an alarm to go off after TIMEOUT seconds;  if
	#     the command finishes successfully before that alarm hits, the alarm
	#     is then removed.  If the alarm hapens, that means the command took
	#     more than TIMEOUT seconds and is probably hung, in which case we
	#     kill it.  See page 595 of Perl Cookbook:
	#
	#######
	
	#  This variable tells what to do when an alarm occurs.  
	#  This die only happens within the eval, i.e. it kills the code within
	#     the eval, but note that it doesn't kill the process spawned in the ``.
	$SIG{ALRM} = sub { die "timeout" };

	eval {
		#  Set the alarm to happen after TIMEOUT seconds
		alarm($ENV{TIMEOUT});
		
		#  Start the command
		@result = `$command`;
		$retval = $?;
		
		#  Clear the alarm.  (You don't get here if the timeout alarm was
		#   triggered above.)    
		alarm(0);
		
	};
	
	if ($@) {
		#  You get here only if an error came out of the eval block;  it could
		#  be a syntax error (since eval blocks are only compiled when run), 
		#  or a timeout alarm or perhaps something unexpected.
		if ($@ =~ /timeout/) {
			#  That means the ALRM was rung.
			print "*******     RunProgram - program running longer than timeout "
				."($ENV{TIMEOUT}s) and must be killed\n\n";
			
			chomp ( my $OS = `$myuname` );	
			#  Want to kill the process (still running) such that it will leave 
			#   a core file, which will make debugging easier.  Get the PID
			#   of the lowest level child of this script ($$) because otherwise,
			#   we'll just kill something top level and the core file is useless.
			#
			if ( $OS =~ /SunOS/i ) {
				print "*******     Looking for children with command:  \'$myptree $$ \'\n";
				@pids = `$myptree $$ `; 
				print "#######     DEBUG:  $myptree finds:\n".join('',@pids)."\n";
				#  Find lowest;  expect to see
				#  WWWW ssh something
				#    XXXX -tcsh
				#      YYYY perlscript
				#        NNNN  top level executable, maybe what we want
				#          MMMM   or maybe it calls lower level script swith ISDCTask
				#      ZZZZ and maybe another child to be avoided
				#	14756 /isdc/sfw/sbin/sshd
				#	  20201 /isdc/sfw/sbin/sshd
				#	    20206 -tcsh
				#	      27132 perl -x test6.pl
				#	        27134 sleep 15			<----- Kill this one!
				#	        27135 ptree 27132		<----- Not this one.
				#
				#  Going to have to assume that N or M is what we want, i.e. first 
				#    end we reach.  So:
				foreach (@pids) {
					chomp;
					print "#######     DEBUG:  looking at entry:  $_\n";
					$_ =~ /^(\s*)(\S+)\s+/;
					#  Basically, count indented spaces.  Set indentcount to new number
					#   of indented spaces if new greater than old number.  If not,
					#   then that means we've gone out to a different child, probably
					#   not the one we want.  
					#
					#  Watch out for first, which has no indentation at all
					if ( !($1) || ($1 gt $indentcount) ) {
						print "#######     DEBUG:  Indentation greater than previous.  Continuing.\n" if ($1);
						print "#######     DEBUG:  No indentation, so must be top.  Continuing.\n" unless ($1);
						$indentcount = $1;
						$pid = $2;
					}
					else { 
						print  "#######     DEBUG:  Indentation less than previous.  Stopping at PID $pid\n";
						last;
					}	
				}
			}
			elsif ( $OS =~ /Linux/i ) {
				#	pstree only gives from process given to the end.
				#	pstree -p 29792
				#	ibis_correction(29792)---sh(29806)---ibis_comp_evts_(29807)
				#	pstree -np $$
				#	perl(19983)-+-sleep(19985)		<------kill this one!
				#	            `-pstree(19986)	<------Not this one.
				print "*******     Looking for children with command:  \'$mypstree $$ \'\n";
				@pids = `$mypstree -np $$`; 
				print "#######     DEBUG:  $mypstree finds:\n".join('',@pids)."\n";
				foreach ( @pids ) {
					chomp;
					print "#######     DEBUG:  looking at entry:  $_\n";
					next if /pstree/;
					( $pid ) = ( /\((\d+)\)$/ );
					last if ( $pid );
				}
			}
			else {
				&Error ( "OS ->$OS<- is not Linux or SunOS!.  I'm confused." );
			}

			if ($#pids < 0) {
				print "*******     Didn't get anything (which is wierd), so it may have to be killed by hand.\n";
			}
			
			else {
				#  If you found it, send ABRT, which tells it to dump core
				print "*******     Attempting to kill (ABRT) child process PID==$pid\n";
				if (kill 'ABRT',$pid) {
					print "*******     Succeded in killing it.\n";
				}
				else {
					print "*******     Couldn't kill it, so maual cleanup may be necessary\n";
				}
			}
			#  Return special value which PipelineStep at least will read.
			#  (Is 3 going to come from anywhere else?  What to use?!)
#			$ENV{COMMONLOGFILE} = $initialcommonlogfile;		#	040408 - Jake - SCREW 1437
			return ($ISDCPipeline::timeout_return,"program timed out after $ENV{TIMEOUT} seconds.");
			
		}
		else {
			#  If we get here, then we got some unexpected error from within
			#   the eval.  It doesn't mean a "normal" error in the command
			#   executed but something wierd.  
			
			alarm(0);   # clean the still-pending alarm
			
			&Error ( "RunProgram - failure within eval block:  $@" );	#	060525
		}
	} # end if ($@)
	
	print "********     RunProgram - retval is $retval\n\n" unless (defined($quiet));
		
	return ($retval,@result);
}




##############################################################################

=item B<PipelineEnvVars> ( )

This function simply prints all environment variables.  It is useful for debugging.

=cut

sub PipelineEnvVars {
	
	my $key;
	print "\n*************************************************************\n";
	print "              ENVIRONMENT VARIABLES:\n\n";
	foreach $key (sort keys %ENV) {
		print $key, '=', $ENV{$key},"\n";
	}
	print "************ end ENVIRONMENT VARS ****************************\n";
	
}




##############################################################################

=item B<RevNo> ( $scwid )

This simply parses a science window ID and returns the rev number.

=cut

sub RevNo {
	
	my $scwid = $_[0];
	
	$scwid =~ /([0-9]{4}).*/;
	my $rev = $1;
	print "*******      Rev no for scwid $scwid is $rev\n";
	return $rev;
}

##############################################################################

=item B<SeqNo> ( $scwid )

This simply parses a science window ID and returns the sequence number.

=cut

sub SeqNo {
	
	my $scwid = $_[0];
	
	$scwid =~ /([0-9]{4})([0-9]{4}).*/;
	my $seq = $2;
	print "*******      Seq no for scwid $scwid is $seq\n";
	return $seq;
}





##############################################################################

=item B<GetAttribute> ( $object,$attribute,$type )

This function takes a DOL and a keyword/attibute and uses dal_attr to return the exit status and the value of the keyword. 

=cut

sub GetAttribute {
	# takes the DOL of whatever you want to open,
	#   the attribute name (i.e. keyword), 
	#   returns array of status, value, unit, and comment for attribute
	#
	my ($object,$attribute,$type) = @_;
	my @temp;
	$type = "DAL_CHAR" unless ($type);
	my $oldlog;
	print "\n****************************************************************************\n";
	print "            GETTING attribute $attribute from $object\n\n";
	print "****************************************************************************\n";
#	Change from just COMMONSCRIPT check to include COMMONLGFILE check cause cssfin during REDO_CO..
#	if ( $ENV{COMMONSCRIPT} ) {
	if ( ( $ENV{COMMONSCRIPT} ) || ( $ENV{COMMONLOGFILE} ) ) {
		$oldlog = $ENV{COMMONLOGFILE};
		delete $ENV{COMMONSCRIPT};
		delete $ENV{COMMONLOGFILE};
	}
	my ($retval,@result) = &RunProgram(
		"dal_attr indol=\"$object\" keynam=$attribute action=READ type=$type value_i= "
			."value_r= value_b=yes value_s= value_cr= value_ci= unit= comment="
		);
	$ENV{COMMONSCRIPT} = 1 if ($oldlog);
	$ENV{COMMONLOGFILE} = $oldlog if ($oldlog);
	print "*********** end GET attribute *********************************************\n";
	if ($retval) {
		return ($retval,@result);
	}
	else{
		foreach (@result) {
			chomp;
			next unless (/Log_0.*\s$attribute\s=\s(.*)\s+\//);
			return ($retval,"$1") if (defined($1));
		}
		print "*******      WARNING:  attribute $attribute blank in $object\n";
		return ($retval,"");
	}
}



##############################################################################

=item B<PutAttribute> ($object,$attribute,$value,$type,$comment,$unit)

Note that unlike GetAttribute, this runs PipelineStep so it's visible. 

=cut

sub PutAttribute {
	# takes the DOL of whatever you want to open,
	#   the attribute name (i.e. keyword), 
	#   the attribute value
	#   the attribute type
	#   the comment
	#
	my ($object,$attribute,$value,$type,$comment,$unit) = @_;
	my @temp;
	$type = "DAL_CHAR" unless ($type);
	$comment = "Attribute set by pipeline" unless ($comment);
	my $oldlog;
	my ($value_i,$value_r,$value_b,$value_s);
	
	$value_i = $value if ($type =~ "DAL_INT");
	$value_r = $value if ($type =~ "DAL_DOUBLE");
	$value_b = $value if ($type =~ "DAL_BOOL");
	$value_s = $value if ($type =~ "DAL_CHAR");
	&Error ( "I don't know how to do type $type yet, sorry!" ) if ($type =~ "DAL_DBLCMPLX");	#	060525
	
	print "\n****************************************************************************\n";
	print "            SETTING attribute $attribute in $object\n\n";
	print "****************************************************************************\n";
	
	&ISDCPipeline::PipelineStep(
		"step"         => &ProcStep()." - set $attribute attribute",
		"program_name" => "dal_attr",
		"par_indol"    => "$object",
		"par_keynam"   => "$attribute",
		"par_action"   => "WRITE",
		"par_type"     => "$type",
		"par_value_i"  => "$value_i",
		"par_value_r"  => "$value_r",
		"par_value_b"  => "$value_b",
		"par_value_s"  => "$value_s",
		"par_value_cr" => "0.0",
		"par_value_ci" => "0.0",
		"par_unit"     => "$unit",
		"par_comment"  => "$comment",
		);
	
}  # end sub PutAttribute





##############################################################################

=item B<RootPutAttribute> ($object,$attribute,$type,$value,$unit,$comment)

=cut

sub RootPutAttribute {
	# takes the DOL of whatever you want to open,
	#   the attribute name (i.e. keyword), 
	#   the DAL data type of the attribuge, e.g. DAL_CHAR
	#   the value you want to give the attribute,
	#   the unit of the attribute,
	#   and the comment for the attribute.
	#   each of type, value, unit, and comment may be left empty.
	#
	# Must have ROOTSYS defined before calling this function
	#   returns array of status
	my ($object,$attribute,$type,$value,$unit,$comment) = @_;
	my @result;
	delete $ENV{DISPLAY};
	
	open(TMPC,">/tmp/root$ENV{TIME_STAMP}") 
		or &Error ( "Cannot open /tmp/root$ENV{TIME_STAMP}to write:  $!" );	#	060525
	
	print TMPC <<"	EOF";
	{
		int stat;
		char *attribute = "$attribute";
		char *value = "$value";
		char *unit = "$unit";
		char *comment = "$comment";
		dal_dataType type = $type;
		dal_element *a;
		a = NULL;
		stat = DALobjectOpen("$object",&a,stat);
		stat = DALattributePut(a,attribute,type,value,unit,comment,stat);
		stat = DALobjectClose(a,DAL_SAVE,stat);
		gISDCrootReturnStatus = stat;
	}
	EOF
	#	It must be alone on its line.
	#	This EOF must match the previous EOF.
	#	If it has a tab in the beginning, it MUST have a tab here.
	
	close(TMPC);
	
	#  @result = &RunProgram("ISDCroot -b -q -l /tmp/root$ENV{TIME_STAMP}");
	
	# because root is hard wired to return status 0 when -q is used, we must call
	#  it in this contorted way if we want any error checking
	@result = &RunProgram("echo \".q\" | isdcroot -b -l /tmp/root$ENV{TIME_STAMP}");
	
	unlink "/tmp/root$ENV{TIME_STAMP}";
	
	return @result;
}




##############################################################################

=item B<MoveLog> ($oldlog,$newlog,$link)

Given an old logfile, new logfile, and a link name, this function moves the old logfile to the new logfile and changes the link to point to the new location.

=cut

sub MoveLog {
	# moves an OPUS observation log file, both the real one and the link
	my ($oldlog,$newlog,$link) = @_;
	my @results;
	print "*******     mving $oldlog to $newlog\n";
	@results = &RunProgram("$mymv -f $oldlog $newlog");
	&Error ( "result was: \n@results" ) if ($results[0]);	#	060525
	print "*******     rming $link\n";
	@results = &RunProgram("$myrm -f $link");
	&Error ( "result was: \n@results" ) if ($results[0]);	#	060525
	print "*******     symlinking $newlog to $link\n";
	symlink $newlog, $link or &Error ( "cannot symlink $newlog to $link" );	#	060525
	
	return;
}


##############################################################################

=item B<CheckStructs> ($group,@list)

This takes a group and a list of structures (space separated string) and checks using B<dal_list> whether each structure is in the group.  It returns a status (0 if all are attached, N if N are missing) and the list of those missing.

=cut

sub CheckStructs {
	# for each structure in the input list (space separated data structures), 
	# call find_struct and parse result to determine if a structure is 
	# attached to a group.  
	# 
	# Return status 0 only if all structures in list are found
	
	my ($group,@list) = @_;
	my $ok = 0;
	my $structure;
	my @result;
	my @missing;
	my $oldscript;
	my $oldlog;
	$oldscript = $ENV{COMMONSCRIPT} if ($ENV{COMMONSCRIPT});
	$oldlog = $ENV{COMMONLOGFILE} if ($ENV{COMMONLOGFILE});
	delete $ENV{COMMONLOGFILE};
	delete $ENV{COMMONSCRIPT};
	print "\n***********************************************************************\n";
	print "             CHECKING STRUCTURES\n";
	print "***********************************************************************\n";
	foreach $structure (@list) {
		my ($retval,@output) = &RunProgram(
			"dal_list dol=$group extname=\"$structure*\" exact=yes longlisting=no fulldols=no","quiet"
			);
		if ($retval) {
			print "*******      ERROR:  Cannot verify data structures:  @output\n";
			exit 1;
		}
		push(@missing,$structure) unless (@output);
		$ok++ unless (@output);
	}
	$ENV{COMMONSCRIPT} = $oldscript if ($oldscript);
	$ENV{COMMONLOGFILE} = $oldlog if ($oldlog);
	print "************ end CHECKING STRUCTS *************************************\n";  
	return ($ok,@missing);
}

##############################################################################

=item B<EnvStretch> ( @vars )

This routine takes a variable name which is defined in a resource file and determines the value using the OPUS tool B<osfile_stretch_file>.  The variable is changed from the literal value in the path file (e.g. "OPUS_WORK/nrtscw/input") to the actual value (e.g. "/nrt/scratch/trj_opustest1/nrtscw/input").  

=cut

sub EnvStretch {
	
	my @vars = @_;
	my $var;
	my $retval;
	my @output;
	my @strsplit;
	
	print "\n****************************************************************************\n";
	print "                   STRETCHING ENVIRONMENT\n@vars\n";
	print "****************************************************************************\n";
	foreach $var (@vars) {
		&Error ( "$var not defined!" ) unless (defined($ENV{$var}));	#	060525
		if ($ENV{$var} =~ /:/) {
			@strsplit = split(':',$ENV{$var});
			$ENV{$var} = $ENV{$strsplit[0]};
			$ENV{$var} .= "/".$strsplit[1] if ($strsplit[1]);
			print "********      environment variable $var is now $ENV{$var}\n";
		}
		else {
			print "********      WARNING:  don't know how to stretch $var with value $ENV{$var}\n";
		}
		
	}
	print "***************** end STRETCHING *****************************************\n";
	return;
	
}




##############################################################################

=item B<MakeIndex> ( %att )

This is a complicated subroutine for updating index files in one smart step using B<idx_add>.  PipelineStep is used to log and handle errors properly.  It can create the index if it does not exits, take an explicit list of files or a file match pattern, and handle all (?) of the possible error cases.

It takes a hash as input with the following parameters:

=over 5

=item B<root> -

the root filename of the index (all except ".fits")

=item B<subdir> -

the subdirectory to run from

=item B<add> -

to add to an index or create a new one;  if add is set to 1 and the template exits, it is updated;  otherwise, it is created anew.  

=item B<template> -

template file to use in creation;  only used if index does not already exist or you want to replace it.

=item B<type> -

OSF type for getting the logging right.

=item B<osfname> -

OSF name for getting the loggin right.

=item B<filematch> -

shell filematch to use in a Perl glob.

=item B<files> -

list of files

=item B<filedir> -

directory to search;  if not specified, file list or file match must have path, or files must be in subdir specified.  

=item B<ext> -

extension of structure to be indexed; if not specified, must already be in list of given files.  

=item B<security> -

the security to call idx_add with.  Default value is 1, which means index will be checked that it is still correct;  othewise, its contents will not be verified.   

=item B<protect> -

By default ("protect"=>1), the files to be indexed are write protected first.  Set to 0 to skip this.  

=back

=cut

sub MakeIndex {
	#  create index of input file types, in output directory specified, with 
	#   given root name.  Creates using dal_create and <root>_new.fits, 
	#   adds each structure found using glob on the input file types and idx_add,
	#   and lastly does a mv from <root>_new to <root>.fits
	#
	# give "root" "subdir"  "osfname" "type"  "ext" ("filematch" or "files") 
	#  ["add"] ["template"] ["filedir"] ["security"] ["clean"] ["sort"]
	#  ["sortType"] ["sortOrder"] ["protect"] ["collect"]
	#
	# where filematch is an expression to glob, files is a space separated list
	#  and filedir is prepended if given
	#
	# ext is the extension of the structure to be added;  if left off,
	#  make sure a list of DOL's is given (not tested)
	#
	# add not zero (1 default) means add to existing index rather than recreate
	#
	# security, sortType, and sortOrder passed to idx_add (see help);
	#  defaults respectively 0, 1, and 1;  
	#
	# sort is a column name; can be undefined, and no sorting performed
	#
	# clean = N means all but N last index versions removed, but 0 cleans none.
	# 
	# collect = yes means use idx_merge instead of idx_add;  must give..?
	#
	# protect = 1 by default write protects the file(s) to be added first.
	#
	# NOTE:  it is almost always best to use relative paths, at least when
	#  creating any index which is not used and deleted immediately. 
	#
	
	croak( "MakeIndex: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	#my ($root,$idxtpl,$subdir,$filedir,$filematch,$osfname,$type,$add,$extn) = @_;
	my $status;
	my @result;
	
	$att{security}  = "0"   unless ( defined ( $att{security}  ) );
	$att{sortType}  = "1"   unless ( defined ( $att{sortType}  ) );
	$att{sortOrder} = "1"   unless ( defined ( $att{sortOrder} ) );
	$att{sort}      = ""    unless ( defined ( $att{sort}      ) );
	$att{subdir}    = "./"  unless ( defined ( $att{subdir}    ) );
	$att{osfname}   = $ENV{OSF_DATASET} unless ( defined ( $att{osfname} ) );
	$att{type}      = $ENV{OSF_DATA_ID} unless ( defined ( $att{type}    ) );
	$att{clean}     = 2     unless ( defined ( $att{clean}   ) );
	$att{add}       = 1     unless ( defined ( $att{add}     ) );
	$att{protect}   = 1     unless ( defined ( $att{protect} ) );
	$att{stamp}     = 0     unless ( defined ( $att{stamp}   ) );
	$att{ext}       = "[1]" unless ( defined ( $att{ext}     ) );
	$att{collect}   = "no"  unless ( defined ( $att{collect} ) );
	
	print "\n********************************************************************************\n";
	print "             MAKING INDEX $att{root}\n";
	print "********************************************************************************\n";
	print "******     root file name is $att{root}\n";
	print "******     using idx_merge instead of idx_add\n" if ($att{collect} =~ /y|Y/);

	#	051206 - Jake - SPR 4394 - added trailing fits
	my @others = sort(glob("$att{subdir}/$att{root}*fits"));
	my $vers;
	my $index_old = "";
	if (@others) {
		#   print "******     Previous indices are:\n",join("\n",@others),"\n\n";
		$index_old = "$others[$#others]";
		print "******     old index file is $index_old\n";   
	}
	$vers = &TimeLIB::MyTime();
	$vers =~ s/(-|:|T)//g;
	my $index_new = "new_$att{root}_$vers.fits";
	print "******     new version will be $index_new\n";
	
	&ISDCLIB::DoOrDie ( "$mymkdir -p $att{subdir}" ) unless ( -d $att{subdir} );	#	060524
	chdir("$att{subdir}") or &Error ( "Cannot chdir to $att{subdir}" );	#	060525
	print "******     directory is now $att{subdir}\n";
	unlink("$index_new") if (-e "$index_new");
	
	#	FIX
	#	051206 - Jake - What happens here if $index_old is ""?

	#  if add and index exists, copy it to _new;
	if (($att{add}) && (-e "$index_old")) {
		($status,@result) = &RunProgram("$mycp $index_old $index_new");
		&Error ( "Cannot cp $index_old $index_new: @result" ) if ($status);	#	060525
		($status,@result) = &RunProgram("$mychmod +w $index_new");
		&Error ( "Cannot chmod $index_new: @result" ) if ($status);	#	060525
		# must set template to empty string if file exists
		$index_new .= "[GROUPING]";
		$att{template} = "";
	}
	elsif (!(defined($att{template}))) {
		#  otherwise, must have template given
		&Error ( "Cannot create index $att{root}_new.fits without template" );	#	060525
	}
	my @files;
	if ($att{filematch}) {
		print "Looking for $att{filematch}\n";
		@files = sort(glob("$att{filematch}"));
	}
	elsif ($att{files}) {
		@files = split('\s+',$att{files});
	}
	else {
		&Error ( "No files given to MakeIndex" );	#	060525
	}
	my $one;
	
	if ( (!$att{stamp}) && ($att{template})) {
		#  Since it must be stamped on creation, we force stamping if this
		#   is the first index.  
		$att{stamp}++;
	}
	
	foreach $one (@files) {
		$one = "$att{filedir}/$one" if ($att{filedir});
		print "******     Adding file $one\n";
		# remove any instances of multiple "//" in paths
		$one =~ s:\/+:\/:g;
		
		# write protect if necessary
		&PipelineStep (
			"step"         => &ProcStep()." - write protect file",
			"program_name" => "$mychmod -w $one",
			"subdir"       => "$att{subdir}",
			"dataset"      => "$att{osfname}",
			"type"         => "$att{type}",
			) if (($att{protect}) && (-w "$one"));
		
		&PipelineStep (
			"step"          => &ProcStep()." - idx_add to index $index_new",
			"program_name"  => "idx_add",
			"par_element"   => "$one"."$att{ext}",
			"par_index"     => "$index_new",
			"par_template"  => "$att{template}",
			"par_stamp"     => "$att{stamp}",
			"par_update"    => "1",
			"par_sortType"  => "$att{sortType}",
			"par_sortOrder" => "$att{sortOrder}",
			"par_sort"      => "$att{sort}",
			"subdir"        => "$att{subdir}",
			"dataset"       => "$att{osfname}",
			"type"          => "$att{type}",
			"par_security"  => "$att{security}",
			) unless ($att{collect}=~ /y|Y/);
		
		&PipelineStep (
			"step"          => &ProcStep()." - idx_merge to index $index_new",
			"program_name"  => "idx_merge",
			"par_index"     => "$index_new",
			"par_template"  => "$att{template}",
			"par_element"   => "$one"."$att{ext}",
			"par_sort"      => "$att{sort}",
			"par_sortType"  => "$att{sortType}",
			"par_sortOrder" => "$att{sortOrder}",
			"par_stamp"     => "$att{stamp}",
			"par_checkDupl" => "1",			#	050214 - Jake
			"subdir"        => "$att{subdir}",
			"dataset"       => "$att{osfname}",
			"type"          => "$att{type}",
			) if ($att{collect}=~ /y|Y/);
	}

	$index_new =~ s/\[GROUPING\]//g;
	my $index = $index_new;
	$index =~ s/new_//;
	&PipelineStep (
		"step"         => &ProcStep()." - update $index",
		"program_name" => "$mymv $index_new $index",
		"subdir"       => "$att{subdir}",
		"dataset"      => "$att{osfname}",
		"type"         => "$att{type}",
		);
	print "******     update of $index successfull\n";
	
	if ($att{clean}) {
		print "******     cleaning up all but $att{clean} old versions\n";
		my $i;
		#  Clean all others up to last minus <# to leave> 
		for ($i=0; $i <= $#others - $att{clean}; $i++) {
			#  Don't delete the one without a stamp
			next if ($others[$i] =~ /$att{root}\.fits$/);
			print "******     removing $others[$i]\n";
			unlink ("$others[$i]");
		}
	}
	print "*************** end MAKING INDEX ***************************************\n";
	return;   
} # end of MakeIndex



##############################################################################

=item B<IndexDetach> ( %att )

This function does the revers of MakeIndex.  Given an input child and index, the child is detached, with all the versioning and linking of MakeIndex.  

Detach a child from an index safely (copies, links, etc. as in MakeIndex).  Since used in cleanup script, none of the standard pipeline formatting, logging, etc.  

give "root", "subdir", "child" or "pattern", "delete"[yes,no], "collect"[y,n] (n default)

=cut

sub IndexDetach {
	croak( "IndexDetach: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	$att{collect} = "n" unless (defined($att{collect}));
	$att{pattern} = "" unless (defined($att{pattern}));
	$att{delete} = "no" unless (defined($att{delete}));
	$att{clean} = 2 unless (defined($att{clean}));
	my $status;
	my @result;
	my $file;
	print ">>>>>>>     Detaching child $att{child} from Index $att{root}\n";
	
	#	051206 - Jake - SPR 4394 - added trailing fits
	my @others = sort(glob("$att{subdir}/$att{root}_*fits"));
	my $vers;
	my $index_old = "";
	my $continue = 1;
	if (@others) {
		#   print "******     Previous indices are:\n",join("\n",@others),"\n\n";
		$index_old = "$others[$#others]";
		print "******     old index file is $index_old\n";
	}
	else {
		print "*******     No indices found.  Returning.\n";
		return;
	}
	$vers = &TimeLIB::MyTime();
	$vers =~ s/(-|:|T)//g;
	my $index_new = "new_$att{root}_$vers.fits";
	my $index = "$att{root}_$vers.fits";
	print "******     new version will be $index_new\n";
	
	chdir("$att{subdir}") or &Error ( "Cannot chdir to $att{subdir}" );	#	060525
	
	print "******     directory is now $att{subdir}\n";
	unlink("$index_new") if (-e "$index_new");
	
	($status,@result) = &RunProgram("$mycp $index_old $index_new",1);
	&Error ( "Cannot update $index_new: @result" ) if ($status);	#	060525
	($status,@result) = &RunProgram("$mychmod +w $index_new",1) unless (-w "$index_new");
	&Error ( "Cannot chmod $index_new: @result" ) if ($status);	#	060525
	
	if ($att{delete} =~ "yes") {
		#  If it's legal to delete the child, then just changing the permissions
		#   here is all we need. 
		$file = $att{child};
		$file =~ s/(.*)\[.*\]$/$1/;
		($status,@result) = &RunProgram("$mychmod +w $file",1) unless (-w "$file");
		&Error ( "Cannot chmod $file: @result" ) if ($status);	#	060525
	}
	
	if ($att{collect} =~ /y/) {
		print "*******     Running in \'collect\' mode, i.e. looping over all children and detaching.\n";    
	}
	
	while ($continue) {
		
		#  For collect=y, change child to filename[extn,N] and increment
		#   N until error 1301, DAL_BAD_HDU_NUM.  
		#  Note that even with delete=y, the child file and structures aren't
		#   ever touched, apparently.  I thought they should be, though...?  
		if ($att{collect} =~ /y/) {
			$att{child} =~ /^(.*)\[(.*)\]$/ if ($continue == 1);
			$att{child} =~ /^(.*)\[(.*),\d\]$/ if ($continue > 1);
			$att{child} = "${1}[${2},$continue]";
			$continue++; # for the next time.
		}
		else {
			#  So it doesn't do it again for normal cases:
			$continue = 0;
		}
		
		($status,@result) = &RunProgram(
			"dal_detach object=$index_new"."[GROUPING] child=$att{child} pattern=$att{pattern} "
				."delete=$att{delete} recursive=no showonly=no reverse=no",1
			);
		
		print @result;
		if ($status) {
			chomp $result[$#result];
			my @strsplit = split('-',$result[$#result]);
			my $warning;
			#  If you get a DAL_BAD_HDU_NUM and you're on more than one child,
			#   this is your signal to stop.  (Note:  even if you're doing the
			#   first of N, continue is already 2 because it's incremented first.)
			if ( ( $strsplit[$#strsplit] =~ /1301/ ) && ($continue > 1) ) {
				$continue = 0;
				print "*******     Status 1301 hit;  finished looping.\n";
			}
			else {
				&Error ( "cannot detach $att{child}:\n@result" );	#	060525
			}
		} # if $status
		if (@result) {
			print "*******     Child was detached:  @result\n";
		}
		else {
			#  We assume that if you're looping and one of the children isn't
			#   attached, none are.  Otherwise, you must have a mess and I can't
			#   help you here.  
			print "*******     Nothing was done;  removing new index.\n";
			unlink "$index_new";
			return;
		}
	} # end while ($continue)
	
	
	($status,@result) = &RunProgram("$mymv $index_new $index");
	&Error ( "cannot update index:\n@result" ) if ($status);	#	060525
	
	&ISDCPipeline::LinkUpdate(
		"root"   => "$att{root}",
		"subdir" => "$att{subdir}",
		);
	
	if ($att{clean}) {
		print "******     cleaning up all but $att{clean} old versions\n";
		my $i;
		# this is old versions;  always one more we just made, so +1
		#  (e.g. if clean=1, meaning leave only one, this removes all others)
		for ($i=0; $i <= $#others - $att{clean} + 1; $i++) {
			#  Don't delete the one without a stamp
			next if ($others[$i] =~ /$att{root}\.fits$/);
			print "******     removing $others[$i]\n";
			unlink ("$others[$i]");
		}
	}
	
	return;   
} # end of IndexDetach



##############################################################################

=item B<CleanGroup> ( %att )

Takes an input root and just runs dal_clean, copying and linking as usual.

=cut

sub CleanGroup {
	croak( "CleanGroup: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	$att{descend} = 0 unless ($att{descend});
	$att{subdir} = "." unless ($att{subdir});
	$att{backPtrs} = 0 unless ($att{backPtrs});
	$att{checkSum} = 1 unless (defined $att{checkSum});
	#  Chatty 2 means only list removed entries.
	$att{chatty} = 2 unless ($att{chatty});
	#  Number of indices to leave:
	$att{clean} = 2 unless (defined $att{clean});
	
	my $status;
	my @result;
	my $file;
	print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
	print ">>>>>>>     Cleaning Index $att{root}\n";
	
	#	051206 - Jake - SPR 4394
	my @others = sort(glob("$att{subdir}/$att{root}_*fits"));
	my $vers;
	my $index_old = "";
	my $continue = 1;
	if (@others) {
		#   print "******     Previous indices are:\n",join("\n",@others),"\n\n";
		$index_old = "$others[$#others]";
		print "******     old index file is $index_old\n";
	}
	else {
		print "*******     No indices found.  Returning.\n";
		print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
		return;
	}
	$vers = &TimeLIB::MyTime();
	$vers =~ s/(-|:|T)//g;
	my $index_new = "new_$att{root}_$vers.fits";
	my $index = "$att{root}_$vers.fits";
	print "******     new version will be $index_new\n";
	
	if ($att{subdir} !~ /^\.$/) {
		chdir("$att{subdir}") or &Error ( "Cannot chdir to $att{subdir}" );	#	060525
		print "******     directory is now $att{subdir}\n";
	}
	
	unlink("$index_new") if (-e "$index_new");
	
	($status,@result) = &RunProgram("$mycp $index_old $index_new",1);
	&Error ( "Cannot update $index_new: @result" ) if ($status);	#	060525
	($status,@result) = &RunProgram("$mychmod +w $index_new",1) unless (-w "$index_new");
	&Error ( "Cannot chmod $index_new: @result" ) if ($status);	#	060525
	
	
	($status,@result) = &RunProgram(
		"dal_clean inDOL=$index_new"."[1] checkExt=$att{descend} backPtrs=$att{backPtrs} "
			."checkSum=$att{checkSum} chatty=$att{chatty}"
		);
	
	&Error ( "cannot clean $index_new:\n@result" ) if ($status);	#	060525
	
	
	($status,@result) = &RunProgram("$mymv $index_new $index");
	&Error ( "cannot update index:\n@result" ) if ($status);	#	060525
	
	&ISDCPipeline::LinkUpdate(
		"root"   => "$att{root}",
		"subdir" => "$att{subdir}",
		);
	
	if ($att{clean}) {
		print "******     cleaning up all but $att{clean} old versions\n";
		my $i;
		# this is old versions;  always one more we just made, so +1
		#  (e.g. if clean=1, meaning leave only one, this removes all others)
		for ($i=0; $i <= $#others - $att{clean} + 1; $i++) {
			#  Don't delete the one without a stamp
			next if ($others[$i] =~ /$att{root}\.fits$/);
			print "******     removing $others[$i]\n";
			unlink ("$others[$i]");
		}
	}
	
	print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
	return;   
} # end of CleanGroup





##############################################################################

=item B<LinkReplace> ( %att )

With the same inputs as LinkUpdate, this function looks at the list of files matching the root, removes all but the link and the last version, and moves that last version to replace the link.  (It uses a mv -f to replace the link, which is two atomic system operations and not completely safe.)  

this function takes a root string and a subdirectory and cleans up all files with that root, replacing the link with the last version (assuming all files are <root>_VV..VV.fits and the link is <root>.fits

=cut

sub LinkReplace {
	my %att = @_;
	my $root = $att{root};
	$att{ext} = ".fits" unless ($att{ext});
	print "**************************************************************\n";
	print "              REPLACING LINK ${root}$att{ext} in $att{subdir}\n";
	my $retval;
	my @result;
	my $i;
	chdir $att{subdir} or &Error ( "cannot chdir to $att{subdir}" );	#	060525

	my @files = sort(glob("${root}*$att{ext}"));

	print "********    files are:\n",join("\n",@files),"\n";
	&Message ( "********    files are:\n",join("\n",@files),"\n" );

	if ( @files >= 2 ) {
		#	if there are more than 2 files
		#	remove all but the most recent actual files
		#	and move the newest to the link name
		for ($i = 1; $i < $#files; $i++) {
			print "********     removing file $files[$i]\n";
			unlink $files[$i];
		}

		#	$files[0] is the link name                ( ie. isgri_prp_cal_index.fits )
		#	$files[$#files] is the newest actual file ( ie. isgri_prp_cal_index_20050810142936.fits )
		#	the link doesn't need removed, my just overwrites it correctly.
		($retval,@result) = &RunProgram("$mymv -f $files[$#files] $files[0]");
	} # if there are two or more files

	#	050719 - Jake - SPR 4271
	#	minor adjustments here

	&Error ( "cannot update link:\n@result" ) if ($retval);	#	060525
	
	print "************ end REPLACING LINK ****************************\n";
	
	return;
}




##############################################################################

=item B<LinkUpdate> ( %att )

This function takes a root file name and a subdirectory and makes a link with just the root to the last version found.  I.e. files of <root>*.fits are searched, and a link <root>.fits is set to point to the last.

It takes a hash as input with the following parameters:

=over 5

=item B<root> -

the root filename of the index (all except "_VVVVVVV.fits") 

=item B<subdir> -

the subdirectory to run from 

=item B<ext> -

the extension, ".fits" by default 

=item B<type> -

the OSF type, OSF_DATA_ID by default

=item B<logfile> -

the logfile, $LOG_FILES/$dataset.log by default 

=back  

update a link of given root-name to last version of same root (all files in the same place) give root, ext, subdir

=cut

sub LinkUpdate {
	my %att = @_;
	my $root = $att{root};
	$att{ext} = ".fits" unless ($att{ext});
	print "***********************************************************************\n";
	print "              UPDATING LINK ${root}$att{ext} in $att{subdir}\n";
	
	my $retval;
	my @result;
	chdir $att{subdir} or &Error ( "cannot chdir to $att{subdir}" );	#	060525
	my @files = sort(glob("${root}_*$att{ext}"));
	if (@files) {
		#    print "********    files are:\n",join("\n",@files),"\n";
		print "********    symlinking $files[$#files] ${root}$att{ext}\n";
		($retval,@result) = &RunProgram("$myrm -f ${root}$att{ext} ; "
			."$myln -s $files[$#files] ${root}$att{ext}");
		&Error ( "cannot update link:\n@result" ) if ($retval);	#	060525
	}
	else {
		print "********    nothing matches $root;  quitting\n";
	}
	print "********** end UPDATING LINK ***************************************\n";
	return;
}



##############################################################################

=item B<GetICFile> ( %att )

=over 5

This function takes a requested data structure and attempts to find the correct IC file using the master table, the index, and ic2dol.  It optionally takes a direct path to the files and globs whatever it finds there.  Either way, it returns the last match (default) or an array of the matching files.  

Parameters are:  

=over 5 

=item B<structure> -

mandatory;  the IC structure, e.g. IBIS-ALRT-LIM 

=item B<alias> -

optional alias;  IC_ALIAS env var used as default

=item B<master> -

optional master table; 
REP_BASE_PROD/idx/ic/ic_master_table.fits used as default

=item B<error> - Error if nothing found?  Default is 1 (error). 

=back

Additional parameters of B<ic2dol>.

=back

This function takes an IC file data structure name and searches for it in the master table using default (but changable) values for the master table location, the alias name (default IC_ALIAS env var) 

default:  GetICFile("structure"=>"IBIS-ALRT-LIM");
or:       GetICFile("structure"=>"IBIS-ALRT-LIM","alias"=>"test");
or:       GetICFile("structure"=>"IBIS-ALRT-LIM","select"=>"VERSION==1.0");
or:  GetICFile("structure"=>"IBIS-ALRT-LIM","numLog"=> 1);
or:  GetICFile("structure"=>"IBIS-ALRT-LIM","filematch"=> "swg_osm.fits[1]");
where filematch takes the TSTART from the file and uses it to construct
a selection expression.  If filematch and select, the expressions
will be ANDed.  

NOTE:  Default behavior is to return only one matching file;  if "numLog" specified (0 means all), an array of <numLog> last is returned.  Note that ic2dol takes them off the *top*, i.e. if numLog=1, it returns the *first* file, not the last.  so think about reverse sorting it.  Default is sort="VSTART VSTOP",numLog=1, sortType=0, so it reverse sorts them by VSTART and thus returns the most *recent* file.  

This is necessary because of IC files which all have VSTOP which are

optional "sort", sortType, numLog, outFormat, txtFile 

NOTE:  default "error"=>1 will error if nothing found;  set to 0 and it will return with nothing.  

=cut

sub GetICFile {
	
	my %att = @_;
	my $retval;
	my @files;
	my $file;
	my $oldcommonlog;
	my $tstart;
	
	&Error ( "GetICFile must have data structure name at minimum!" ) 	#	060525
		unless ($att{structure});
	
	$att{alias} = $ENV{IC_ALIAS} unless ($att{alias});
	$att{master} = "$ENV{REP_BASE_PROD}/idx/ic/ic_master_file.fits" 
		unless ($att{master});
	$att{index} = "$ENV{REP_BASE_PROD}/idx/ic/$att{structure}-IDX.fits" 
		unless (defined($att{index}));
	$att{numLog} = 1 unless (defined($att{numLog})); # default returns one
	$att{outFormat} = 0 unless ($att{outFormat}); # DOL to STDOUT
	$att{txtFile} = "" unless ($att{txtFile});
	$att{select} = "" unless ($att{select});
	$att{sort} = "VSTART VSTOP" unless ($att{sort});
	#  Sort in reverse order, since we usually set numLog=1, which tells ic2dol
	#   to only return the *first*.  We want the latest, so do reverse sort.
	#   (SPR 2655)
	$att{sortType} = "0" unless (defined($att{sortType}));
	$att{error} = 1 unless (defined($att{error}));
	$att{filematch} = "" unless ($att{filematch});
	$att{keymatch} = "TSTART" unless ($att{keymatch});
	$att{sortOrder} = "1" unless (defined($att{sortOrder}));
	
	print "***********************************************************************\n";
	print "*******     Finding IC structure $att{structure} using alias $att{alias} and master table $att{master}\n";
	print "***********************************************************************\n";
	
	if ($att{filematch}) {
		print "*******     Getting $att{keymatch} keyword from $att{filematch}\n";
		($retval,$tstart) = GetAttribute("$att{filematch}","$att{keymatch}","DAL_DOUBLE");
		&Error ( "cannot get $att{keymatch} keyword from $att{filematch}:\n$tstart" ) 	#	060525
			if ($retval);
		print "*******     $att{keymatch} of $att{filematch} is $tstart\n";
		if ($att{select}) {
			$att{select} = "($att{select}) && ($tstart > VSTART) && ($tstart < VSTOP)";
		}
		else {
			$att{select} = "($tstart > VSTART) && ($tstart < VSTOP)";
		}
		print "*******     Expr will be:  $att{select}\n";
	} # end of if filematch
	
	print "*******     Looking for index $att{index}\n";
	
	if ((-e "$att{master}") && (-e "$att{index}")) {
		print "*******     master table and index found\n";
		# call ic2dol
		$att{master} .= "[GROUPING]";
		$att{index} .= "[GROUPING]";
		
		($retval,@files) = &ISDCPipeline::PipelineStep(
			"step"          => &ProcStep()." - find IC structure $att{structure}",
			"stoponerror"   => "$att{error}",
			"program_name"  => "ic2dol",
			"par_index"     => "$att{index}",
			"par_select"    => "$att{select}",
			"par_aliasRef"  => "$att{alias}",
			"par_icConfig"  => "$att{master}",
			"par_sort"      => "$att{sort}",
			"par_sortType"  => "$att{sortType}",
			"par_sortOrder" => "$att{sortOrder}",
			"par_numLog"    => "$att{numLog}",
			"par_outFormat" => "$att{outFormat}",
			"par_txtFile"   => "$att{txtFile}",
			"par_extname"   => "$att{structure}",
			"getstdout"     => 0,
			);
		
		#    $ENV{COMMONLOGFILE} = $oldcommonlog;
		if ( (@files) && !($retval)) {
			print "*******     Found files:\n",join("\n",@files),"\n";
			print "************** end Finding IC struct *********************************\n";
			chomp $files[$#files];
			return @files unless ($att{numLog});
			#      splice(<array>,<offset>,<length>)
			return splice(@files,($#files - $att{numLog} + 1),$att{numLog}) if ($att{numLog});
		}
		else {
			print "*******     ERROR:  a run-time error has occured\n" if ($retval);
			print "*******     WARNING:  no files matched selection.\n" unless (@files);
			if (!($att{error})) {
				print "*******      WARNING:  returning with nothing.\n";
				print "************* end Finding IC struct ************************************\n";
				#	040820 - Jake - SCREW 1533
				&Message ( "WARNING:  no structure $att{structure} found, but continuing." );
				return;
			}
			else {
				&Error ( "Unknown problem.  No matching IC file found?  This was an empty 'die' that I replaced with &Error" );
			}
		} # if no files found
	} # if master and index 
	else {
		print "*******     either master table or index not found:\n$att{master}\n$att{index}\n";
		&Error ( "cannot find master index $att{master}" ) unless (-e "$att{master}");	#	060525
		&Error ( "cannot find index $att{index}." ) unless (-e "$att{index}");	#	060525
	}
	
	#  this shouldn't be reached, but who knows.
	#	050629 - Jake - we actually got here.  Probably because Dave was messing with the IC tree at the time
	#
	#	*******     STATISTICS for dal_attach on 031700670020:  execution time 0 elapsed seconds;  load averages 0.09, 0.25, 0.26.
	#	======================= end PIPELINESTEP ========================
	#	***********************************************************************
	#	*******     Finding IC structure INTL-CONV-MOD using alias CONS and master table /isdc/cons/ops_1/idx/ic/ic_master_file.fits
	#	***********************************************************************
	#	*******     Looking for index /isdc/cons/ops_1/idx/ic/INTL-CONV-MOD-IDX.fits
	#	*******     either master table or index not found:
	#	/isdc/cons/ops_1/idx/ic/ic_master_file.fits
	#	/isdc/cons/ops_1/idx/ic/INTL-CONV-MOD-IDX.fits
	#	*******     GetICFile got to a place where it shouldn't have!
	#	2005179153437-E-MISSING XPOLL - could not locate exit status mapping for XPOLL_STATE.151 in process resource file. (1)
	#	2005179153437-E-SEVERE An exception of type No_entry has occurred.
	#	XPOLL_STATE.151 (1)
	#
	&Error ( "GetICFile got to a place where it shouldn't have!" );	#	060525
}  # end of GetICFile



##############################################################################

=item B<GetICIndex> ( %att )

Very similar to GetICFile but uses B<ic_find> executable instead of B<ic2dol>.  Result is an index of matching files instead of an ASCII list.   

This function takes an IC file data structure name and searches for it in the master table using default (but changable) values for the master table location, the alias name (default IC_ALIAS env var) 

Unlike GetICFile, it returns an index

default: GetICIndex("structure"=>"IBIS-ALRT-LIM","subIndex"=>"index.fits");
or:      GetICIndex("structure"=>"IBIS-ALRT-LIM","alias"=>"test");
or:      GetICIndex("structure"=>"IBIS-ALRT-LIM","select"=>"VERSION==1.0");
or:      GetICIndex("structure"=>"IBIS-ALRT-LIM","numLog"=> 1);
or:      GetICIndex("structure"=>"IBIS-ALRT-LIM","filematch"=> "swg_osm.fits[1]");
where filematch takes the TSTART from the file and uses it to construct
a selection expression.  If filematch and select, the expressions
will be ANDed.  

NOTE:  Default behavior is to create index named <structure>-IDX.fits

optional "sort", sortType, sortOrder, accessType as for ic_find.

NOTE:  default "error"=>1 will error if nothing found;  set to 0 and it will return with nothing.  

=cut 

sub GetICIndex {
	
	my %att = @_;
	my $retval;
	my @result;
	my $file;
	my $oldcommonlog;
	my $tstart;
	
	&Error ( "GetICIndex must have data structure name at minimum!" ) 	#	060525
		unless ($att{structure});
	
	$att{alias} = $ENV{IC_ALIAS} unless ($att{alias});
	$att{master} = "$ENV{REP_BASE_PROD}/idx/ic/ic_master_file.fits" unless ($att{master});
	$att{index} = "$ENV{REP_BASE_PROD}/idx/ic/$att{structure}-IDX.fits" unless (defined($att{index}));
	$att{subIndex} = $att{structure}."-IDX.fits" unless (defined($att{subIndex}));
	$att{select} = "" unless ($att{select});
	$att{sort} = "VSTART VSTOP" unless ($att{sort});
	$att{sortType} = "1" unless (defined($att{sortType}));
	$att{sortOrder} = "1" unless (defined($att{sortOrder}));
	$att{accessType} = "DAL_DISK" unless (defined($att{accessType}));
	$att{error} = 1 unless (defined($att{error}));
	$att{filematch} = "" unless ($att{filematch});
	$att{keymatch} = "TSTART" unless ($att{keymatch});
	
	print "***********************************************************************\n";
	print "*******     Finding IC structure $att{structure} using alias $att{alias} and master table $att{master}\n";
	print "***********************************************************************\n";
	
	if ($att{filematch}) {
		print "*******     Getting $att{keymatch} keyword from $att{filematch}\n";
		($retval,$tstart) = GetAttribute("$att{filematch}","$att{keymatch}","DAL_DOUBLE");
		&Error ( "cannot get $att{keymatch} keyword from $att{filematch}:\n$tstart" ) if ($retval);	#	060525
		print "*******     $att{keymatch} of $att{filematch} is $tstart\n";
		if ($att{select}) {
			$att{select} = "($att{select}) && ($tstart > VSTART) && ($tstart < VSTOP)";
		}
		else {
			#      $att{select} = "($tstart >= VSTART) && ($tstart =< VSTOP)";
			#  Write this more intuitively:
			$att{select} = "(VSTART <= $tstart) && (VSTOP >= $tstart)";
		}
		print "*******     Expr will be:  $att{select}\n";
	} # end of if filematch
	
	print "*******     Looking for index $att{index}\n";
	
	if ((-e "$att{master}") && (-e "$att{index}")) {
		print "*******     master table and index found\n";
		# call ic2dol
		$att{master} .= "[GROUPING]";
		$att{index} .= "[GROUPING]";
		
		($retval,@result) = 
		&ISDCPipeline::PipelineStep(
			"step"           => &ProcStep()." - find IC structure $att{structure}",
			"program_name"   => "ic_find",
			"par_icConfig"   => "$att{master}",
			"par_extname"    => "$att{structure}",
			"par_index"      => "$att{index}",
			"par_aliasRef"   => "$att{alias}",
			"par_select"     => "$att{select}",
			"par_sort"       => "$att{sort}",
			"par_sortType"   => "$att{sortType}",
			"par_sortOrder"  => "$att{sortOrder}",
			"par_subIndex"   => "$att{subIndex}",
			"par_accessType" => "$att{accessType}",
			"stoponerror"    => "$att{error}",
			);
		
		if ( (-e "$att{subIndex}") && !($retval)) {
			print "*******     Index $att{subIndex} was created.\n";
			print "************** end Finding IC struct *********************************\n";
			return "$att{subIndex}"."[GROUPING]";
		}
		else {
			if (!($att{error})) {
				print "*******      WARNING:  returning with nothing\n";
				print "************* end Finding IC struct ************************************\n";
				#	040820 - Jake - SCREW 1533
				&Message ( "WARNING:  no structure $att{structure} found, but continuing." );
				return;
			}
			else {
				&Error ( "no files match selection;  " );	#	060525
			}
		} # if no files found
	} # if master and index 
	
	else {
		print "*******     either master table or index not found:\n$att{master}\n$att{index}\n";
		&Error ( "cannot find master index $att{master}" ) unless (-e "$att{master}");	#	060525
		&Error ( "cannot find index $att{index}." ) unless (-e "$att{index}");	#	060525
		
	}
	
	#  this shouldn't be reached, but who knows.
	&Error ( "GetICIndex got to a place where it shouldn't have!" );	#	060525
	
}  # end of GetICIndex



##############################################################################

=item B<CollectIndex> ( %att )

=over 5 

This function takes an index and creates a working version of it, adding to an exising index if necessary.  It does so in as safe a way as possible, so that if the index is updated by a parallel process, we should not have a problem.  All pipelines which use any index in the repository should use this function or CollectIndex.  As the name indicates, this function uses the executable B<idx_merge>, while the other uses B<idx_find>.  

The function first copies the index in place, if permissions allow, to a temporary name stamped with the process ID.  Then B<idx_merge> is run to create the new index in the subdir specified.  If the working index exists, the new elements are added.  If not, it is created with the given template.  Then the temp index in the repository is deleted.  

It takes a hash as input with the following parameters:

=over 5 

=item B<index> -

full path to index

=item B<workname> -

working name for new index

=item B<subdir> -

subdir to put working index in

=item B<template> -

index template

=item B<sort> -

column to sort on [""]

=item B<sortType> -

As in idx_merge, [1]

=item B<sortOrder> -

As in idx_merge, [1]

=item B<security> -

As in idx_merge, [0]

=item B<update> -

As in idx_merge, [0]

=item B<stamp> -

As in idx_merge, [0]

=item B<type> -

pipeline type, ie. "rev", "scw", etc.  Optional

=item B<osfname> - pipeline OSF, Optional.  

=back

=back

Function to use an index safely;  we have to bend over backwards to make sure we don't have any problems with this index being updated underneath us.  First copy to temp name in place, then run idx_collect to create new version in working subdir, then delete temp copy in repos.  Can be run twice with same output working index;  adds to it.  If write protected, don't do copy in place.   

WARNING:  do not give an absolute path to the working index if it is on a different partition from the original;  DAL screws up the paths from /isdc/nrt to /divers/scratch/ etc.....

CollectIndex(
          "index" => "full path to index",
          "workname" => "working name for new index",
          "subdir" => "subdir to put working index in",
          "template" => "index template",
          ["sort" => "",]
          ["sortType" => "1",]
          ["sortOrder" => "1",]
          ["update" => "0",]
          ["stamp" => "0",]
          ["type" => "rev",]
          ["osfname" => "osfname",]

=cut

sub CollectIndex {
	croak( "CollectIndex: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	my $tmpname;
	my $status;
	my @result;
	my ($root,$path,$ext);
	
	$att{sortType} = "1" unless (defined($att{sortType}));
	$att{sortOrder} = "1" unless (defined($att{sortOrder}));
	$att{update} = "0" unless (defined($att{update}));
	$att{sort} = "" unless (defined($att{sort}));
	$att{subdir} = "./" unless (defined($att{subdir}));
	$att{osfname} = $ENV{OSF_DATASET} unless (defined($att{osfname}));
	$att{type} = $ENV{OSF_DATA_ID} unless (defined($att{type}));
	$att{stamp} = 0 unless(defined($att{stamp}));
	$att{caution} = 1 unless(defined($att{caution}));
	
	print "*************************************************************************\n";
	print "             COLLECT index $att{index}\n\n";
	print "*************************************************************************\n";
	print "******     original index name is $att{index}\n";
	print "******     working index name will be $att{workname}\n";
	print "******     subdir is $att{subdir}\n" if ($att{subdir});
	if ($att{subdir}){
		&ISDCLIB::DoOrDie ( "$mymkdir -p $att{subdir}" ) unless ( -d $att{subdir} );
		chdir("$att{subdir}") or &Error ( "Cannot chdir to $att{subdir}" );	#	060525
	}
	
	#  If writeable and caution needed, copy to temp name in place using 
	#    PID $$ as prefix
	if ( (-w "$att{index}") && ($att{caution})) {
		print "*******     Index writeable;  copying to temporary name\n";
		($root,$path,$ext) = &File::Basename::fileparse($att{index},'\..*');
		$tmpname = $path.$$."temp_$root".$ext;
		print "*******     Copying Index $att{index} to $tmpname\n";
		&File::Copy::copy("$att{index}","$tmpname") 
			or &Error ( "cannot copy $att{index} to $tmpname:  $!" );	#	060525
	}
	#  Otherwise, use in place.
	else {
		print "*******     Index write protected;  using in place\n";
		$tmpname = $att{index};
	}
	
	if (-e "$att{workname}") {
		$att{workname} .= "[GROUPING]";
		$att{template} = "";
	}
	($status) = &ISDCPipeline::PipelineStep(
		"step"          => &ProcStep()." - create working index $att{workname}",
		"program_name"  => "idx_merge",
		"par_index"     => "$att{workname}",
		"par_template"  => "$att{template}",
		"par_element"   => "$tmpname"."[GROUPING]",
		"par_sort"      => "$att{sort}",
		"par_sortType"  => "$att{sortType}",
		"par_sortOrder" => "$att{sortOrder}",
		"par_stamp"     => "$att{stamp}",
		"par_checkDupl" => "1",			#	050214 - Jake
		"type"          => "$att{type}",
		"dataset"       => "$att{osfname}",
		"subdir"        => "$att{subdir}",
		"stoponerror"   => 0,
		);
	
	unlink "$tmpname" if (-w "$tmpname");
	exit 1 if ($status);
	print "************ end COLLECT Index *************************************\n";
	return;
} # end of CollectIndex





##############################################################################

=item B<FindIndex> ( %att )

=over 5 

This function takes an index and creates a sub-index of selected members.  It does so in as safe a way as possible, so that if the index is updated by a parallel process, we should not have a problem.  All pipelines which use any index in the repository should use this function or CollectIndex.  As the name indicates, this function uses the executable B<idx_find>, while the other uses B<idx_merge>.  

The function first copies the index in place, if permissions allow, to a temporary name stamped with the process ID.  Then B<idx_find> is run to create the new index in the subdir specified.  If the working index exists, the new elements are added.  If not, it is created with the given template.  Then the temp index in the repository is deleted.  

It takes a hash as input with the following parameters:

=over 5

=item B<index> -

full path to index

=item B<workname> -

working name for new index

=item B<subdir> -

subdir to put working index in

=item B<template> -

index template

=item B<select> -

selection expression [""]

=item B<sort> -

column to sort on [""]

=item B<sortType> -

As in idx_find, [1]

=item B<sortOrder> -

As in idx_find, [1]

=item B<accessType> -

[DAL_DISK]

=item B<type> -

pipeline type, ie. "rev", "scw", etc.  Optional

=item B<osfname> -

pipeline OSF, Optional.  

=back

=back

Function to use an index safely;  we have to bend over backwards to make sure we don't have any problems with this index being updated underneath us.  First copy to temp name in place, then run idx_find to create new version in working subdir, then delete temp copy in repos.  Can be run twice with same output working index;  adds to it.  If write protected, don't do copy in place.   

The main difference with CollectIndex is the selection expression.

WARNING:  do not give an absolute path to the working index if it is on a different partition from the original;  DAL screws up the paths from /isdc/nrt to /divers/scratch/ etc.....

FindIndex(
          "index" => "full path to index",
          "workname" => "working name for new index",
          "subdir" => "subdir to put working index in",
          "template" => "index template",
          "select" => "selection expression",
          ["sort" => "",]
          ["sortType" => "1",]
          ["sortOrder" => "1",]
          ["accessType" => "DAL_DISK",]
          ["type" => "rev",]
          ["osfname" => "osfname",]
          ["required" => 1,]

=cut

sub FindIndex {
	
	croak( "FindIndex: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	my $tmpname;
	my $status;
	my @result;
	my ($root,$path,$ext);
	
	$att{sortType} = "1" unless (defined($att{sortType}));
	$att{sortOrder} = "1" unless (defined($att{sortOrder}));
	$att{required} = 1 unless (defined($att{required}));  # error if nothing found?
	
	$att{sort} = "" unless (defined($att{sort}));
	$att{subdir} = "./" unless (defined($att{subdir}));
	$att{osfname} = $ENV{OSF_DATASET} unless (defined($att{osfname}));
	$att{type} = $ENV{OSF_DATA_ID} unless (defined($att{type}));
	print "*****************************************************************************\n";
	print "             FIND index $att{index}\n\n";
	print "*****************************************************************************\n";
	print "****** original index name is $att{index}\n";
	print "****** working index name will be $att{workname}\n";
	print "****** subdir is $att{subdir}\n" if ($att{subdir});
	print "****** selection is '$att{select}'\n" if ($att{select});
	if ($att{subdir}){
		&ISDCLIB::DoOrDie ( "$mymkdir -p $att{subdir}" ) unless ( -d $att{subdir} );
		chdir("$att{subdir}") or &Error ( "Cannot chdir to $att{subdir}" );	#	060525
	}
	
	#  If write protected, return name is all necessary;  can be used in place
	if (!-w "$att{index}"){
		print "*******     Index write protected;  using in place\n";
		$tmpname = $att{index};
	}
	# otherwise,  copy to temp name in place using PID $$ as prefix
	else {
		print "*******     Index writeable;  copying to temporary name\n";
		($root,$path,$ext) = &File::Basename::fileparse($att{index},'\..*');
		$tmpname = $path.$$."temp_$root".$ext;
		&File::Copy::copy("$att{index}","$tmpname") 
			or &Error ( "cannot copy $att{index} to $tmpname:  $!" );	#	060525
	}
	if (-e "$att{workname}") {
		$att{workname} .= "[GROUPING]";
		$att{template} = "";
	}
	($status,@result) = &ISDCPipeline::PipelineStep(
		"step"           => &ProcStep()." - create working index $att{workname}",
		"program_name"   => "idx_find",
		"par_subIndex"   => "$att{workname}",
		"par_index"      => "$tmpname"."[GROUPING]",
		"par_select"     => "$att{select}",
		"par_sort"       => "$att{sort}",
		"par_sortType"   => "$att{sortType}",
		"par_sortOrder"  => "$att{sortOrder}",
		"par_accessType" => "DAL_DISK",
		"type"           => "$att{type}",
		"dataset"        => "$att{osfname}",
		"subdir"         => "$att{subdir}",
		"stoponerror"    => 0,
		);
	
	unlink "$tmpname" if (-w "$tmpname");
	exit 1 if ($status);
	# check for result;  if exists, return.
	# 
	# (has extn only if existed before)
	print "************ end FIND Index ****************************************\n";
	return if ((-e "$att{workname}") || ($att{workname} =~ /\[/)); 
	# if not, and if required, then error
	&ISDCPipeline::PipelineStep(
		"step"         => "ERROR from FindIndex",
		"program_name" => "ERROR",
		"error"        => "Index selection $att{select} resulted in no match from index $att{index}",
		"dataset"      => "$att{osfname}",
		"type"         => "$att{type}",
		) if ($att{required});
	# otherwise, print warning and return.
	print "*******     WARNING:  Index selection $att{select} resulted in no match for index $att{index}\n";
	print "************ end FIND Index ****************************************\n";
	return;
	
} # end of FindIndex


##############################################################################

=item B<DiffOBTs> ($obt1, $obt2)

This function takes two strings in the format of OBTs (20 digits in units of 2^-20 seconds) and finds their difference, which is not correct with Perl operators due to the length of the integers.  It simply takes two strings and then runs the system command `echo obt1 - obt2 | bc` and returns the result.

=cut

sub DiffOBTs {
	#  give two big numbers, does the difference with bc and formats as an
	#   obt, i.e. 20 digits.
	my ($obt1, $obt2) = @_;
	
	print "*******     Calculating difference of $obt1 and $obt2 in OBT format\n";
	my $difference = `$myecho $obt1 - $obt2 | $mybc`;
	chomp $difference;
	&Error ( "Cannot perform difference!" ) if ($?);	#	060525
	$difference = 0 if ($difference < 0);
	$difference = sprintf("%020s",$difference);
	
	print "*******     Difference is $difference\n";
	return $difference;
} # end sub DiffOBTs




##############################################################################

=item B<WriteAlert> ( %att )

This function takes a alert message, ID, and level and calls ril_write.  It uses the PipelineStep function with the usual defaults found from the environment.

=cut

sub WriteAlert {
	#  Give task, step, level, subdir.  

	croak( "WriteAlert: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	my ($retval,@result);
	$att{task} = "Pipeline - ".&ProcStep() unless(defined($att{task}));
	$att{step} = &ProcStep()." - ALERT" unless ($att{step});
	$att{level} = 0 unless ($att{level});
	$att{subdir} = "" unless ($att{subdir});
	
	#   Create subdir if it doesn't exist;  make group writeable.  
	if ( ($att{subdir}) && (!-d "$att{subdir}")) {
		($retval,@result) = &ISDCPipeline::RunProgram("$mymkdir -p $att{subdir}");
		&Error ( "cannot mkdir $att{subdir}:  @result" ) if ($retval);	#	060525
		($retval,@result) = &ISDCPipeline::RunProgram("$mychmod g+w $att{subdir}");
		&Error ( "cannot chmod g+w $att{subdir}:  @result" ) if ($retval);	#	060525
	}
	
	&Error ( "alert level $att{level} not allowed" ) unless ($att{level} =~ /^[0-3]$/);	#	060525
	
	$att{id} = "" unless (defined($att{id}));
	
	&ISDCPipeline::PipelineStep(
		"step"         => $att{step},
		"program_name" => "ril_write",
		"par_LogType"  => "Alert_$att{level}",
		"par_Message"  => "$att{message}",
		"par_Task"     => "$att{task}",
		"par_ProcedureNumber" => "$att{id}",
		"subdir"       => $att{subdir},
		);
	
	return;
	
}


##############################################################################

=item B<BBUpdate> ( %att )

This function takes any of a variety of selection parameters and updates a blackboard based on the requested modification.  It can select based on type, matching string, current status, an excluded string, a minimum age, etc., and change it status or delete the OSF.  It is used primarily by the various monitoring and cleaning processes.  

Function used in pipeline monitors to search for OSFs to update or delete en masse.  Primarily used in cleanup functions, so the defaults are set for that, but it can be used generally by bb_mod.pl to do just about anything.  

This function remains general and expects the calling script to hand it any combination of the following:

      "agelimit" => "days between creation and when it should be deleted",
      "type" => "irv",
      "match" => "2002120",
      "errors" => 1,
      "cdate" => "YYYYMMDDHHMMSS", (or any substring starting large obviously)
      "dcf" => "IBI",
      "list" => "space separated list of specific datasets"
      "exclude" => "string not to match in dataset

to find what datsets to update.  It will usually NAD these as you'd expect, where errors means only OSFs with error status (normally, you want a longer agelimit for these), cdate means OSFs created before this date (where you can specify a partial string, e.g. 20021213 and it will pack with zeros), match is any string the OSF_DATASET must match (which can include special characters for pattern matching in //, e.g. "^RRRR"), type is the data type, and dcf the DCF.  Obviously, agelimit and cdate are related;  saying agelimit=1day is the same as saying cdate<=(now - 1day).  Note that curstat can be used as a selection, but note that it's not checked carefully, only a simple pattern match current =~ /curstat/;  so put special characters in curstat to match what you want within the full status string of the OSF, e.g. to match the 3rd column of 7 as x, "curstat" => "^\w{2}x".  

Then you specify what to do with it:  
   - either take the default behavior, where the "CL" column is expected to be "_" and is changed to "o".
   - changed those using column, curstat, and newstat respectively.
   - fullstat may be specified, giving all desired column status values, e.g. "fullstat" => "cccccco" is the same as the default for nrtscw
   - "delete" => 1 will delete the OSFs instead.  This option may only be used from the command line, as you will be queried.  To delete within a pipeline, update the CL to d (which is done after setting it to o, which does additional cleanup.)
   - "return" => 1 will do nothing but return the list of OSFs in an array.
   - "return" => 2 will do nothing but return a hash of OSFs vs status

(Note that the returned array of OSFs is sorted, but not necessarily the keys of the hash table.  I don't know why.)

Typicall, then, the calling script will use this function a handful of times for all the things it wants cleaned on different timescales.  E.g. clean completed OSFs after one week and error OSFs after one month.

The optional nocheck parameter, which if defined and non-zero means that the curstat value will not be enforced, i.e. an OSF will be updated even if curstat is specified and the current OSF doesn't match it.  This is useful in ADP, for example, where something may have been  marked for cleaning automatically (empty THFs), and either encountered an error, or the delete proces isn't running, or some other unforseen circumstance.  A bit dangerous and may not be useful, but we'll see.

Note that in all cases, if the OSF status contains a p anywhere, it is skipped.  We don't want to update an OSF which is processing.

=cut

sub BBUpdate {
	
	croak( "BBUpdate:  Need even number of args" ) if ( @_ % 2 );
	
	my $interactive = &Interactive();
	print "*****************************************************************************\n";
	print &TimeLIB::MyTime()."     BBUpdate\n";
	print "*****************************************************************************\n";
	
	my %att = @_;
	my ($retval,@result);
	my ($osf,@osfs);
	my ($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command);
	my $now;
	my @cdate;
	my $rootcommand;
	my @files;

	$att{safe} = "no" unless ($att{safe});
	
	#	050907 - Jake - SPR 4314
	$att{donotconfirm} = 0 unless ( defined ($att{donotconfirm}) );

#	foreach (keys(%att)) {
#		print ">>>>>>>     DEBUG:  given parameter $_ with value $att{$_}\n";
#	}
	
	if ( (!defined($att{path})) && (defined $ENV{PATH_FILE_NAME}) )  {
		$att{path} = $ENV{PATH_FILE_NAME};
		$att{path} =~ s/\.path//;
	}
	elsif (!defined($att{path})) {
		&Error ( "parameter path not defined and PATH_FILE_NAME not set either!" );	#	060525
	}
	
	######
	#  Get the options:
	######
	
	#  Default behavior turned off in resource files by setting OSF_AGELIMIT
	#   to 0.
	if ( (defined $att{agelimit}) && ($att{agelimit} == 0) ) {
		#  In interactive program bb_mod.pl, have to call with agelimit set to
		#   something, even empty.  But only in pipeline usage is it correctly
		#   set to 0 when you want to turn off somethign without commenting
		#   out code.  So, if defined and not interactive, if it's zero, then
		#   this was turned off and we quit immediately.
		if ($interactive) {
			print ">>>>>>>     WARNING:  given AGE LIMIT of 0;  ignoring.\n";
			delete $att{agelimit};
		}
		else {
			print ">>>>>>>     WARNING:  given AGE LIMIT of 0;  quitting.\n";
			return;
		}
		
	}
	
	#  delete
	if ($att{delete}) {
		#  Test if you're interactive;  only allowed if so.
		&Error ( "cannot specify BBUpdate option delete from within a pipeline!  "	#	060525
			."Instead, update the CL column to d to do this properly within OPUS." ) unless ($interactive);
		print "\n*******     Deleting datasets ";
	}
	elsif ($att{return}) {
		print ">>>>>>>     Looking for datasets ";
	}
	#  fullstat (specify all column status values)
	elsif ($att{fullstat}) {
		print ">>>>>>>     Updating datasets from CURRENT $att{matchstat} to new "
			."full STATUS $att{fullstat}" if ($att{matchstat}); 
		print ">>>>>>>     Updating datasets to new full STATUS $att{fullstat}" 
			unless ($att{matchstat}); 
	}
	#  or specify a column and current and old status values of that one only
	else {
		$att{column} = "CL" unless ($att{column});
		$att{newstat} = "o" unless ($att{newstat});
		$att{curstat} = "_" unless ($att{curstat});
		print ">>>>>>>     Updating datasets in COLUMN $att{column} from CURRENT "
			."$att{curstat} to NEW status $att{newstat} "; 
	}
	
	#  list
	if ($att{list}) {
		print  ".  The following datasets were given as a list input:\n$att{list}\n"
			."(Note that if other parameters for selecting datasets were given, they will be ignored.)\n";
	}
	
	#  type
	if ($att{type}) {
		print " of TYPE $att{type}";
	}
	#  dcf
	if ($att{dcf}) {
		print " of DCF $att{dcf}";
	}
	
	#  error OSFs only
	if ($att{errors}) {
		print " with an ERROR status";
	}
	
	#  matching string
	if ($att{match}) {
		print " MATCHING string $att{match}";
	}
	
	
	#  agelimit (given in days after creation date of OSF)
	if ($att{agelimit}) {
		print " on a AGELIMIT of $att{agelimit} days (".($att{agelimit} *= 3600 * 24)." seconds), i.e. ";
		#  To specify a agelimit is to ask for OSFs where cdate + agelimit < now, 
		#   which is the same thing as saying cdate < now - agelimit.  So we take
		#   the two options and make them the same thing.
		#
		#  The difficulty is doing calculations in the format YYYYMMDDHHMMSS;  
		#   can't just subtract.  Do the calculation in seconds and then go back.
		#   It's annoying, but I want these times human readable in the logging
		#   so debugging is easier.  
		#
		#    $now = time;  # non-leap seconds since Jan1, 1970, UTC
		#    print "\n>>>>>>>    DEBUG:  now is $now\n"; 
		#    print ">>>>>>>    DEBUG:  limit is now ".($now -  $att{agelimit} )." seconds\n";
		#    @cdate = localtime ($now -  $att{agelimit} );
		#    # @cdate = (sec,min,hour,mday,mon,year,wday,yday,isdst)
		#    $cdate[5] += 1900;  #  since localtime returns year - 1900
		#    $cdate[4] += 1;  # since localtime returns months as 0..11
		#    #  Tack on zero if necessary
		#    foreach ($cdate[0],$cdate[1],$cdate[2],$cdate[3],$cdate[4]) {
		#      $_ = "0".$_ if (/^\d$/); 
		#    }
		#    $att{cdate} = join '', (@cdate)[5,4,3,2,1,0];
		
		#  New function which does the above.  
		$att{cdate} = &TimeLIB::UTCops("delta" => $att{agelimit});  
		
	}
	
	#  cdate (creation date before X)
	if ($att{cdate}) {
		print " created before DATE $att{cdate}";
		#  Pack the right side with zeros until it's 14 characters, i.e.
		#   if somebody says 20021121, then make it 20021121000000.  That
		#   way, people don't have to specify the seconds but *can* if they want.
		while ($att{cdate} !~ /^\d{14}$/) {
			$att{cdate} = $att{cdate}."0";
		}
		
	}
	
	print "\n>>>>>>>     BBUpdate given safe=yes, so will use osf_test instead of ls "
		."in several places.\n" if ($att{safe} eq "yes");
	print "\n\n";
	
	if ($att{delete}) {
		$rootcommand = "osf_delete ";
	}
	elsif ($att{newstat}) {
		$rootcommand = "osf_update -c $att{column} -s $att{newstat} ";
	}
	elsif ($att{fullstat}) {
		$rootcommand = "osf_update -s $att{fullstat} ";
	}
	elsif ($att{return}) {
	}
	else {
		&Error ( "quite frankly, I'm confused.  You didn't specify delete, return, or a new status." );	#	060525
	}
	
	#  Sanity check:  there are only four current cases:
	#   1)  interactively, you can do whatever you want
	#   2)  you give a list of OSFs
	#   3)  you give a date/timeout
	#   4)  you give a revolution number in match
	#  If you don't give a date, match, or a list and you're in a pipeline,
	#   then this errors, because generally that means a problem in the
	#   calling script.  If you need to add a different possibility,
	#   modify this sanity check accordingly so as not to shoot yourself
	#   in the foot.
	if (! ( ($att{list}) || ($att{cdate}) || ($att{match}) || ($interactive) || ($att{return}) ) ) {
		&Error ( "sanity check failed, i.e. you're not running interactively, but you didn't "	#	060525
			."give either a list, match, or a date/timeout selection, nor did you just ask for a list returned.  "
			."Rather than make a mess, I'm just going to quit now." );
	} # end sanity check
	
	######
	#   Go look for matching OSFs:
	######
	
	if ($att{list}) {
		
		#  Still want to check the status and only update them if they are correct:
		foreach $osf (split ' ',$att{list}) {
			
			($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF (`$myls $ENV{OPUS_WORK}/$att{path}/obs/*$osf*`) 
				unless ($att{safe} eq "yes");
			($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF (`osf_test -p $att{path} -f $osf`) 
				if ($att{safe} eq "yes");
			
			#  Check curstat and column, since this is used in ADP without checking:
			if (($att{column}) && ($att{curstat})) {
				#  Can't check this with the above, which can't easily figure out
				#   which part of the status goes with the column name.
				@result = `osf_test -p $att{path} -f $dataset -pr $att{column}`;
				chomp $result[0];
				&VerbalNext(">>>>>>>     DEBUG:   status $result[0] of column $att{column} is not $att{curstat};  skipping.\n")  
					if ( ($result[0] !~ /$att{curstat}/) && !($att{nocheck}) );
			}
			
			push @osfs, $dataset;
			
		}  # foreach osf
		
	} # if list given
	
	else {
		
		@files = `$myls $ENV{OPUS_WORK}/$att{path}/obs 2> /dev/null` unless ($att{safe} eq "yes");
		@files = `osf_test -p $att{path}` if ($att{safe} eq "yes");
		
		foreach $osf (@files) {
			
			chomp $osf;
			next unless ($osf =~ /\w/);  #  get blanks from osf_test sometimes
			$osf = &File::Basename::basename($osf);
			next if ($osf =~ /^lock$/); # from ls
			($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF ($osf);
			
			#      print ">>>>>>>     DEBUG:  got dataset $dataset with status $osfstatus\n";
			
			#  Touch nothing which is currently processing (but OK to return):
			#      &VerbalNext(">>>>>>>     DEBUG:  status $osfstatus still processing;  skipping.\n") if  ( ($osfstatus =~ /p/) && !($att{return}));
			next if  ( ($osfstatus =~ /p/) && !($att{return}));
			
			#  Now, for each of the possible selection criteria above, test and next:
			
			#  TYPE
			#      &VerbalNext(">>>>>>>     DEBUG:  type $thistype doesn't match $att{type};  skipping.\n") if ( ($att{type}) && ($thistype !~ /$att{type}/i) );
			next if ( ($att{type}) && ($thistype !~ /$att{type}/i) );
			
			#  DCF
			#      &VerbalNext(">>>>>>>     DEBUG:   DCF $dcfnum doesn't match $att{dcf};  skipping.\n") if ( ($att{dcf}) && ($dcfnum !~ /$att{dcf}/i) );
			next if ( ($att{dcf}) && ($dcfnum !~ /$att{dcf}/i) );
			
			#  ERROR status
			#      &VerbalNext(">>>>>>>     DEBUG:   status $osfstatus doesn't contain an ERROR;  skipping.\n")  if ( ($att{errors}) && ($osfstatus !~ /x/) );
			next  if ( ($att{errors}) && ($osfstatus !~ /x/) );
			
			#  Match
			#      &VerbalNext(">>>>>>>     DEBUG:   dataset $dataset doesn't match string $att{match};  skipping.\n")  if ( ($att{match}) && ($dataset !~ /$att{match}/) );

			#	print (">>>>>>>     DEBUG:   dataset $dataset doesn't match string $att{match};  skipping.\n") if ( ($att{match}) && ($dataset !~ /$att{match}/i) ); 
			next  if ( ($att{match}) && ($dataset !~ /$att{match}/i) ); 
			
			#  ESCLUDE Match
			#      &VerbalNext(">>>>>>>     DEBUG:   dataset $dataset matches exclude string $att{exclude};  skipping.\n")  if ( ($att{exclude}) && ($dataset =~ /$att{exclude}/) ); 
			next  if ( ($att{exclude}) && ($dataset =~ /$att{exclude}/i) ); 
			
			#  Creation Date
			#     &VerbalNext(">>>>>>>     DEBUG:   OSF creation date ".(&TimeLIB::HexTime2Local($hextime))." not later than requested creation date $att{cdate};  skipping.\n")  if ( ($att{cdate}) && (&TimeLIB::HexTime2Local($hextime) > $att{cdate}) );
			next if ( ($att{cdate}) && (&TimeLIB::HexTime2Local($hextime) > $att{cdate}) );
			
			#  MatchStatus
			#      &VerbalNext(">>>>>>>     DEBUG:   status $osfstatus doesn't match $att{matchstat};  skipping.\n")  if ( ($att{matchstat}) && ($osfstatus !~ /$att{matchstat}/) );
			next  if ( ($att{matchstat}) && ($osfstatus !~ /$att{matchstat}/) );
			
			#  CurStat and Column
			if (($att{column}) && ($att{curstat})) {
				#  Can't check this with the above, which can't easily figure out
				#   which part of the status goes with the column name.
				@result = `osf_test -p $att{path} -f $dataset -pr $att{column}`;
				chomp $result[0];
				#	&VerbalNext(">>>>>>>     DEBUG:   status $result[0] of column $att{column} is not $att{curstat};  skipping.\n")  if ( ($result[0] !~ /$att{curstat}/) && !($att{nocheck}) );
				next if ( ($result[0] !~ /$att{curstat}/) && !($att{nocheck}) );
			}
			
			#  If you're still here, then you want this one.
			#      print ">>>>>>>     DEBUG:  matched dataset $dataset with status $osfstatus\n";
			push @osfs, $dataset;
			
		} # end foreach osf in path/obs
		
	} # end else (not given $att{list})
	
	
	if ($#osfs < 0) {
		print "*******     ".&TimeLIB::MyTime()." - Found no OSFs matching your input.  Quitting.\n";
		return;
	}
	
	
	######
	#  If you just wanted the list, return it:
	######
	if ( (defined $att{return}) && ($att{return} == 1) ) {
		print ">>>>>>>    ".&TimeLIB::MyTime()." - No action given;  returning array of matching OSFs.\n";
		return (sort @osfs);
	}  
	elsif ( (defined $att{return}) && ($att{return} == 2) ) {
		print ">>>>>>>    ".&TimeLIB::MyTime()." - No action given;  returning hash of matching OSFs and their status.\n";
		my %hash;
		#  NOTE:  this sorting doesn't make it through, i.e. if you
		#   take the returned hash and don't sort the keys, they might not
		#   be sorted, even though here I sort them before defining them.  
		foreach $osf (sort @osfs) {
			#  Note:  match with __ after;  no datasets so long that there won't
			#   be two _ to follow.  Yes, an assumption which might break if 
			#   somebody makes a *really* long OSF.  But need to match 
			#             .OSF__ 
			#   so we don't get e.g. science windows associated with the ObsGrp
			#   in SA.  
			
			############################
			#   SPR 3034:  this ls gives rise to transient errors when the 
			#    OSF is being updated at that instant.  Seems unlikely, but it
			#    has happened half a dozen times in a week in SA, which is far 
			#    too many.  I don't want to mess with NRT, which seems OK, so 
			#    make it optional how we do this.  Use ls unless specified to
			#    be "safe", i.e. use osf_test.
			
			if ($att{safe} eq "yes") {
				$osf = `osf_test -p $att{path} -f $osf`;
				($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF ($osf);
			}
			else {
				($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command) = &OPUSLIB::ParseOSF (`$myls $ENV{OPUS_WORK}/$att{path}/obs/*.${osf}__*`);
			}
			$hash{$osf} = $osfstatus;      
			
		} # foreach osf
		print ">>>>>>>    ".&TimeLIB::MyTime()." - Done.\n";
		return %hash;
	} # if return=2
	

	######
	#  Otherwise, continue.  If interactive, query to confirm:
	######

	#	050907 - Jake - SPR 4315
	if ( ( $interactive ) && ( ! $att{donotconfirm} ) ) {
		#	Only prompt IF 
		#		$interactive (ie. at the command line from bb_mod.pl or something
		#		AND I didn't say $att{donotconfirm}

		print ">>>>>>>     Found the following OSFs matching your input:\n".join("\n",@osfs)."\n";

		print ">>>>>>>     Are you sure you want to update the status of ".($#osfs + 1)." OSF(s) to $att{newstat}?  Type 'yes':  " if ($att{newstat});
		print ">>>>>>>     Are you sure you want to update the status of ".($#osfs + 1)." OSF(s) to $att{fullstat}?  Type 'yes':  " if ($att{fullstat});
		print ">>>>>>>     Are you sure you want to delete ".($#osfs + 1)." OSF(s)?  Type 'yes':  " if ($att{delete});
		my $reply = <STDIN>;
		chomp $reply;
		if ($reply !~ /^yes$/) {
			print ">>>>>>>     You didn't type 'yes';  quitting.\n";
			exit 0;
		}
	}  # end if interactive
	
	
	######
	#  Now do it:
	######
	foreach $osf (@osfs) {
		$command = $rootcommand." -p $att{path} -f $osf";
		
		print ">>>>>>>     Running \'$command\'\n";
		@result = `$command`;
		
		if ($?) {
			print  ">>>>>>>     ERROR:  status $? from command '$command':\n@result\n>>>>>>>     quitting.\n";
			&Error ( "status $? from command '$command':\n@result\n>>>>>>>     quitting." );	#	060525
		}
		else {
			print ">>>>>>>     Command successful.\n";
		}
		
	} # end foreach $osf
	
	
	print ">>>>>>>     ".&TimeLIB::MyTime()."   Done.\n";
	print "*****************************************************************************\n";
	
	return;
	
} # end sub BBUpdate


##############################################################################

=item B<VerbalNext> ( )

Tiny function to help debugging in loops;  passed a string which you can print here or not, depending on whether you want to see lots and lots of output.

I don't think that this is used anywhere anymore.

=cut

sub VerbalNext {
	
	#  print @_;
	next;
	
} # end sub VerbalNext


##############################################################################

=item B<Interactive> ( )

Tiny function returns 1 if interactive and 0 if not (e.g. OPUS).  Page 518 of Perl Cookbook.  Basically tests if STDIN and STDOUT are opened to a tty.

=cut

sub Interactive {
	return -t STDIN && -t STDOUT;
} # end sub Interactive


###########################################################################

=item B<ConvertTime> ( %att )

ConvertTime wrapper for converttime executable

ConvertTime(
               "informat"  => "",   
               "intime"    => "",
               "outformat" => "",
               "dol"       => "",
               "accflag"   => "",
               );

formats = {YYYYDDDHH, TT, IJD, IJS, UTC, CCSDS, OBT, OBTFITS, REVOLUTION}

dol must be set to a swg if converting to/from OBT.

Maybe this should be in TimeLIB.pm

=cut

sub ConvertTime {
	
	my %att = @_;
	$att{accflag} = 5 unless (defined $att{accflag});
	$att{dol} = ""unless (defined $att{dol});
	
#
#		060915 - Jake - apparently this is no longer true, but it will take a bit longer without a dol
#
#	&Error ( "ConvertTime expects a DOL for OBT conversions!" ) 	#	060525
#		if ( (  ($att{informat} =~ /OBT/) || ($att{outformat} =~ /OBT/) ) && !($att{dol}) );
	
	my ($retval,@result) = 
	&ISDCPipeline::PipelineStep(
		"step"          => &ProcStep()." - convert $att{informat} to $att{outformat}",
		"program_name"  => "converttime",
		"par_informat"  => "$att{informat}",
		"par_intime"    => "$att{intime}",
		"par_outformat" => "$att{outformat}",
		"par_dol"       => "$att{dol}",
		"par_accflag"   => "$att{accflag}",		#  Jake corrected 040108 SPR 3411
		"getstdout"     => 1,
		);
	
	my $result;
	foreach (@result) { 
		next unless /^.*.$att{outformat}.:\s+(\S+)\s*$/i;
		$result = $1;
		last;
	}
	&Error ( "cannot parse result:@result" ) unless ($result =~ /\w/);	#	060525
	print "*******     $att{informat} $att{intime} converted to $att{outformat} is $result\n";
	return $result;
	
	
} # end ConvertTime


##############################################################################

=item B<UTCops_OLD> ( %att )

Maybe this should be trashed

UTCops("now" => "time in seconds","delta" => "seconds to subtract")

or

UTCops("utc" => "YYYYMMDDHHMMSS","delta" => "seconds to subtract")

Returns UTC format YYYYMMDDHHMMSS, minus given delta seconds [optional].  
If no start time is given, it calls time to get the non-leap seconds 
since Jan1, 1970, UTC.

Note that the delta parameter can be negative, to *add* seconds.  

=cut

sub UTCops_OLD {
	my %att = @_ ;
	my ($year,$mon,$mday,$hour,$min,$sec);
	$att{delta} = 0 unless (defined $att{delta});
	# non-leap seconds since Jan1, 1970, UTC
	$att{now} = time unless (defined $att{now} || defined $att{utc});
	
	if (defined $att{utc}) {
		#              1:YYYY   2:MM   3:DD   4:hh   5:mm    6:ss
		$att{utc} =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/;
		($year,$mon,$mday,$hour,$min,$sec) = ($1,$2,$3,$4,$5,$6);
		$year -= 1900;  #  timelocal takes a year which has 1900 subtracted
		$mon -= 1;  # timelocal takes months 0..11
		
		#  timelocal(ss,mm,hh,DD,MM,YY) 
		#  returns like time above, non-leap seconds since Jan1, 1970, UTC
		#  corresponding to the input.  
		$att{now} = Time::Local::timelocal($sec,$min,$hour,$mday,$mon,$year);
		
	}
	
	#  Now, whether given nothing (now), a time in seconds to be called "now",
	#   or a time in UTC to be called "now", take that "now", apply the delta,
	#   and convert the result back to YYYYMMDDHHMMSS.  
	
	my @cdate = localtime ($att{now} - $att{delta} );  # converts to 
	# @cdate = (sec,min,hour,mday,mon,year,wday,yday,isdst)
	#  in local time zone (which is GMT on Ops net.)
	
	$cdate[5] += 1900;  #  since localtime returns year - 1900
	$cdate[4] += 1;  # since localtime returns months as 0..11
	#  Tack on zero if necessary
	foreach ($cdate[0],$cdate[1],$cdate[2],$cdate[3],$cdate[4]) {
		$_ = "0".$_ if (/^\d$/); 
	}
	
	#  Returns string YYYYMMDDHHMMSS
	return join '', (@cdate)[5,4,3,2,1,0];
	
}  # end sub Sec2UTC


##############################################################################

=item B<FindDOL> ( $grpdol, $extname, $stopIfNotFound )

dal_list wrapper.

WARNING:  if there are more than one of these extnames, it will probably only return the first one.

If I have to add any more parameters, I should probably change this to a hash

=cut

sub FindDOL {
	my ( $grpdol, $extname, $stopIfNotFound ) = @_;

	$stopIfNotFound = "Yes" unless ( $stopIfNotFound );
	my ( $usenextline, $foundDOL );

	my $initialcommonlogfile = $ENV{COMMONLOGFILE};
	$ENV{COMMONLOGFILE} = "+".$ENV{COMMONLOGFILE} unless ( $ENV{COMMONLOGFILE} =~ /^\+/ );

	my ($retval,@results) = &ISDCPipeline::RunProgram(
		"dal_list dol=$grpdol extname=$extname exact=n longlisting=yes fulldols=yes mode=h"
		); 

	$ENV{COMMONLOGFILE} = $initialcommonlogfile;

	print "Searching dal_list output for $extname.\n";
	foreach (@results) {
		chomp;
		if ( $usenextline =~ /YES/ ) {
#			$foundDOL = $_;
#			$foundDOL =~ s/Log_2\s+\:\s+(.*)\s*$/$1/;
			( $foundDOL ) = ( /Log_2\s+\:\s+(.*)\s*$/ );
			print "Found $foundDOL.\n\n";
			last;
		}
		#       if this is the struct and a GROUP or a TABLE, take the next one
		#       Only if 'Log_1  :     IBIS-GNRL-GTI        TABLE Cols=         6, Rows=         3'
		#       or      'Log_1  : IBIS-GNRL-GTI-IDXren GROUP 19 child'
		#       but NOT 'Log_1  : Parameter extname = IBIS-GNRL-GTI'
		$usenextline = "YES" if ( /^Log_1\s+:\s+$extname/ );
	}

	#	SPR 4293
	&Error   ( "No $extname DOL found in $grpdol!?") 
		unless ( ( $foundDOL ) || ( $stopIfNotFound =~ /n/i ) );

	&Message ( "No $extname DOL found in $grpdol!?")
		unless ( $foundDOL );

	return "$foundDOL";
}	#	end FindDOL



##############################################################################

1;

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

