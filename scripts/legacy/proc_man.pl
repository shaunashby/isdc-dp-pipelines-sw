#!/usr/bin/perl

=head1 NAME

I<proc_man.pl> - script to display and manipulate the current B<OPUS> processes

=head1 SYNOPSIS

I<proc_man.pl>

=head1 DESCRIPTION

=item

=cut

use strict;
use warnings;

use File::Basename;
use ISDCPipeline;
use OPUSLIB qw(:osf_stati);
use ISDCLIB;
use UnixLIB;

if  ( (defined @ARGV) && ($ARGV[0] =~ /^--h/) ) {
	print "\nUSAGE:  proc_man.pl [--path=] [--process=] [--command=] [--machine=] [--pipelinefile=] [--check]\n\n"
		."where process can be an individual process or \'pipeline\', indicating start all processes in path.pipeline file,"
		." or modify/halt all processes currently running in that path.  "
		."Commands may be:  start, restart, suspend, resume, or status.  "
		."Specifying --command=status simply prints the current status.  "
		."Specify --check, with or without a path, and it will check that all "
		."processes listed in the pipeline file are currently running.\n\n";
	exit 0;
}

##############################################################################
##
##                        MAIN
##
##############################################################################
my ( $path, $process, $command, $machine ) = ( "", "", "", "" );
my ( $pid, $proc, $stat, $hex, $pth, $node, $com );
my %processes;
my %stats;
my %curcom;
my %machines;
my %paths;
my %foundpids;

my @list;
my $reply;
my $pstat;
my @pids;
my $newpstat;
my $runcom;
my @date;
my @result;
my $status;
my $check;
my $one;
my @running;
my ( $real_pid, $time );
my $warnings = 0;
my $runline;
my $donotconfirm;
my $donotabscheck;
my $pipelinefile;
my @opusworks;
my $printedwarning = 0;

&GetParameters();

###########################################################################
#  Go look for the requested processes:
###########################################################################

OPUSWORKDIR : foreach my $opuswork ( @opusworks ) {
	$ENV{OPUS_HOME_DIR} = "$opuswork/opus";

	@pids = ();

	print "\n$prefix1 Working $opuswork\n";

	#  If no particular process was specified, or if "pipeline" was, then 
	#   go look for whatever is on the blackboard or in the pipeline file:
	if ( ! ( $process ) || ( $process =~ /pipeline/ ) ) {
		print "$prefix1 Process given was 'pipeline';  expanding.\n";
		
		#  Starting or checking based on the pipeline file:
		if ( ( $command =~ /start/ ) || ( $check ) ) {
			#  To start a pipeline, get contents of .pipeline file:
			
			#  Store by PID, but in this case, don't have one yet so just number 'em:
			$pid = 0;
			
			@list = &ISDCLIB::DoOrDie ( "$mycat $pipelinefile" );

			#  Fill hashes for each column in the pipeline file:
			foreach ( @list ) {
				chomp;
				next if ( /^\s*!/ );
				next unless ( /^(\S+)\s+$path\s+(\S+)/ );
				$processes{$pid} = $1;
				$machines{$pid} = $2 unless ( $machine );
				$machines{$pid} = $machine if ( $machine );
				$paths{$pid} = $path;  # just so it works later for check
				push @pids, $pid;
				$pid++;
			}
		}# if start or check
		
		else {
			# 00003c68-ninpst___-idle___________.3df06d71-nrtinput_-nrtscw1_____________-____
			#  Note:  this traps for the wrong path, since nrtscw matches
			#   machines as well as paths.   Another check below.
			@list = glob ( "$ENV{OPUS_HOME_DIR}/*.*-${path}*-*" );
			foreach ( @list ) {
				chomp;
				$_ = &File::Basename::basename ( $_ );
				( $pid, $proc, $stat, $hex, $pth, $node, $com ) = &OPUSLIB::ParsePSTAT ( $_ );
				next if ( ( $machine ) && ( $node !~ /^$machine$/ ) );		#	071113 - Jake - SPR 4762 - added ^ and $
				next if ( ( $path ) && ( $pth !~ /$path/ ) );
				
				########################
				#  In fact, different machines may have the same PID, and this
				#   happened once.  So instead of using just the PID as the key, 
				#   use the string "node.pid"
				$foundpids{"$node.$pid"} = $pid;	#	060501 - Jake - SCREW 1856
				$pid = "$node.$pid";
				########################
				push @pids, $pid;
				$processes{$pid} = $proc;
				$machines{$pid} = $node;
				$stats{$pid} = $stat;
				$curcom{$pid} = $com;
				$paths{$pid} = $pth;
			}
		} # if not starting
	} # if pipeline
	
	else { # not pipeline
		if ( $command =~ /start/ ) {
			$pid = 0;
			push @pids, $pid;
			$processes{$pid} = $process;
			$paths{$pid} = $path;
	
			if ( $machine ) {
				$machines{$pid} = $machine;
			} else {
				@list = &ISDCLIB::DoOrDie ( "$mycat $pipelinefile" );
	
				my @machines2start;
				#  Find the appropriate machines
				foreach ( @list ) {
					chomp;
					next if ( /^\s*!/ );	#	I don't think this is really needed as the next line should take care of it.
					next unless ( /^$process\s+$path\s+(\S+)/ );
					push @machines2start, $1;
				}
				die "$prefix1 ERROR:  Path:$path and Process:$process not found in $pipelinefile"
					unless ( @machines2start );
	
				print "$prefix1 Found Machine(s): @machines2start for Path: $path and Process: $process\n";
				print "$prefix1 Do you want to $command these?  [y]:  ";
				$reply = <STDIN>;
				chomp $reply;
				if  ( ( $reply ) && ( $reply !~ /^y/i ) ) {
					print "$prefix1 Quitting.\n";
					exit 0;
				}
	
				foreach ( @machines2start ) {
					$runcom = "$0 --path=$paths{$pid} --process=$processes{$pid} --machine=$_ --command=start --donotconfirm";
					print "$prefix1 Running \'$runcom\'\n";
					print `$runcom`;
				}
				exit 0;
			}	#	not $machine
		} else {
			@list = glob ( "$ENV{OPUS_HOME_DIR}/*-*${process}_*-*${path}*" );
			@list = glob ( "$ENV{OPUS_HOME_DIR}/*-*${process}-*-${path}*" ) unless ( @list );
			
			foreach ( @list ) {
				chomp;
				$_ = &File::Basename::basename ( $_ );
				
				( $pid, $proc, $stat, $hex, $pth, $node, $com ) = &OPUSLIB::ParsePSTAT ( $_ );
				
				next if ( ( $path ) && ( $pth !~ /$path/ ) );      
				
				#  Might want to suspend both nswdp processes on different machines, for 
				#   example.
				#      die "${prefix}  ERROR:  multiple processes $process on path $path;  specify machine please.\n
				#			(I'm assuming you don't have two identical processes on the same machine.)\n" 
				#			if ( (defined $processes{$proc}) && (!(defined $machine)) && ($command !~ /stat/) );
				
				if ( ( $machine ) && ( $node !~ /^$machine$/ ) ) { next; }		#	071113 - Jake - SPR 4762 - added ^ and $
				
				$foundpids{$pid} = $pid;	#	060501 - Jake - SCREW 1856
				$processes{$pid} = $proc;
				$machines{$pid} = $node;
				$stats{$pid} = $stat;
				$curcom{$pid} = $com;
				$paths{$pid} = $pth;
				
				push @pids, $pid;
				
			}
		} # not start
	} # not pipeline
	
	
	###########################################################################
	#  Confirm what we found:
	###########################################################################
	
	if ( @pids ) {
		print "$prefix1 Found the following:\n\n" unless ( $check );

		#	060407 - Jake - for a better appearance, I now sort by path, process and machine, instead of the default, pid.
		foreach $pid ( sort { 
			$paths{$a} cmp $paths{$b} 
							||
			$processes{$a} cmp $processes{$b} 
							||
			$machines{$a} cmp $machines{$b} 
			} @pids ) {


			#	{PID}-{PROCESS}-{PROC_STAT}.{START_TIME}-{PATH}-{NODE}-{PROC_CMD}
			#	$pstat =~ /^(\w{8})-(\w{9})-(\w{15}).(\w{8})-(\w{9})-(\w{20})-(\w{4})$/;
			print "\n\n$prefix1 Looking for the following:  " if ( $check );
			printf ( "%-20s", "process=$processes{$pid}" );
			printf ( "%-20s", "path=$paths{$pid}" );
			printf ( "%-20s", "machine=$machines{$pid}" );
			printf ( "%-20s", "pid=$foundpids{$pid}" ) if ( defined ( $foundpids{$pid} ) );	#	060501 - Jake - SCREW 1856
			printf ( "%-30s", "Status=$stats{$pid}" ) if ( defined ( $stats{$pid} ) );
			printf ( "%-20s", "CurrentCommand=$curcom{$pid}" ) if ( ( defined ( $curcom{$pid} ) ) && ( $curcom{$pid} =~ /[a-z]/ ) );
			printf "\n";
			
			if ( $check ) {
				#  Check if this is on the blackboard
				#   Note [_-] after proc to tightly match adp not adpmon, but also
				#   get cleanopus, which doesn't leave space for _'s after.
				#	071113 - Jake - SPR 4762 - added the _ after machines
				$runline = "$myls $ENV{OPUS_HOME_DIR}/*-*$processes{$pid}"."[_-]"."*.*-*$paths{$pid}*$machines{$pid}_* 2> /dev/null";
				@list = `$runline`;
				if ( ! ( @list ) ) {
					print "$prefix1 WARNING:  cannot find process $processes{$pid} on path $paths{$pid} on machine $machines{$pid};  checking other machines.\n";
					$warnings++;
					#   PSTATs look like:
					#  {PID}-{PROCESS}-{PROC_STAT}.{START_TIME}-{PATH}-{NODE}-{PROC_CMD}
					#   Try to match path in right place, and not get e.g. cleanosf
					#   running on nrtscw3 in path nrtrev when you're looking for it
					#   running in path nrtscw.
					
					$runline = "$myls $ENV{OPUS_HOME_DIR}/*-$processes{$pid}"."[_-]"."*.*-$paths{$pid}*-*-* 2> /dev/null";
					@list = `$runline`;
					if ( ! ( @list ) ) {
						print "$prefix1 ERROR:  cannot find BB entry for process $processes{$pid} on path $paths{$pid} on ANY machine!\n";
						$check++;
					}
				} # end if not found on right machine
				
				foreach ( @list ) {
					chomp;
					next unless ( /$processes{$pid}/ ); # blanks
					$_ = &File::Basename::basename ( $_ );	  
					( $pid, $proc, $stat, $hex, $pth, $node, $com ) = &OPUSLIB::ParsePSTAT ( $_ );    
					print "$prefix1 Found BB entry for:         ";
					
					printf ( "%-20s", "process=$proc" );
					printf ( "%-20s", "path=$pth" );
					printf ( "%-20s", "machine=$node" );
					printf ( "%-20s", "pid=$foundpids{$pid}" );	#	060501 - Jake - SCREW 1856
					printf ( "%-30s", "Status=$stat" );
					printf ( "%-20s", "CurrentCommand=$com" );
					printf "\n";
					
					if ( $stat =~ /absent/ ) {
						$check++; 
						print "$prefix1 ERROR:  process $proc ABSENT according to BB!\n";
						next;
					}
					#  Now check that it's really running:  ps -ef will return lines like:
					#        UID   PID  PPID  C    STIME TTY      TIME CMD
					#   ops_nrt  1538  1516  0   Feb 26 ?       72:58 xpoll -p /isdc/sw/nrt_sw/prod/opus//nrtrev//nrtrev.path -r nrvirn -v 4dded9a8fc
					
					# recall that cleanosf isn't an xpoll but an osfdelete task
					#  Note space after proc to tightly match adp not adpmon

					if ( `uname -s` =~ /SunOS/ ) {
						#	$runcom = "rsh $node /usr/ucb/ps axw | egrep -v grep";
						$runcom = "rsh $node $myps axw | egrep -v grep";
					} else {
						$runcom = "$myssh $node $myps axw | egrep -v grep";
#						$runcom = "$myssh -1 $node $myps axw | egrep -v grep";
					}
					print "$prefix1 \n$prefix1 $runcom \n";
					
					@running =  grep ( /.*xpoll.*${pth}\.path\s-r\s${proc}($|\s)/, `$runcom` ) unless ( $proc eq "cleanosf" );
					
					@running =  grep ( /.*osfdelete.*${pth}\.path\s-r\s${proc}($|\s)/, `$runcom` )  if ( $proc eq "cleanosf" );
					
					if ( ( @running ) && ! ( $? ) ) {
						chomp $running[0];
						#                   PID TT         S   TIME  COMMAND
						$running[0] =~ /^\s*(\S+)\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(\S+).*/;
						$real_pid = $1;
						print "$prefix1 Confirmed process is still running on $node with PID $real_pid.\n";
					}
					else {
						print "$prefix1 ERROR:   cannot confirm process is running!\n";
						$check++;
					}
				} # foreach found

			} # if check

		} # foreach %processes
		
		next OPUSWORKDIR if ( ! ( $command ) || ( $command =~ /^stat/ ) );
	
		if ( $check ) {
			$check--; # started 1, upped every one not found.  
			print "\n$prefix1 Done checking;  there were $warnings warnings.\n";
			print "\n$prefix1 ERROR:  couldn't find $check processes.\n" if ( $check );
			print "\n$prefix1 Found all processes.\n" unless ( $check );
			exit $check;
		}
		
		#  
		#  Confirm:
		#
		unless ( $donotconfirm ) {		#	040726 - Jake - SCREW 1524 - added to assist with --command=restart
			print "$prefix1 Do you want to $command these?  [y]:  ";
			$reply = <STDIN>;
			chomp $reply;
			if  ( ( $reply ) && ( $reply !~ /^y/i ) ) {
				print "$prefix1 Quitting.\n";
				exit 0;
			}
		}
	} # if keys(%processes)
	else {
		print "$prefix1 No processes found.\n\n";
		next OPUSWORKDIR;
	}
	
	&CheckPMG();		#	this doesn't work perfectly.  if pipeline file has multiple restricted entries, it still starts them all.
	
	###########################################################################
	#  Now do it:
	###########################################################################
	
	@date = gmtime;
	$date[5] = $date[5] + 1900;
	$date[4] = $date[4] + 1;
	#  force two digit format
	foreach ( @date ) { $_ = "0${_}" if ($_ < 10); }
	
	open ( LOG, ">$ENV{OPUS_HOME_DIR}/proc_man_$date[5]-$date[4]-$date[3]T$date[2]:$date[1]:$date[0].log" ) 
		or die "$prefix1 ERROR:  couldn't open $ENV{OPUS_HOME_DIR}/proc_man_$date[5]-$date[4]-$date[3]T$date[2]:$date[1]:$date[0].log to write.";
	
	foreach $pid ( @pids ) {
		
		print "$prefix1 Sending command $command to process $processes{$pid} in path $paths{$pid} on machine $machines{$pid}\n";
		
		if ( $command =~ /start/ ) {
			#  Start command looks like:
			# odcl_broker U machine process OPUS_DEFINITIONS_DIR:blue.path
			
			# If you run proc_man on the same machine as you want to start the process
			#  on, it won't do an rsh.  This means DISPLAY will still be set, and that
			#  could cause a problem in e.g. SA where ISDCRoot will try to open a GUI.
			delete $ENV{DISPLAY} if (defined $ENV{DISPLAY});
			$runcom = "odcl_broker U $machines{$pid} $processes{$pid} OPUS_DEFINITIONS_DIR:$paths{$pid}.path";
			
		} elsif ( $command =~ /rest/ ) {		#	040726 - Jake - SCREW 1524
			#/isdc/run/pipelines/nrt/opus//00002674-nswosm___-idle___________.41051348-nrtscw___-nrtscw3_____________-halt
	
			#	040809 - Jake - added this whole section to deal with restarting absent OSFs
			my $need2halt;
			my $need2delete;
			unless ( $donotabscheck ) {
				$runcom = "$0 --path=$paths{$pid} --process=$processes{$pid} --machine=$machines{$pid} --check";
				print "$prefix1 Running \'$runcom\'\n";
				@result = `$runcom`;
				print "@result";
				foreach ( @result ) {
					if ( /Confirmed process is still running on $machines{$pid} with PID [\d]+/ ) {
						$need2halt++;
						last;
					} elsif ( ( /ERROR:   cannot confirm process is running/ ) ||
						( /ERROR:  process $processes{$pid} ABSENT according to BB/ )    ||
						( /ERROR:  couldn't find [\d]+ processes./ ) ) {
						$need2delete++;
						last;
					}
				}
			} else {
				#	071113 - Jake - SPR 4762 - added the _ after machines
				$runcom = "$myls $ENV{OPUS_HOME_DIR}/*-*$processes{$pid}"."[_-]"."*.*-*$paths{$pid}*$machines{$pid}_* 2> /dev/null";
				my $restartingosf = `$runcom`;
				chomp ( $restartingosf );
				if ( $restartingosf =~ /absent/ ) {
					$need2delete++;
				} else {
					$need2halt++;
				}
			}

			if ( $need2delete ) {
				#	This SHOULD only be 1 file, but I suppose could be more which would be a problem. Jake
				#	000061a5-csasw1___-absent_________.41178a4e-conssa___-anaS4_______________-____
				#	000061a5-csasw1___-idle___________.41178a4e-conssa___-anaS4_______________-____
				#	071113 - Jake - SPR 4762 - added the _ after machines
				$runcom = "$myrm $ENV{OPUS_HOME_DIR}/*-*$processes{$pid}"."[_-]"."*.*-*$paths{$pid}*$machines{$pid}_* 2> /dev/null";
				print "$prefix1 Running \'$runcom\'\n";
				print `$runcom`;
			} elsif ( $need2halt ) {
				$runcom = "$0 --path=$paths{$pid} --process=$processes{$pid} --machine=$machines{$pid} --command=halt --donotconfirm";#	040804 - Jake - SPR 3799
				print "$prefix1 Running \'$runcom\'\n";
				print `$runcom`;
	
				$runcom = "$myls $ENV{OPUS_HOME_DIR}/*-*$processes{$pid}"."[_-]"."*.*-*$paths{$pid}*$machines{$pid}_* 2> /dev/null";
				my $restartingosf = `$runcom`;
				chomp ( $restartingosf );
				print "$prefix1 $restartingosf\n";
	
				print "$prefix1 Waiting for OSF to disappear";
				while ( -e "$restartingosf" ) {
					print ".";
				   sleep 3;
				}
				print "\n";
			}
	
			$runcom = "$0 --path=$paths{$pid} --process=$processes{$pid} --machine=$machines{$pid} --command=start --donotconfirm";
			print "$prefix1 Running \'$runcom\'\n";
	
		} else {		#	command is NOT start or restart
			#  Reduce the command to the four letter field:
			$command =~ s/^(\w{4}).*$/$1/;
			
			#  Find the PSTAT file again, since it may have changed since you last
			#   looked;  then this had better be quick, else something else may
			#   update the PSTAT and the mv will fail.
				#	000061a5-csasw1___-idle___________.41178a4e-conssa___-anaS4_______________-____
#			@list = glob ( "$ENV{OPUS_HOME_DIR}/*-$processes{$pid}*-$paths{$pid}*-$machines{$pid}*" );
			@list = glob ( "$ENV{OPUS_HOME_DIR}/$foundpids{$pid}*-$processes{$pid}*-*.*-$paths{$pid}*-$machines{$pid}_*-*" )	#	060501 - Jake - SCREW 1856
				unless ( $processes{$pid} =~ /adp/ );	#	060501 - Jake - added this "unless"

#			@list = glob ( "$ENV{OPUS_HOME_DIR}/*-$processes{$pid}_*-$paths{$pid}*-$machines{$pid}*" ) 
			@list = glob ( "$ENV{OPUS_HOME_DIR}/$foundpids{$pid}*-$processes{$pid}_*-$paths{$pid}*-$machines{$pid}_*" ) 	#	060501 - Jake - SCREW 1856
				if ( $processes{$pid} =~ /adp/ );
			
			die "$prefix1  ERROR:  multiple processes $processes{$pid} on path $paths{$pid};  "
				."specify machine please.\n(I'm assuming you don't have two identical processes on the same machine.)" if ( $#list > 0 );
			$pstat = $list[$#list];

			chomp $pstat;
			$pstat = &File::Basename::basename ( $pstat );
			
			$newpstat = $pstat;
			#  Entries look like:
			# 00000dda-nswst____-idle___________.3dd4b4ac-nrtscw___-nrtscw2_____________-____
			$newpstat =~ s/^(.*)-(\S{4})$/$1-$command/;

			$runcom = "$mymv $ENV{OPUS_HOME_DIR}/$pstat $ENV{OPUS_HOME_DIR}/$newpstat";
			
		}
		
		#	050912 - Jake - SPR 4317
		if ( ( "$ENV{OPUS_HOME_DIR}/$pstat" eq "$ENV{OPUS_HOME_DIR}/$newpstat" ) 
			&& ( $newpstat ) ) {
			print     "Current pstat and new pstat are the same.  Skipping.\n";
			print LOG "Current pstat and new pstat are the same.  Skipping.\n";
		} else {
			print     "$prefix1 Running \'$runcom\'\n";
			print LOG "$prefix1 Running \'$runcom\'\n";
			@result = `$runcom`;	#	040804 - Jake - @result here is actually ~ "[1] 14125"
			if ( $? ) {
				print LOG "$prefix1 ERROR:  could not \'$runcom\': @result\n";
				close LOG;
				die "$prefix1 ERROR:  could not \'$runcom\': @result";
#	061211 - Jake - push this in temporarily for something
#			} else {
#				print @result;
			}
		}
	} # foreach process

}	#	end of foreach my $opuswork ( @opusworks ) {

#  If no particular process was specified, or if "pipeline" was, then 
close LOG;
print "\n$prefix1 \n$prefix1 Done.\n$prefix1\n";
exit 0;


###########################################################################

=head1 SUBROUTINES

=over

=item B<GetParameters> ( )

Parse the command line parameters and check that they make sense.

=cut

sub GetParameters {
	
	foreach ( @ARGV ) {
		
		if ( /^--pa\w*=(.*)$/ ) {
			$path = $1;
		}
		elsif ( /^--pr\w*=(.*)$/ ) {
			$process = $1;
		}  
		elsif ( /^--co\w*=(.*)$/ ) {
			$command = $1;
			$check++ if (/co\w*=check/ );
		}  
		elsif ( /^--m\w*=(.*)$/ ) {
			$machine  = $1;
			#	051114 - Jake - SCREW 1791
			$machine =~ tr/A-Z/a-z/;
			$machine =~ s/anab/anaB/i;
			$machine =~ s/anas/anaS/i;
		}
		elsif ( /^--ch/ ) {
			$check++;
			$command = "check";
		}
		elsif ( /^--donotconfirm$/ ) {
			$donotconfirm++; 		#	040726 - Jake - SCREW 1524 - added to assist with --command=restart
		}
		elsif ( /^--donotabscheck$/ ) {
			$donotabscheck++;		#	040809 - Jake - Absent OSF checking
		}
		elsif ( /^--pi\w*=(.*)$/ ) {
			$pipelinefile  = $1;	#	050307 - Jake - SCREW 1674
		}
		elsif ( /--o\w*=(.*)$/ ) {
			print "$prefix1 \n$prefix1 DO NOT USE THE --opuswork FEATURE WHEN DOING ANYTHING OTHER THAN LOOKING!!!\n$prefix1\n" unless ( $printedwarning );
			$printedwarning++;
			foreach ( `$myls -d $1` ) {
				chomp;
				print "$prefix1 Evaluating $_\n";
				print "$prefix1 WARNING : opuswork -$_- does not exist." unless ( -d $_ );
				push @opusworks, $_;
			}
		}
		else {
			die "$prefix1 ERROR:  don't recognize parameter $_";
		}
		
	} # foreach argv
	
	if ( ! ( @ARGV ) ) {
		$command = "status";
	}

	#
	#  and check that they make sense:
	#
	die "\n$prefix1 ERROR:  command must be 'start', 'restart', 'halt', 'resu', 'susp', 'check' or 'stat'"
		if ( ( $command ) && ( $command !~ /^(start|restart|halt|resu|susp|stat|check)/ ) );
	
	$command = "rest" if ( $command =~ /restart/ );
	
	if ( ( $command =~ /start/ ) && ( ! ( $process ) || ( $process =~ /pipeline/ ) ) && ( $machine ) ) {
		print "\n$prefix1 WARNING:  you didn't specify a process, so I'm starting a pipeline and "
			."ignoring the pipeline listed machine in favor of $machine.\n";
		$process = "pipeline";
	}
	
	die "$prefix1 ERROR:  you must give a path" if ( ( $command =~ /start/ ) && ( ! ( $path ) ) );
	
	$pipelinefile = "$ENV{ISDC_OPUS}/$path/$path.pipeline" unless ( $pipelinefile );
	
	unless ( @opusworks ) {
		#  Have to have OPUS_WORK set:
		if ( ( ! defined ( $ENV{OPUS_WORK} ) ) || ( ! -d "$ENV{OPUS_WORK}" ) ) {
			die "$prefix1 ERROR:  OPUS_WORK $ENV{OPUS_WORK} does not exist.";
		}
		push @opusworks, $ENV{OPUS_WORK};
	}

	return;
}


###########################################################################

=item B<CheckPMG> ( )

If starting, check PMG restrictions file

All variables are currently global, so no need to pass anything.

Note that currently, we only have restrictions where a given process can only have one instance regardless of path or machine.  So we assume that below.  If more sophisticated restrictions are needed, this will have to be rewritten.

=cut

sub CheckPMG {
	if ( $command =~ /start/ ) {
		my @entry;
		
		open ( DAT, "$ENV{ISDC_OPUS}/pipeline_lib/pmg_restrictions.dat" ) 
			or die "$prefix1 ERROR:  cannot open $ENV{ISDC_OPUS}/pipeline_lib/pmg_restrictions.dat";
		
		#  Create hash of restrictions related to inputs
		while ( <DAT> ) {
			next if ( ( /^\s*!/ ) || ( ! ( /\w/ ) ) );  # ignore comments and blank lines
			@entry = split '\.';
			
			foreach ( @pids ) {
				if ( $processes{$_} =~ /^$entry[0]$/ ) {
					print "$prefix1 DEBUG:  found PMG restriction for process $entry[0];  checking for existing instances.\n";
					
#	061020 - Jake - SPR 4595 - can't start adp because process gets confused with path
#	00005c86-adpst____-idle___________.45362893-adp______-isdcifts____________-____
#	00005c8b-adpmon___-idle___________.45362896-adp______-isdcifts____________-____
#	00005c99-adp______-idle___________.4536289b-adp______-isdcifts____________-____
#	00005ca5-adpfin___-idle___________.453628a2-adp______-isdcifts____________-____
#	00005cb3-cleanopus-idle___________.453628ab-adp______-isdcifts____________-____
#	00005cc0-cleanosf_-idle___________.453628b5-adp______-isdcifts____________-____

#					if ( `$myls $ENV{OPUS_HOME_DIR}/*-$entry[0]* 2> /dev/null` ) {
					if ( `$myls "$ENV{OPUS_HOME_DIR}/*-$entry[0]"."[-_]*.*" 2> /dev/null` ) {
						die "$prefix1 ERROR:  found existing instance of process $entry[0] which is restricted.";
					}
				} # foreach pid
			} # end if ($processes{$entry[0]})
			
		} # end while (<DAT>)
		close DAT;
		
	}  #  if start, check PMG restrictions

	return;
}



=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run /usr/bin/perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

