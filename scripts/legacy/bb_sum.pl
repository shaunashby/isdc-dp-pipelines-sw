#!perl

=head1 NAME

I<bb_sum.pl> - Utility for summarizing a blackboard in a readable form.

=head1 SYNOPSIS

I<bb_sum.pl>

=head1 DESCRIPTION

Utility for summarizing a blackboard in a readable form.

=cut

use strict;
use warnings;

use OPUSLIB;
use UnixLIB;
use TimeLIB;
use File::Basename;

my $quiet = 0;
my $do_pipeline = "";
my $pipeline;
my ($e_count,$w_count,$p_count,$c_count,$d_count,$total_count,$s_count);
my $time = &TimeLIB::MyTime();
my $osf;
my ($hextime,$osfstatus,$dataset,$thistype,$dcfnum,$command);
my $last_exec;
my $exit_status;
my $type;
my $askdcf;
my $revno;
my $cdate;
my $match = "";
my $status;
my @opusworks;
my $prefix = ">> ";																			

#  Log the time:
print "${prefix} ${time}  bb_sum:  checking pipeline blackboards\n";				

#  Check the parameter given:
foreach (sort @ARGV) {
	if (/--h/) {
		print "USAGE:  bb_sum.pl [--path=] [--errors_only] [--type=] [--dcf=] [--revno=] [--match=] [--status=]\n"
			."Note that OPUS_WORK must be set.\n";
		exit 0;
	}
	elsif (/--pi\w*\=(.*)$/) {
		$do_pipeline = $1;
		print "${prefix} Examining path ${do_pipeline} only.\n";						
	}
	elsif (/--pa\w*\=(.*)$/) {
		$do_pipeline = $1;
		print "${prefix} Examining path ${do_pipeline} only.\n";						
	}
	elsif (/--e/) {
		$quiet = 1;
	}
	elsif (/--t\w*\=(.*)$/) {
		$type = $1;
	}
	elsif (/--s\w*\=(.*)$/) {
		#  Since params sorted, we know match would have been set first.
		$status = $1;
	}
	elsif (/--dcf\=(.*)$/) {
		$askdcf = $1;
	}
	elsif (/--m\w*\=(.*)$/) {
		#  Since params sorted, we know match would have been set first.
		$match = $1;
	}
	elsif (/--o\w*\=(.*)$/) {
		foreach ( `$myls -d $1` ) {
			chomp;
			print "Evaluating $_\n";
			print "opuswork -$_- does not exist." unless ( -d $_ );
			push @opusworks, $_;
		}
	}
	elsif (/--r\w*\=(.*)$/) {
		$revno = $1;
		#  match and revno could be used at the same time.
		if ($match) {
			$match = "^${revno}.*${match}" unless ($pipeline =~ /nrtqla|consssa/);
			$match = "^\\w{5}${revno}.*${match}" if ($pipeline =~ /nrtqla|consssa/);
		}
	}
	else {
		die "${prefix} ERROR:  don't recognize parameter $_";
	}
}

unless ( @opusworks ) {
	#  Have to have OPUS_WORK set:
	if ( (  ! defined ( $ENV{OPUS_WORK} ) ) || ( ! -d "$ENV{OPUS_WORK}" ) ) {
		die "${prefix} ERROR:  OPUS_WORK $ENV{OPUS_WORK} does not exist.";
	}
	push @opusworks, $ENV{OPUS_WORK};
}

=item
Loop over all given OPUS_WORK directories.

=cut

foreach my $opuswork ( @opusworks ) {

=item
Loop over all sub directories of the current OPUS_WORK directory.

=cut

	foreach $pipeline ( glob ( "${opuswork}/*" ) ) {
		#  Skip junk files, opus dir, and any pipelines not specified (if any):
		next if ( ( $do_pipeline ne "" ) && ( $pipeline !~ /$do_pipeline/ ) );
		next if ( $pipeline =~ /opus$/ );
		next if ( ! -d "${pipeline}" );
		print "${prefix} Examining pipeline $pipeline :\n" unless ( $do_pipeline );	
		$e_count = 0;
		$w_count = 0;
		$p_count = 0;
		$c_count = 0;
		$d_count = 0;
		$s_count = 0;
		$total_count = 0;

=item
Loop over all files (osf) in the /obs/ directory in current sub directory.

=cut

		foreach $osf ( sort ( glob ( "${pipeline}/obs/*" ) ) ) {
			#	050901 - Jake - Why is this ( $osfstatus ) and not just $osfstatus ?
			( $osfstatus ) = &File::Basename::fileparse ( $osf, '\..*/' );
			next if ( $osfstatus =~ /^lock$/ );
			#  Entry looks like 
			#  XXXXXXXX-ssssss______<snip>______.DATA_SET______<snip>______-typ-dcf-___
			#  hex_time status    etc.
			#  So match between first - and .
			#  (Remember the ? means minimal matching.  And need ___ not to match
			#    cases where DCF num is "1__" for ilts, for example.)
			#    ($osfstatus =~ s/^\w{8}-([a-z]+)_+\.(\w.*[a-z0-9])___+-(\w{3})-.*/$1/i) or next;
			#    $dataset = $2;
			#    $thistype = $3;
			( $hextime, $osfstatus, $dataset, $thistype, $dcfnum, $command ) = &OPUSLIB::ParseOSF ( $osfstatus );
			
			next if ( ( $status ) && ( $osfstatus !~ /$status/ ) );
			next if ( ( $type )   && ( $thistype  !~ /$type/i ) );
			next if ( ( $askdcf ) && ( $dcfnum    !~ /$askdcf/i ) );
			next if ( ( $revno )  && ( $dataset   !~ /^$revno/ )      && ( $pipeline !~ /nrtqla|consssa/ ) );
			# for QLA, the revno is after five word chars, e.g. qsib_002100330010:
			next if ( ( $revno )  && ( $dataset   !~ /^\w{5}$revno/ ) && ( $pipeline =~ /nrtqla|consssa/ ) );
			next if ( ( $match )  && ( $dataset   !~ /$match/ ));
			
			$last_exec = "";
			$exit_status = "";
			
			$cdate = &TimeLIB::HexTime2Local ( $hextime );
			
			$total_count++ if ( $osf !~ /lock/ );
			
			if ( ( $osfstatus =~ /x/ ) && ( -e "${pipeline}/logs/${dataset}.log" ) ){				#	normal situation
				$e_count++;
				#  Some command lines too long for awk!        
				$last_exec = `$mygrep Command\\ was: ${pipeline}/logs/${dataset}.log | $mytail -1 `;
				$last_exec =~ s/^-+\s+Command\swas:\s+(\S+)\s+.*/$1/;
				chomp $last_exec;
				$last_exec = "?" unless ( $last_exec );										
	
				if ( $last_exec =~ /\// ) {		# if last_exec has a / in it, it's a unix command like /usr/bin/mv
					chomp ( my $tempstatus = `$mygrep "Retval is " ${pipeline}/logs/${dataset}.log | $mygrep "from the command" | $mytail -1` );
					( $exit_status ) = ( $tempstatus =~ /Retval is ([\-\d]+) from the command/ );
	
					#Error   2005-01-04T12:05:44:  missing IBIS IC file IBIS-VCTX-GRP
					unless ( $exit_status ) {
						chomp ( $exit_status = `$mygrep "^Error " ${pipeline}/logs/${dataset}.log | $mytail -1` );
						( $exit_status ) = ( $exit_status =~ /^Error\s*.{20}\s*(.+)$/ );
						$exit_status = "See next line...\n${prefix}   ...${exit_status}" if ( $exit_status );
					}
	
				} else {
					#	Error_2 2004-12-26T02:43:32 osm_data_check 1.1: Task osm_data_check terminating with status -12352
					chomp ( $exit_status = `$mygrep ${last_exec}\\ terminating ${pipeline}/logs/${dataset}.log | $mytail -1` );
					( $exit_status ) = ( $exit_status =~ /terminating with status ([\-\d]+)/ );	# $exit_status only a number if exists
	
					#-----   Retval is 134 from the command (not CommonExit!)
					unless ( $exit_status ) {
						chomp ( $exit_status = `$mygrep "Retval is " ${pipeline}/logs/${dataset}.log | $mygrep "from the command" | $mytail -1` );
						( $exit_status ) = ( $exit_status =~ /Retval is ([\-\d]+) from the command/ );
					}
					if ( $exit_status ) { $exit_status *= -1 if ( $exit_status < 0 ); }
	
					#-----   Retval indicates a TIMEOUT!
					unless ( $exit_status ) {
						chomp ( $exit_status = `$mygrep "Retval indicates a " ${pipeline}/logs/${dataset}.log | $mytail -1` );
						( $exit_status ) = ( $exit_status =~ /Retval indicates a (.+)\s*/ );
					}
	
					#-----   program timed out after 3600 seconds.
					unless ( $exit_status ) {
						chomp ( $exit_status = `$mygrep "program timed out after" ${pipeline}/logs/${dataset}.log | $mytail -1` );
						$exit_status = "Timed out" if ( $exit_status );
					}
	
					#Error   2005-01-04T12:05:44:  missing IBIS IC file IBIS-VCTX-GRP
					unless ( $exit_status ) {
						chomp ( $exit_status = `$mygrep "^Error " ${pipeline}/logs/${dataset}.log | $mytail -1` );
						( $exit_status ) = ( $exit_status =~ /^Error\s*.{20}\s*(.+)$/ );
						$exit_status = "See next line...\n${prefix}   ...${exit_status}" if ( $exit_status );
					}
	
				}
	#			$exit_status = `$mygrep ${last_exec}\\ terminating ${pipeline}/logs/${dataset}.log | $mytail -1 | nawk -Fstatus\\ - '{print \$2}'` 
	#				unless ($last_exec =~ /\//); # if last_exec has a / in it, it's a unix command like /usr/bin/mv
				chomp $exit_status if ( $exit_status );
				$exit_status = "?" unless ( $exit_status );									
			}
			elsif ( ( $osfstatus =~ /x/ ) && ( ! -e "${pipeline}/logs/${dataset}.log" ) ){		#	late arrival and arc_prep run?
				$e_count++;
				if  ( ( $pipeline =~ /adp/ ) && ( ( $dataset =~ /^(THF|TSF|revno|orbita|opp|pod|iop|pad)/ ) || ( $dataset  =~ /(OLF)$/ ) )
					|| ( $pipeline =~ /consssa/ ) 
					)  {
					$last_exec   = "  NO LOG!";
					$exit_status = "";
					$exit_status = "HAS ARC_PREP RUN?" if ( $pipeline =~ /adp/ );
				} else {
					#	SCREW 1559
					my $parsedrevno = substr ( $dataset, 0, 4);	#	doesn't work in ADP, SSA
					$last_exec   = "  NO LOG!"; # if ( -e "${pipeline}/logs/${parsedrevno}_arc_prep.log" );
	#				$last_exec   = "${parsedrevno}??" unless ( $last_exec );										
					$exit_status = "ARC_PREP HAS RUN!" if ( -e "${pipeline}/logs/${parsedrevno}_arc_prep.log" );
					$exit_status = "NO ${parsedrevno} ARC_PREP!" unless ( $exit_status );										
				}
			}
			else {
				if ( $osfstatus =~ /p/ ) { $p_count++; }
				elsif ( $osfstatus =~ /(w|v|s|g|-)$/ ) { $w_count++; }
				# count things marked for deletion as complete (if c before)
				elsif ( $osfstatus =~ /(c|cd|co)$/ ) { $c_count++; } 
				elsif ( $osfstatus =~ /(wd|wo)$/ ) {} # empty OLF/THFs in ADP, or junk
				else {  
					print "$prefix ERROR:  cannot figure out status $osfstatus\n"; 	
				}
				
			} # if not error
			
			#  Keep this separate, since it may be with an error, waiting, or 
			#   complete status in another column
			if ( $osfstatus =~ /(o|d)$/ ) { $d_count++; }
			
			if ( $command =~ /susp/ ) { $s_count++;}
			
			if ( ! ( $last_exec ) && ( $quiet ) ) {
				#  If told to be quiet, i.e. errors only, don't print entries which
				#     didn't have errors.
			}
			else {
				print "$prefix ";																	
				printf ( "%-23s","  Date: $cdate" );											
				#	printf ("%-40s","Dataset==${dataset}") unless ($do_pipeline =~ conssa);	#	040722 - Jake - SCREW 1523
				#	printf ("%-60s","Dataset==${dataset}") if ($do_pipeline =~ conssa);		#	040722 - Jake - SCREW 1523
				printf ( "%-44s"," OSF: $dataset" );										#	was 47 but seemed large
				printf ( "%-14s"," Stat: $osfstatus" );
				printf ( "%-17s"," Cmd: $command" ) unless ( $command =~ /____/ );	
				#	ls ~isdc_lib/archive/deliveries/ | gawk -F\- '{print $1}' | uniq | $mygrep -vs root_bin_linux_no_pthread | wc -L
				#	yields 22  ( ibis_science_analysis is 21 chars )
				printf ( "%-29s"," Last: $last_exec" ) if ( $last_exec );
				printf ( "%-19s"," Exit: $exit_status" ) if ( $last_exec );
				print "\n";
			}
		} # foreach OSF    
	
		print "$prefix "																			
			."Pipeline=$pipeline  "
			."Total==$total_count  "
			."Errors=$e_count  "
			."Processing=$p_count  "
			."Waiting=$w_count  "
			."Complete=$c_count "
			."Cleaning/Deletion=$d_count "
			."Suspended=$s_count \n" 
			if ( $total_count );
		
	}  # end of foreach pipeline
}	# end of foreach @opusworks

exit 0;

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

