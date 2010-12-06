package CRVLIB;

=head1 NAME

CRVLIB.pm - CONS Revolution Library

=head1 SYNOPSIS

use I<CRVLIB.pm>;

=head1 DESCRIPTION

=cut

use strict;
use File::Basename;
use ISDCPipeline;
use UnixLIB;
use OPUSLIB;
use TimeLIB;
use SSALIB;

my $retval;
my @result;
my @list;

sub CheckPipeline;		#	Generic single pipeline check
sub CheckIngested;		#	Generic archive completion check
sub CheckClean;
sub StartSSA;

$| = 1;


##########################################################################

=back

=head1 SUBROUTINES

=over

=cut

##########################################################################

=item B<CheckIngested> ( $pipeline, $revno, $filesuffix )

This function checks that all scw for revolution have matching scw_0268010500300000.trigger_20050126143544.COMPLETED
This function also checks that the necessary rev files have been archived.  Which ones?

DEFINITION:
Ingested - no files from the given revolution and pipeline exist in 
OPUS_MISC_REP:/trigger/$pipeline or OPUS_MISC_REP:/trigger/ingest

ENV.ARC_TRIG = arcingest               ! where the pipeline writes them
ENV.ARC_TRIG_INGESTING = arcingesting  ! where archingest gets them
ENV.ARC_TRIG_DONE = archingest_done    ! where they're moved when ingested 
arcingest              = OPUS_MISC_REP:/trigger/cons_rev
		also cons_scw and cons_ssa
arcingesting           = OPUS_MISC_REP:/trigger/ingest
archingest_done        = OPUS_MISC_REP:/log/ingest/trigger.COMPLETED

=cut

sub CheckIngested {
	my ( $pipeline, $revno, $filesuffix ) = @_;


	print "\n\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
		.">>>>>>>     CheckIngested ( $pipeline, $revno, $filesuffix )\n"
		.">>>>>>>     ".&TimeLIB::MyTime()."  Checking Ingested status of rev $revno in $pipeline Pipeline\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";

	print "Checking for archive triggers for $pipeline $revno outside OPUS_MISC_REP:/log/ingest/trigger.COMPLETED\n";

	my $undonetriggers;
	if ( $pipeline =~ /consscw/ ) {
		#	cons_scw/scw_0268010500300000.trigger_20050126143544.COMPLETED
		#	cons_rev/scw_0182rev0000.trigger
		$undonetriggers  = `$myls $ENV{OPUS_MISC_REP}/trigger/cons_rev/scw_$revno* 2> /dev/null`;
		$undonetriggers .= `$myls $ENV{OPUS_MISC_REP}/trigger/cons_scw/scw_$revno* 2> /dev/null`;
		$undonetriggers .= `$myls $ENV{ARC_TRIG_INGESTING}/scw_$revno*   2> /dev/null`;
	}
	elsif ( $pipeline =~ /consssa/ ) {
		#	css_sssp_0025007700100000.trigger_TODAYSDATE.COMPLETED
		$undonetriggers  = `$myls $ENV{OPUS_MISC_REP}/trigger/cons_ssa/css_ss*_$revno* 2> /dev/null`;
		$undonetriggers .= `$myls $ENV{ARC_TRIG_INGESTING}/css_ss*_$revno*   2> /dev/null`;
	}
	elsif ( $pipeline =~ /conssma/ ) {
		#	css_smsp_0145005500100000.trigger			??????
		$undonetriggers  = `$myls $ENV{OPUS_MISC_REP}/trigger/cons_ssa/css_sm*_$revno* 2> /dev/null`;
		$undonetriggers .= `$myls $ENV{ARC_TRIG_INGESTING}/css_sm*_$revno*   2> /dev/null`;
	}

	if ( $undonetriggers ) {
		print "Found uningested $pipeline triggers for $revno:\n";
		print "$undonetriggers";
		print "NOT touching $ENV{REV_INPUT}/${revno}_$filesuffix\n";
	} else {
		print "Found no archive triggers for $pipeline $revno outside OPUS_MISC_REP:/log/ingest/trigger.COMPLETED\n";
#		print "touching $ENV{REV_INPUT}/${revno}_$filesuffix\n";	#	unnecessary
#		`$mytouch "$ENV{REV_INPUT}/${revno}_$filesuffix"`;		#	unnecessary

		if ( $pipeline =~ /consscw/ ) {
			print "Checking for the existance of science windows for $revno.\n";
			print "No science windows found for $revno.\n" && return
				unless ( `$myls -d $ENV{REP_BASE_PROD}/scw/$revno/$revno* 2> /dev/null` );
			&StartSSA ( $revno );
		}

		elsif ( $pipeline =~ /consssa/ ) {
			print "Checking for the existance of ssa science windows for $revno.\n";
			print "No ssa science windows found for $revno.\n" && return
				unless ( `$myls -d $ENV{REP_BASE_PROD}/scw/$revno/$revno* 2> /dev/null` );
			print "Starting conssma for $revno.\n";

			if ( -e "$ENV{ISDC_OPUS}/consssa/cons_sma_start.sh" ) {
				print "Running \`$ENV{ISDC_OPUS}/consssa/cons_sma_start.sh $revno\`.\n";
				#	MUST unset COMMONLOGFILE because otherwise uses Log_1 and other stuff as science window IDs
				system ( "unset COMMONLOGFILE; $ENV{ISDC_OPUS}/consssa/cons_sma_start.sh $revno" );
				print "$ENV{ISDC_OPUS}/consssa/cons_sma_start.sh $revno failed:$?" && return if ( $? );
			} else {
				print "$ENV{ISDC_OPUS}/consssa/cons_sma_start.sh does not exist so can't run it\n";
			}

			#	$ENV{SMA_INPUT}/002401234567_isgri.trigger_done
			if ( `$myls $ENV{SMA_INPUT}/$revno*  2> /dev/null` ) {
				foreach ( `$myls $ENV{SMA_INPUT}/$revno*` ) {
					chomp;
					unless ( -z $_ ) {
						print "Found : \n$_\n";
						print "conssma for $revno started.\n";
						print "touching $ENV{REV_INPUT}/${revno}_sma.started";
						`$mytouch "$ENV{REV_INPUT}/${revno}_sma.started"`;
						last;	#	at least one trigger is created and being that I don't know how many should've been ...
					}
				}
			} else {
				print "NO TRIGGERS CREATED for conssma $revno!  NOT STARTED!.\n";
				print "NOT touching $ENV{REV_INPUT}/${revno}_sma.started";
			}
		}

		elsif ( $pipeline =~ /conssma/ ) {
			&CheckClean( $revno );
		}

	}

	return;
}	#	CheckIngested


##########################################################################

=item B<CheckPipeline> ( $pipeline, $revno, $input, $osfok, $filesuffix )

Generic Pipeline Check

CheckPipeline ( pipeline, revno, triggerlocation,        OSF-OK-Status,         filenamesuffix   );
CheckPipeline (  consscw,  0234, $ENV{SCW_INPUT}, $osf_stati{SCW_DP_C_COR_H}, "iii_prep.trigger" );

=cut

sub CheckPipeline {
	my ( $pipeline, $revno, $input, $osfok, $filesuffix ) = @_;
	my $bad = 0;
	# >>>>>>>     CheckPipeline ( consssa, 0465, /isdc/cons/ops_3/shaun/run/pipelines/cons/consssa/input, ccc, ssa.done )
	print "\n\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
		.">>>>>>>     CheckPipeline ( $pipeline, $revno, $input, $osfok, $filesuffix )\n"
		.">>>>>>>     ".&TimeLIB::MyTime()."  Checking status of rev $revno in $pipeline Pipeline\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
	#  This function takes all the triggers and gets their status on the BB.
	#   (This is better than using BBUpdate because there may be a trigger
	#   not picked up yet if you're unlucky.  This function will retun a
	#   null status of "_" for that case.)
	my $osfpath = ( $pipeline =~ /conssma/ ) ? "consssa" : "$pipeline";
	my $osfmatch = "";
	$osfmatch = "-sma-" if ( $pipeline =~ /conssma/ );
	$osfmatch = "-ssa-" if ( $pipeline =~ /consssa/ );
	# Enable nasty hack to allow match on -scw- type instead. When automatically
	# starting SSA after rev pipeline, OSFs are created with scw as type on BB. Checking
	# uses -ssa- so this means that BBnever cleaned. FOrce this by using -scw- for SSA 
	#pipeline checking:
	if ( ($ENV{NASTY_REV3_SSA_HACK}) && ($pipeline =~ /consssa/) ) {
       print "- NASTY hack in operation.....using \"-scw-\" as match string for glob.\n", if ($ENV{DEBUGIN});
	   $osfmatch = "-scw-";
	} 
	($retval,@list) = &OPUSLIB::OSFstatus (
		"files" => "$input/$revno*trigger*",
		"path"  => "$osfpath",
		"match" => "$osfmatch",
		);
        
	&ISDCPipeline::PipelineStep(
		"step"         => "Rev ERROR",
		"program_name" => "ERROR",
		"error"        => "Cannot check status of $pipeline triggers:\n@list\n",
		) if ($retval);

	#  Count the number which aren't ready:
	foreach ( @list ) {  $bad++ unless /^$osfok$/;   } 
        
	#  Do nothing if there are no osfs for consssa and conssma:
	if  ( ( $#list <= 0 ) && ( $pipeline =~ /consssa|conssma/ ) ) {
		print ">>>>>>>     Found 0 OSFs.\n";
		return;
	}
	#  Do nothing if there are any:
	elsif ($bad > 0) {
		print ">>>>>>>     There are $bad OSFs not done yet in the $pipeline pipeline.  "
			."Not ready to write ${revno}_$filesuffix yet.\n";
		return;
	} else {
		if ( ( $pipeline =~ /consscw/ ) && ( $osfok =~ /$osf_stati{SCW_DP_C_COR_H}/ ) ) {
			#	This isn't really necessary as nothing checks for scwdp.done
			($retval,@result) = &ISDCPipeline::RunProgram("$mymv $ENV{REV_INPUT}/${revno}_scwdp.started $ENV{REV_INPUT}/${revno}_scwdp.done");
			die "*******     ERROR:  cannot \'$mymv $ENV{REV_INPUT}/${revno}_scwdp.started $ENV{REV_INPUT}/${revno}_scwdp.done\':  @result" if ($retval);
		}
		elsif ( $pipeline =~ /consssa/ ) {
			#	This isn't really necessary as below touches the ssa.done file (just leaves a ssa.started)
			($retval,@result) = &ISDCPipeline::RunProgram("$mymv $ENV{REV_INPUT}/${revno}_ssa.started $ENV{REV_INPUT}/${revno}_ssa.done");
			die "*******     ERROR:  cannot \'$mymv $ENV{REV_INPUT}/${revno}_ssa.started $ENV{REV_INPUT}/${revno}_ssa.done\':  @result" if ($retval);

			&ISDCPipeline::RunProgram (
				"$mymv $ENV{OPUS_MISC_REP}/trigger/cons_ssa/css_ss*_$revno* $ENV{ARC_TRIG_INGESTING}/" 
				);
		}
		elsif ( $pipeline =~ /conssma/ ) {
			#	This isn't really necessary as below touches the sma.done file (just leaves a sma.started)
			($retval,@result) = &ISDCPipeline::RunProgram("$mymv $ENV{REV_INPUT}/${revno}_sma.started $ENV{REV_INPUT}/${revno}_sma.done");
			die "*******     ERROR:  cannot \'$mymv $ENV{REV_INPUT}/${revno}_sma.started $ENV{REV_INPUT}/${revno}_sma.done\':  @result" if ($retval);

			&ISDCPipeline::RunProgram (
				"$mymv $ENV{OPUS_MISC_REP}/trigger/cons_ssa/css_sm*_$revno* $ENV{ARC_TRIG_INGESTING}/" 
				);
		}
		elsif ( $pipeline =~ /consinput/ ) {		#	&& ( $osfok =~ /$osf_stati{INP_COMPLETE}/ ) ) {
			&ISDCPipeline::BBUpdate(
				"path"      => "consrev",
				"match"     => "^${revno}_",
				"matchstat" => "^$osf_stati{REV_GEN_H}\$",
				"fullstat"  => "$osf_stati{REV_ST_C}",
				);
		}
		elsif ( $pipeline =~ /consrev/ ) {
			&ISDCPipeline::BBUpdate(
				"path"      => "consscw",
				"match"     => "^$revno",
				"matchstat" => "^$osf_stati{SCW_DP_H}\$",
				"fullstat"  => "$osf_stati{SCW_DP_W_COR_H}",
			);
		}

		#	The following isn't always necessary, but ...
		($retval,@result) = &ISDCPipeline::RunProgram("$mytouch $ENV{REV_INPUT}/${revno}_$filesuffix");
		die "*******     ERROR:  cannot \'$mytouch $ENV{REV_INPUT}/${revno}_$filesuffix\':  @result" if ($retval);
	} # end if all completed

	return;
}		#	CheckPipeline


##########################################################################

=item B<CheckClean> ( $revno )

All processing is done, including arc_prep, so archive ingest trigger
has already been written.  Wait for archiving ingest to finish and
move that trigger to completed dir, then clean blackboards

=cut

sub CheckClean {
	
	my ($revno) = @_;
	
	print "\n\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
		.">>>>>>>     ".&TimeLIB::MyTime()."  CheckClean : Checking Cleaning status of rev $revno\n"
		.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
	
	#########
	#  First check for the rev trigger:
	#########
	if (-e "$ENV{REV_INPUT}/${revno}_arc_prep.trigger_done") {
		# This means we haven't cleaned yet, so check:
		print ">>>>>>>    Checking whether completed revolution $revno has been successfully archived.\n";
		
		#  Don't know what it will rename it, so just look for anything:
		#    if (glob "$ENV{ARC_TRIG_DONE}/scw_${revno}rev0000.trigger*") {
		if (`$myls $ENV{ARC_TRIG_DONE}/scw_${revno}rev0000.trigger* 2> /dev/null`) {
			
			print ">>>>>>>     Found Arc Ingest trigger under $ENV{ARC_TRIG_DONE};  cleaning blackboards.\n";
			
			&ISDCPipeline::BBUpdate(
				"match" => "^${revno}_",
				"path"  => "consrev",		#	this is kinda unnecessary as the match will only match rev files
												#	but maybe it'll speed things up. (050810)
				);
			
		} else {
			print ">>>>>>>       Did NOT find Arc Ingest trigger under $ENV{ARC_TRIG_DONE};  "
				."not cleaning revolution $revno yet.\n";
			print "#######     DEBUG:  command \'$myls $ENV{ARC_TRIG_DONE}/scw_${revno}rev0000.trigger*\' gives:\n"
				.`$myls $ENV{ARC_TRIG_DONE}/scw_${revno}rev0000.trigger*`."\n";
			
		}
		
	} # If arc_prep.trigger_done still there
	
	#########
	#  Now, check for any science windows still there:
	#########
	print ">>>>>>>     Looking for $ENV{SCW_INPUT}/$revno*.trigger_done science windows to check\n";
	foreach (glob ("$ENV{SCW_INPUT}/$revno*.trigger_done") ) {
		
		my ($scw) = &File::Basename::fileparse($_,'\..*');
		
		print ">>>>>>>     Checking whether completed science window $scw has been successfully archived.\n";
		
		#  Don't know what it will rename it, so just look for anything:
		if (`$myls $ENV{ARC_TRIG_DONE}/scw_${scw}0000.trigger* 2> /dev/null`) {
			print ">>>>>>>     Found $scw trigger under $ENV{ARC_TRIG_DONE};  cleaning.\n";
		} # end if trigger found in completed directory
		else {
			print ">>>>>>>     Did NOT find $scw trigger under $ENV{ARC_TRIG_DONE};  not cleaning yet.\n";
			next;
		}
		
		#  Only one to do, but use function for checks:
		&ISDCPipeline::BBUpdate(
			"match" => "^$scw\$",
			"path"  => "consscw",
			);
		&ISDCPipeline::BBUpdate(
			"match" => "^$scw\$",
			"path"  => "consinput",
			);
		
	} # end foreach scw not cleaned yet
	
	my $triggerlist;
	$triggerlist  = `$myls $ENV{SCW_INPUT}/$revno*trigger* 2> /dev/null`;
	$triggerlist .= `$myls $ENV{REV_INPUT}/$revno*trigger* 2> /dev/null`;
	$triggerlist .= `$myls $ENV{INP_INPUT}/$revno*trigger* 2> /dev/null`;

	if ( $ENV{AUTO_CLEAN_SSA} ) {
		#	print ">>>>>>>     Looking for $ENV{SSA_INPUT}/ss??_$revno*.trigger_done SSA science windows to check\n";
		#	foreach ( glob ("$ENV{SSA_INPUT}/ss??_$revno*.trigger_done" ) ) {
		print ">>>>>>>     Looking for $ENV{SSA_INPUT}/$revno*_*.trigger_done SSA science windows to check\n";
		foreach ( glob ("$ENV{SSA_INPUT}/$revno*_*.trigger_done" ) ) {
			next unless ( -z $_ );

			my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $_ );

			print ">>>>>>>     Checking whether completed SSA science window $osfname has been successfully archived.\n";

			#  Don't know what it will rename it, so just look for anything:
			#if ( `$myls $ENV{ARC_TRIG_DONE}/css_ss??_${scw}0000.trigger* 2> /dev/null` ) {
			if ( `$myls $ENV{ARC_TRIG_DONE}/css_${osfname}0000.trigger* 2> /dev/null` ) {
				print ">>>>>>>     Found $osfname SSA trigger under $ENV{ARC_TRIG_DONE};  cleaning.\n";
			} # end if trigger found in completed directory
			else {
				print ">>>>>>>     Did NOT find $osfname SSA trigger under $ENV{ARC_TRIG_DONE};  not cleaning yet.\n";
				next;
			}

			&ISDCPipeline::BBUpdate(
				"match" => "^$osfname\$",
				"path"  => "consssa",
				);
		}
		$triggerlist .= `$myls $ENV{SSA_INPUT}/$revno*_*.trigger* 2> /dev/null`;
		#$triggerlist .= `$myls $ENV{SSA_INPUT}/ss??_$revno*trigger* 2> /dev/null`;
	}

	if ( $ENV{AUTO_CLEAN_SMA} ) {
		#	print ">>>>>>>     Looking for $ENV{SMA_INPUT}/sm??_$revno*.trigger_done SMA science windows to check\n";
		#	foreach ( glob ("$ENV{SMA_INPUT}/sm??_$revno*.trigger_done" ) ) {
		print ">>>>>>>     Looking for $ENV{SMA_INPUT}/$revno*_*.trigger_done SMA science windows to check\n";
		foreach ( glob ("$ENV{SMA_INPUT}/$revno*_*.trigger_done" ) ) {
			next if ( -z $_ );

			my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $_ );

			print ">>>>>>>     Checking whether completed SMA science window $osfname has been successfully archived.\n";

			#  Don't know what it will rename it, so just look for anything:
			#if ( `$myls $ENV{ARC_TRIG_DONE}/css_ss??_${scw}0000.trigger* 2> /dev/null` ) {
			if ( `$myls $ENV{ARC_TRIG_DONE}/css_${osfname}0000.trigger* 2> /dev/null` ) {
				print ">>>>>>>     Found $osfname SMA trigger under $ENV{ARC_TRIG_DONE};  cleaning.\n";
			} # end if trigger found in completed directory
			else {
				print ">>>>>>>     Did NOT find $osfname SMA trigger under $ENV{ARC_TRIG_DONE};  not cleaning yet.\n";
				next;
			}

			&ISDCPipeline::BBUpdate(
				"match" => "^$osfname\$",
				"path"  => "consssa",
				);
		}
		$triggerlist .= `$myls $ENV{SMA_INPUT}/$revno*_*.trigger* 2> /dev/null`;
		#$triggerlist .= `$myls $ENV{SMA_INPUT}/sm??_$revno*trigger* 2> /dev/null`;
	}

	#########
	#  Check if it's all cleaned up, and remove all the .done files
	#########
	
	if ( $triggerlist ) {
		print ">>>>>>>     Found something still existing for revolution $revno.  "
			."triggerlist : \n$triggerlist\n"
			."Can't stop checking archive ingest progress.\n";
	}
	else {
		print ">>>>>>>     Found NOTHING existing for revolution $revno;  all must be cleaned up.  "
			."Can now stop checking.  Removing all ${revno}_*.done\n";

		#	050125 - Jake - noticed that scwdp wasn't in list (probably removed in cleanup step)
		#	050218 - Jake - added "ingest","ssa","ssa_ingest","sma","sma_ingest"
		#  050307 - Jake - added "sa", "sa_ingest" ( SCREW 1647 for these and those above )
		foreach ("pp","inp","rev","scwdp","arc","ingest","sa","sa_ingest","ssa","ssa_ingest","sma","sma_ingest") { 
			if ( -e "$ENV{REV_INPUT}/${revno}_${_}.done" ) {
				unlink "$ENV{REV_INPUT}/${revno}_${_}.done" 
					or die ">>>>>>     ERROR:  cannot unlink $ENV{REV_INPUT}/${revno}_${_}.done";
				die ">>>>>>     ERROR:  did not unlink $ENV{REV_INPUT}/${revno}_${_}.done"
					if ( -e "$ENV{REV_INPUT}/${revno}_${_}.done" );
			}
		}
		foreach ( "scwdp", "sa", "ssa", "sma" ) { 
			if ( -e "$ENV{REV_INPUT}/${revno}_${_}.started" ) {
				unlink "$ENV{REV_INPUT}/${revno}_${_}.started" 
					or die ">>>>>>     ERROR:  cannot unlink $ENV{REV_INPUT}/${revno}_${_}.started";
				die ">>>>>>     ERROR:  did not unlink $ENV{REV_INPUT}/${revno}_${_}.started"
					if ( -e "$ENV{REV_INPUT}/${revno}_${_}.started" );
			}
		} # 050307 - Jake - SCREW 1647
		
	}
	
	# At this point we could clean up the copied images for the rev3 pixelisation:
	map {
        if (-f "$ENV{REV_INPUT}/rev3_pixels_${_}_${revno}.done") {
	       print "PIXELISATION: Apparently, $_ has finished and can be cleaned.\n";
	       print "              can clean $ENV{REP_BASE_PROD}/pixels/$_/$revno\n", if (-d "$ENV{REP_BASE_PROD}/pixels/$_/$revno");
	    }
	} ('isgri','jmx');
	return;
	
}  # end CheckClean
##########################################################################


=item B<StartSSA>

=cut

sub StartSSA {

	my ( $revno ) = @_;
	print "Starting consssa for $revno.\n";

	print "Running \`$ENV{ISDC_OPUS}/consssa/cons_ssa_start.sh --instruments \"$ENV{SSA_TRIG_SELECTED_INSTRUMENTS}\" $revno\`.\n";
	#	MUST unset COMMONLOGFILE because otherwise uses Log_1 and other stuff as science window IDs
	system ( "unset COMMONLOGFILE; $ENV{ISDC_OPUS}/consssa/cons_ssa_start.sh --instruments \"$ENV{SSA_TRIG_SELECTED_INSTRUMENTS}\" $revno" );
	print "$ENV{ISDC_OPUS}/consssa/cons_ssa_start.sh --instruments \"$ENV{SSA_TRIG_SELECTED_INSTRUMENTS}\" $revno failed:$?" && return if ( $? );

	if ( `$myls $ENV{SSA_INPUT}/$revno*  2> /dev/null` ) {
		foreach ( `$myls $ENV{SSA_INPUT}/$revno*` ) {
			chomp;
			if ( -z $_ ) {
				print "consssa for $revno started.\n";
				print "touching $ENV{REV_INPUT}/${revno}_ssa.started";
				`$mytouch "$ENV{REV_INPUT}/${revno}_ssa.started"`;
				last;	#	at least one trigger is created and being that I don't know how many should've been ...
			}
		}
	} else {
		print "NO TRIGGERS CREATED for consssa $revno!  NOT STARTED!.\n";
		print "NOT touching $ENV{REV_INPUT}/${revno}_ssa.started";
	}

	return;
}	#	end StartSSA

##########################################################################

1;

__END__ 

=back

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=item B<>


=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <jake.wendt@obs.unige.ch>

=cut

