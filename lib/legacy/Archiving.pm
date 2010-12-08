package Archiving;

=head1 NAME

Archiving.pm - Revolution File Pipeline Archiving Module

=head1 SYNOPSIS

use I<Archiving.pm>;

=head1 DESCRIPTION

This module contains two functions, one to check the given revolution for completeness (CheckRev()) and one to prepare a revolution already determined complete for archive ingest (RevArchiving()).  

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use File::Basename;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use OPUSLIB qw(:osf_stati);
use Datasets;

$| = 1;

########################################################################

=item B<CheckRev> ($revno)

Checks completeness of given revolution.  Writes arc_prep triger if all checks pass.  Returns 0 if *nothing* done, 1 if check passes.

=cut

sub CheckRev {
	my ($revno) = @_;
	my $nextrev = sprintf("%04d",$revno + 1);
	my $bad = 0;
	my $wait = 0;
	my $retval;
	my @nextrevscws;
	my @thisrevscws;
	my @list;
	my $scw;
	my $file;
	#########              set processing type:  NRT or CONS
	my $path = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "consrev" : "nrtrev";
	my $inst = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "cons"    : "nrt";
	
	########################################################################
	## Check that previous revolution is ready for archiving. 
	########################################################################
	
	print	"\n"
		."******************************************************************\n"
		."  Checking that revolution $revno has been triggered for archiving\n"
		."******************************************************************\n";
	
	##
	##  If we can write to it, it's neither in the archive nor already triggered.
	## 
	
	#  This is still OK if arcingest changes the permissions;  the reason is that
	#   this function is only called if there is no arc_prep trigger for the rev.
	#   Arcingest would only be messing with this if there *is* that trigger.
	#   So this is just to determine if the data are on the archive (write 
	#    protected) or on the repository and not yet triggered (writeable).  
	if (-w "$ENV{SCWDIR}/$revno/rev.000/") {
		print "*******     Revolution $revno still writeable;  checking contents\n";
	}
	else {
		print "*******     Revolution $revno not writeable (at $ENV{SCWDIR}/$revno/rev.000/);  "
			."must be archived already.  Quitting.\n";
		return 0;
	}
	
	if (`$myls $ENV{REV_INPUT}/${revno}_arc_prep.trigger* 2> /dev/null`) {
		print "*******     Trigger $ENV{REV_INPUT}/${revno}_arc_prep.trigger exists;  quitting.\n";
		return 0;
	}
	
	########################################################################
	# first, check next rev's scws;  no use if none exist yet (because
	#  this means PP isn't done writing them for the current perhaps)
	
	# for next rev, just need at least one closed by PP, meaning there's a 
	#  trigger for the Input pipeline
	
	# Look in triggers directory;  if archived scw data, there will be 
	#  no triggers for any revolution.  Otherwise, all should be there,
	#  and all should be correct.  
	print "*******     Looking in $ENV{OPUS_WORK}/${inst}input/input/$nextrev*\n";
	@nextrevscws = sort(glob("$ENV{OPUS_WORK}/${inst}input/input/$nextrev*"));
	print "*******     Found in $ENV{OPUS_WORK}/${inst}input/input/$nextrev*:\n*******     ".
		join("\n*******     ",@nextrevscws),"\n" if (@nextrevscws);
	
	print "*******     Looking in $ENV{OPUS_WORK}/${inst}scw/input/$revno*\n";
	@thisrevscws = sort(glob("$ENV{OPUS_WORK}/${inst}scw/input/$revno*"));
	print "*******     Found in $ENV{OPUS_WORK}/${inst}scw/input/$revno*:\n*******     ".
		join("\n*******     ",@thisrevscws),"\n" if (@thisrevscws);
	
	########################################################################
	# There must be science windows of this one;  otherwise, the scw data
	#   must be out of the archive
	#  
	if (@thisrevscws) {
		
		#  Check for a science window closed by PP in next revolution
		if (@nextrevscws) {
			# this means that PP has finished writing this science window, which
			#  automatically means all are closed from previous rev.  
			print "*******     At least one science window for revolution $nextrev finished by PP; "
				." checking status of revolution $revno science windows\n";
			
			#
			# now, check status of scw triggers.  
			($retval,@list) = &OPUSLIB::OSFstatus(
				"files" => "$ENV{SCW_INPUT}/$revno*",
				"path"  => "${inst}scw",
				);
			&Error ( "Rev: Cannot check status of ScW triggers:\n@list" ) if ($retval);

			########
			#  SCREW 1025:  allow some errors, though nothing waiting or processing
			########
			
			#  Count errors and things not done:
			foreach (@list) { 
				$bad++ if /x/;
				$wait++ if ( (/w|p|h|_/) && !(/x/));
			}
			print "*******     There are $bad errored science windows, "
				."and $wait science windows not yet finished.\n";
			
			#  Check if number of errors is under threshhold:
			if ( $bad <= $ENV{ARC_ALLOW_ERR_PERCENT_SCW} * $#list ) {
				print "*******     The $bad science windows with errors are "
					."within the allowed $ENV{ARC_ALLOW_ERR_PERCENT_SCW} "
					."percent of $#list total;  so can go ahead and archive. \n";
			}
			else {
				print "*******     The $bad science windows not completed are "
					."more than the allowed $ENV{ARC_ALLOW_ERR_PERCENT_SCW} "
					."percent of $#list total;  so cannot archive. \n";
				return 0;
				
			}
			
			#  No waiting/processing allowd:
			if ($wait) {
				print "*******     There are $wait science windows waiting or processing;  "
					."cannot archive yet.\n";
				return 0;
			}
			
		} # if nextrevscws
		
		else {
			print "*******     No science window for revolution $nextrev finished by PP; quitting\n";
			return 0;
		}
		
	} # end of if (@thisrevscws)
	
	else {  
		# if there are none in the current rev, then none were loaded 
		#  (and we're reprocessing)
		# 
		# Here, we know no triggers for current rev;  but make sure none
		#  are there at all.  Could be problem with input pipeline or
		#  with triggering, or something...
		
		if (glob("$ENV{OPUS_WORK}/${inst}input/input/*trigger*")) {
			print "*******     ERROR:  found no Input triggers for rev $revno, "
					."but other triggers found!  This is wierd.\n";
			return 0;
		}
		#
		#  So we print a warning if there are none here and none in either 
		#   rev;  this means we're reprocessing and they weren't loaded.
		print "*******      WARNING:  No science windows found for rev $revno;"
			."  this means we're probably reprocessing just the rev part,"
			." in which case we can continue with archiving.\n";
	}
	
	########################################################################
	# now check all previous revolution file OSFs
	#
	print "*******     Now checking rev.000 contents for $revno \n";
	$bad = 0;
	$wait = 0;
	
	#  Check things with triggers:
	if (`$myls $ENV{REV_INPUT}/$revno* 2> /dev/null`) {
		($retval,@list) = &OPUSLIB::OSFstatus(
			"files" => "$ENV{REV_INPUT}/$revno*",
			"path"  => "$path",
			);
		&Error ( "Rev: Cannot check status of rev file triggers:\n@list" ) if ($retval);
		
	} # end if triggers found
	else {
		print "*******     WARNING:  no OSFs found for revolution $revno;  quitting\n";
		return 0;
	}
	
	########
	#  SCREW 1025:  allow some errors, though nothing waiting or processing
	########
	
	#  Count errors and things not done:
	foreach (@list) { 
		$bad++ if /x/;
		$wait++ if ( (/w|p|h|_/) && !(/x/));
	}
	print "*******     There are $bad errored rev files, and $wait not yet finished.\n";
	
	
	#  Check if number of errors is under threshhold:
	if ( $bad <= $ENV{ARC_ALLOW_ERR_PERCENT_REV} * $#list ) {
		print "*******     The $bad rev files with errors are within the allowed "
			."$ENV{ARC_ALLOW_ERR_PERCENT_REV} "
			."percent of $#list total;  so can go ahead and archive. \n";
	}
	else {
		print "*******     The $bad rev files not completed are more than the allowed "
			."$ENV{ARC_ALLOW_ERR_PERCENT_REV} "
			."percent of $#list total;  so cannot archive. \n";
		return 0;
		
	}
	
	#  No waiting/processing allowd:
	if ($wait) {
		print "*******     There are $wait rev files waiting or processing;  cannot archive yet.\n";
		return 0;
	}
	
	if (`$myls $ENV{REV_INPUT}/${revno}_iii_prep.trigger* 2> /dev/null`) {				
		print "*******     Trigger $ENV{REV_INPUT}/${revno}_iii_prep.trigger exists;\n";	
		print "*******     Checking its status;\n";	
		#	060223 - Jake - SPR 4437

		( $retval, @list ) = &OPUSLIB::OSFstatus (
			"files" => "$ENV{REV_INPUT}/${revno}_iii_prep",
			"path"  => "$path",
			);

		print "*******     status is @list\n";	
		if ( ( @list == 1 ) && ( $list[0] =~ /^$osf_stati{REV_COMPLETE}$/ ) ) {
			#
			# Now, everything done, so write the trigger file to do the archiving, etc.
			#
			print "******    All rev files for $revno (including iii_prep) are completed;  "
				."now triggering ${revno}_arc_prep for cleaning, exposure status, and archiving\n";
			system ( "$mytouch $ENV{REV_INPUT}/${revno}_arc_prep.trigger" );
		} elsif ( @list >= 1 ) {
			print "******    Something is wrong.  There appear to be multiple iii_prep osfs?\n";
			print "******    with multiple stati: @list \n";
		} elsif ( @list <= 1 ) {
			print "******    Something may be wrong.  There appear to be no iii_prep osfs?\n";
		} else {
			print "******    ${revno}_iii_prep does not appear to be complete yet : $list[0]\n";
		}
	} else {
		print "******    All rev files for $revno are completed;  "
			."now triggering myself for ${revno}_iii_prep (SPI Calibration) from &Archiving::CheckRev\n";
		system ( "$mytouch $ENV{REV_INPUT}/${revno}_iii_prep.trigger" );
	}
	
	return 1;		#	FIX - 060223 - Jake - Why does this return a 1 and not a 0?
} ##                CheckRev DONE


#########################################################################

=item B<RevArchiving> ($revno)

This does the last steps for archiving a rev.  Cleans up indices, write protects recursively, writes ingest trigger

=cut

sub RevArchiving {
	my ($revno) = @_;
	my ($retval,@result);
	print "*******     Archiving revolution $revno\n";

	my $proc = &ProcStep();

	#########################################################################
	##  First, clean up indices
	#
	
	my @all_indices = sort(glob("$ENV{SCWDIR}/$revno/rev.000/*/*index.fits"));
	print "*******     Need to clean the following indices:\n",join("\n*******     ",@all_indices),"\n";
	my $i;
	my ($base,$path,$ext);
	my $oneindex;
	my @oneindices;
	my @stat;
	my $link_time;
	my $cur_time;
	my $diff;
	foreach $oneindex (@all_indices) {
		# for each type of index, clean
		($base,$path,$ext) = &File::Basename::fileparse($oneindex, '\..*');
		
		##  if this is a temporary index, named with <PID>temp_index.fits,
		##   i.e. one of the copies in place from Find/CollectIndex, then
		##   it just needs to be deleted, not cleaned. 
		if ($base =~ /\d{5}temp_index/) {
			print "******     WARNING:  found temp index to be deleted:\n $oneindex\n";
			unlink $oneindex;
		}
		
		@oneindices = sort(glob("$path/$base*.fits"));
		
		# check time on link;  if link was last updated long enough ago,
		#  it's save to delete previous versions;  otherwise, wait.
		@stat = stat("$oneindices[0]");
		# stat returns: (dev,ino,mode,nlink,uid,gid,rdev,size,atime,mtime,ctime,
		#                blksize,blocks), and we want mtime (modification), 9th
		$link_time = $stat[9];
		$cur_time = time;
		$diff = $cur_time - $link_time;
		if ($diff < $ENV{IDX_WAIT}) {
			print "*******     WAITING:  index $oneindices[0] recently modified;  waiting to clean.\n";
			sleep ($ENV{IDX_WAIT} - $diff);
		}
		
		# function to take root, remove old versions, and replace link with last
		&ISDCPipeline::LinkReplace(
			"root"   => "$base",
			"subdir" => "$path",
			"proc"   => "$proc",
			);
	} # foreach index
	
	
	#########################################################################
	##  check all contents for any unrecognized files;  if any junk found,
	##   error exit.
	&Datasets::RevContentsCheck($revno);
	
	#########################################################################
	##  lock recursively, which will include the directories.  

#	replace with 
	&UnixLIB::Gzip ( "$ENV{SCWDIR}/$revno/rev.000/*/*.fits" );
	
	($retval,@result) = &ISDCPipeline::RunProgram("$mychmod -R -w $ENV{SCWDIR}/$revno/rev.000");
	&Error ( "Problem write protecting rev $revno;  result @result" ) if ($retval);
	
	#########################################################################
	##  Next, write the trigger file
	
	print "******     Triggering archive ingest for revolution $revno\n";

	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ARC_TRIG}" ) unless ( -d "$ENV{ARC_TRIG}" );
	
	open(AIT,">$ENV{ARC_TRIG}/scw_${revno}rev0000.trigger_temp") or
	&Error ( "Cannot write trigger file $ENV{ARC_TRIG}/scw_${revno}rev0000.trigger_temp" );
	print AIT "$ENV{ARC_TRIG}/scw_${revno}rev0000.trigger SCW $ENV{OUTPATH}/scw/$revno/rev.000\n";
	close(AIT);
	
	($retval,@result) = &ISDCPipeline::RunProgram(
		"$mymv $ENV{ARC_TRIG}/scw_${revno}rev0000.trigger_temp $ENV{ARC_TRIG}/scw_${revno}rev0000.trigger");
	&Error ( "Cannot update $ENV{ARC_TRIG}/scw_${revno}rev0000.trigger" ) if ($retval);
	
	#########################################################################
	#   Clean blackboard
	
	if ($proc =~ /nrt/i) {
		#  Leaves errors, since now in NRT (SCREW 1025), this may be triggered
		#   despite errors.  Don't clean here in cons:
		&ISDCPipeline::BBUpdate(
			"match"     => "^${revno}_",
			"except"    => "arc_prep", 
			# Want to only match completed OSFs
			"matchstat" => "^$osf_stati{REV_COMPLETE}\$",
			# And if you use matchstat, you have to then specify fullstat
			"fullstat"  => "$osf_stati{REV_CLEAN}",
			);  
	}
	else {
		#  Instead, in cons, this tells the monitor to watch for when it's all
		#   archived, and only then clean
		($retval,@result) = &ISDCPipeline::RunProgram("$mytouch $ENV{REV_INPUT}/${revno}_arc.done");
		&Error ( "Cannot \'$mytouch $ENV{REV_INPUT}/${revno}_arc.done\':@result" ) if ($retval);
	}
	
	######################################################################### 
	#  Lastly, move arc trigger file, without logging.  
	($retval,@result) = &ISDCPipeline::RunProgram(
		"$mymv $ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_processing $ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_done"
		) if (-e "$ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_processing");
	($retval,@result) = &ISDCPipeline::RunProgram(
		"$mymv $ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_bad $ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_done"
		) if (-e "$ENV{REV_INPUT}/$ENV{OSF_DATASET}.trigger_bad");
	
	return;
	
} # sub RevArchiving


#########################################################################

=item B<OSMarc> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

OSMarc or OMC arc:  exposure status report generation, HK indexing, etc. 

=cut

sub OSMarc {
	
	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	##
	##  Last steps before archiving   
	##
	if (glob("$ENV{SCWDIR}/$revno/$revno*")) {
		&ISDCLIB::DoOrDie ( "$mymkdir -p $workdir/osm" ) unless ( -d "$workdir/osm" );
		
		my $scw_osm_idx = "$ENV{OUTPATH}/idx/scw/GNRL-SCWG-GRP-IDX.fits";
		if (-e "$scw_osm_idx") {
			&ISDCPipeline::FindIndex(
				"index"    => "$scw_osm_idx",
				"workname" => "working_osmscws_index.fits",
				"select"   => "\"REVOL==$revno\"",
				"sort"     => "OBTSTART",
				"subdir"   => "$workdir",
				);
			
			&ISDCPipeline::PipelineStep(
				"step"           => "$proc - Exposure Status",
				"program_name"   => "exposure_status",
				"par_Revolution" => "working_osmscws_index.fits[GROUPING]",
				"par_OutputFile" => "./osm/exposure_report.fits",
				"par_ParamName"  => "ACCEPTED__EVENTS",
				"par_Instrument" => "JMX",
				"subdir"         => "$workdir",
				);
			
			&ISDCPipeline::WriteAlert(
				"step"    => "$proc - ECS generated;  alerting operators",
				"id"      => 700,
				"level"   => 2,
				"message" => "ECS report $revno generated by Rev File pipeline",
				);
			
		} # end if scws index
		
		else {
			&Error ( "Cannot find osmscws index $scw_osm_idx" );
		}
		
		unlink(glob("working*"));
	}
	else {
		&Message ( "WARNING:  no science windows found;  "
			."skipping OSM exposure status generation" );
	}  # end of if science windows
	
	if (-w "$ENV{SCWDIR}/$revno/rev.000/osm/hk_averages.fits") {
		&ISDCPipeline::PipelineStep(
			"step"         => "$proc - Clean HK averages group",
			"program_name" => "swg_clean",
			"par_object"   => "hk_averages.fits[1]",
			"par_showonly" => "no",
			"subdir"       => "$ENV{SCWDIR}/$revno/rev.000/osm/",
			);
	}
	else {
		print "*******     WARNING:  $ENV{SCWDIR}/$revno/rev.000/osm/hk_averages.fits not writable;  "
			."skipping cleaning.\n";
	}
	
	#  Also in this step, before finish step indexes it, need to write protect
	#   the HK averages (which are currently only thing in osm subdir):
	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - write protect HK averages",
		"program_name" => "$mychmod -w hk_averages.fits *avr* *avg*",
		"subdir"       => "$ENV{SCWDIR}/$revno/rev.000/osm",
		) if (`$myls $ENV{SCWDIR}/$revno/rev.000/osm/*av*.fits 2> /dev/null`);
	
} # end sub OMCarc, actually OSMarc

###########################################################################

=item B<RawRemoval> ( $proc, $stamp, $workdir, $osfname, $dataset, $type, $revno, $prevrev, $nexrev )

detach and delete RAW structures that have been replaced by ALLs from swg_raw.fits
and swg_raw.fits as well

=cut

sub RawRemoval {

	my ($proc,$stamp,$workdir,$osfname,$dataset,$type,$revno,$prevrev,$nexrev) = @_;
	my $grpfile = "swg_raw.fits";
	my $scwid = "";

	&Message ( "Starting the Raw Removal process." );

	foreach $scwid ( `$myls -d $ENV{SCWDIR}/$revno/$revno*` ) {
		chomp $scwid;

		next unless ( -r "$scwid/$grpfile" );
		next unless ( -r "$scwid/swg.fits" );

		&ISDCPipeline::RunProgram( "$mychmod +w $scwid/$grpfile" );
		&ISDCPipeline::RunProgram( "$mychmod +w $scwid" );
		&ISDCPipeline::RunProgram( "$mychmod -R +w $scwid/raw" );
		&ISDCPipeline::RunProgram( "$myrm -rf $scwid/raw" );
		&ISDCPipeline::RunProgram( "$myrm     $scwid/$grpfile" );
		&ISDCPipeline::RunProgram( "$mychmod -w $scwid" );
	}

	&Message ( "Done with the Raw Removal process." );

}	#	end sub RawRemoval

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
