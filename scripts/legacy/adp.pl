#!/usr/bin/perl

=head1 NAME

adp.pl - ISDC Auxiliary Data Preparation Pipeline.

=head1 SYNOPSIS

B<adp.pl> - Run from within B<OPUS>.  This is the second step of a
three step B<OPUS> pipeline.  The first step is B<adpst.pl> and the
last step is B<adpfin.pl>.

=head1 DESCRIPTION

I<adp.pl> is the script which runs the main part of the adp B<OPUS>
pipeline.  This is the second step of the three step ADP pipeline.

The purpose of this step is to properly process each type of ADP file
received.  The first step is to create a directory named the same as
the file in the WORKDIR location referenced in the I<adp.resource> and
move the file from ADP_INPUT (see I<adp.resource>) to this new
directory.  Then, depending on the file, process it according to the
list of ACTIONS below.

Additionally, when either an historic attitude or historic orbit file is created in a given revolution, that revolution is then triggered for archive ingest.  First, the contents of the revolution are checked for completeness.  If the attitude and orbit historic files are present, then the following are verified:  orbit_predicted, attitude_predicted, observation_log, time_correlation, attitude_snapshot.  If any are missing, an alert is sent and the revolution not triggered for ingest.  If that test passes, then the timeline_summary and POD files are compared;  if either is missing, or they do not have the same version numbers, an alert is sent.  If that test is passed, then the trigger file is written in the ARC_TRIG location defined in the resource file.  

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use File::Basename;
use TimeLIB;
use OPUSLIB;

$| = 1;

########################################################################
#########              PRELIMINARIES
########################################################################

##
##  machinations to get right environment from path/resource/env vars...
##
&ISDCPipeline::EnvStretch ( "OUTPATH", "ALERTS", "PARFILES", "ARC_TRIG", "ADP_INPUT", "WORKDIR", "AUXDIR", "LOG_FILES" );

##
##  Set dataset names, types, log files, etc.  
##
my $dataset  = $ENV{OSF_DATASET};
my $opuslink = "$dataset.log";
print "OPUS link is $opuslink\n";

&ISDCPipeline::PipelineStep (
	"step"         => &ProcStep()." - STARTING",
	"program_name" => "NONE",
	);

#
#  Everything had to be renamed, so we have to figure out the actual
#   name of the file from the OPUS dataset.  Basically, last _X was .X 
#
if ($dataset =~ /(.*)_(fits|ASF|PAF|OLF|INT|AHF|DAT|tar)/) {
	$dataset = "$1.$2";
}

my $olddataset = "$ENV{ADP_INPUT}/".$dataset."_processing";
my $newworkdir = "$ENV{WORKDIR}/$ENV{OSF_DATASET}/";
`$mymkdir -p $newworkdir`;
die "*******     ERROR:  cannot mkdir $newworkdir." if ($?);
my $newdataset = $newworkdir.$dataset;

if ( ($dataset =~ /(revno|orbita).*/) ) {
	$olddataset = "$ENV{ADP_INPUT}/$1.$1"."_processing";  
}

print "*******     olddataset is $olddataset\n";
# TSF is special
#
#  WARNING:  the following code is a disaster;  just try not to touch it!
#
if ($dataset =~ /TSF_([0-9]{4})_([0-9]{4}).*INT/) {
	# get the first one 
	# (already moved from $rev_xxxxxxxxx_$vers to $rev_$vers by adpst)
	print "got a TSF one\n";
	my $rev = $1;
	my $vers = $2;
	#  just as a placeholder (reset later):
	my @tdataset = glob ( "$ENV{ADP_INPUT}/TSF_$rev*$vers.INT_processing" );
	if (@tdataset) {
		print "@tdataset\n";
		$olddataset = $tdataset[0];
		#  use root name of olddataset to reset newdataset
		my ($newdataset,$path,$suffix) = &File::Basename::fileparse($olddataset, '\..*');
		# then set dataset without big xxxxxxxxx or path:
		$dataset = "$newdataset.INT";
		print "dataset is $dataset\n";
	} else {
		@tdataset = glob("$newworkdir/TSF_$rev*$vers.INT");
		print "looking for $newworkdir/TSF_$rev*$vers.INT\n";
		print "@tdataset\n";
		my ($newdataset,$path,$suffix) = &File::Basename::fileparse($tdataset[0], '\..*');
		$dataset = "$newdataset.INT";
		print "dataset is $dataset\n";
	}
}

# if dataset is in input directory, move it to workdir:
if (-e "$olddataset") {
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - moving dataset",
		"program_name" => "$mycp -p $olddataset $dataset; $myrm -f $olddataset",
		"subdir"       => "$newworkdir",
		);
} else {
	#  Otherwise we are re-running something.  Check for workdir, clean up
	#   anything except dataset, and start
	if (-e "$newworkdir/$dataset") {
		print "*******     Found $newworkdir/$dataset\n";
		my @junk = sort(glob("$newworkdir/*"));
		print "*******     Contents of workdir:\n",join("\n",@junk),"\n";
		my $rmstring;
		foreach (@junk) {
			$rmstring .= "$_ " unless (/$dataset$/);
		}
		print "*******     Removing:  $rmstring\n";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Cleanup old run",
			"program_name" => "$mychmod -R +w $rmstring; $myrm -rf $rmstring",
			"subdir"       => "$newworkdir",
			) if ( $rmstring );
		
	} # end of if (-e "$dataset")
	elsif ( $dataset =~ /arc_prep/ ) {
		# Don't need to keep this trigger or process it.
		unlink "$ENV{ADP_INPUT}/$dataset.trigger_processing";
	}
	else {
		# Otherwise, can't continue
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - ERROR",
			"program_name" => "ERROR",
			"error"        => "Cannot find input dataset $olddataset or $dataset",
			);
	}
} # end of if no olddataset

my $orgdir = "$ENV{AUXDIR}/org/";
my $adpdir = "$ENV{AUXDIR}/adp/";

foreach my $dir ( "$orgdir/ref", "$adpdir", "$ENV{ARC_TRIG}", "$ENV{ALERTS}" ) {
	`$mymkdir -p $dir`;
	die "*******     ERROR:  cannot mkdir $dir." if ($?);
}

SWITCH: {
	
	########################################################################
	# pad, ocs, iop
	
	if ($dataset =~ /(ocs|pad|iop)_([0-9]{2}).*/) {
		my $type = $1;
		my $ao   = "AO".$2;

		my $dirname = "$orgdir/$ao/";
		$dirname   .= "$type/" if ( $type =~ /ocs/ );
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a $type file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$dirname",
			"subdir"       => "$newworkdir",
			);
		
		# copy it to the adp directory as well
		$dirname  = "$adpdir/$ao/";
		$dirname .= "$type/" if ( $type =~ /ocs/ );
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a $type file to adp",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$dirname",
			"subdir"       => "$newworkdir",
			);
		
		## immediately trigger archive ingest for individual file
		my $trigger = "$ENV{ARC_TRIG}/aux_$dataset.trigger";
		$trigger =~ s/\.fits//;
		my $temptrigger = "$trigger"."_temp";
		open(AIT,">$temptrigger") 
			or die "*******     ERROR:  cannot open $temptrigger to write!";
		print AIT "$trigger AUX $adpdir/$ao/$dataset";
		close(AIT);
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write trigger for archive ingest",
			"program_name" => "$mymv $temptrigger $trigger",
			);
		
		last SWITCH;
	}

	if ($dataset =~ /pod_([0-9]{4})_(\d{4}).*/) {
		my $rev = $1;
		my $vers = $2;
		my @prevpods = glob("$orgdir/$rev/pod*");
		if (scalar(@prevpods) >= 1) {
			&ISDCPipeline::WriteAlert(
				"step"    => "adp - POD replan alert",
				"message" => "$dataset received after previous $prevpods[$#prevpods]\;  replan alert",
				"level"   => 2,
				"subdir"  => "$newworkdir",
				"id"      => "500",
				);
		}	  
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a pod file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgdir/$rev/",
			"subdir"       => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a pod file to adp",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$adpdir/$rev.000/",
			"subdir"       => "$newworkdir",
			);
		
		last SWITCH;
	}
	
	if ($dataset =~ /opp_([0-9]{4})_\d{4}_(\d{4}).*/) {
		my $rev  = $1;
		my $vers = $2;
		my @prevopps = glob("$orgdir/$rev/opp*");
		if (scalar(@prevopps) >= 1) {
			&ISDCPipeline::WriteAlert(
				"step"    => "adp - OPP replan alert",
				"message" => "$dataset received while $prevopps[$#prevopps] exists\;  replan alert",
				"level"   => 2,
				"subdir"  => "$newworkdir",
				"id"      => "500",
				);
		}	  
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Untar an opp file",
			"program_name" => "$mytar xvf $dataset",
			"subdir"       => "$newworkdir",
			);
		
		foreach my $oneFile ( glob("*.txt") ) {
			$oneFile =~ /.*opp_([0-9]{8})_([0-9]{4})_([0-9]{4}).*/;
			my $pointing = $1;
			my $catvers  = $2;
			my $version  = $3;
			my $boxPlan  =  "boxplan_$pointing"."_$version.fits";
			my $shotPlan = "shotplan_$pointing"."_$version.fits";
			&ISDCPipeline::PipelineStep (
				"step"           => "adp - convertopp on $oneFile to $boxPlan and $shotPlan",
				"program_name"   => "convertopp",
				"par_oppfile"    => "$oneFile",
				"par_shotplan"   => "$shotPlan",
				"par_boxplan"    => "$boxPlan",
				"par_catversion" => "$catvers",
				"subdir"         => "$newworkdir",
				);
		}
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset $newworkdir/*fits",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy the opp file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgdir/$rev/",
			"subdir"       => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy the opp files to adp",
			"program_name" => "COPY",
			"filename"     => "*.fits",
			"newdir"       => "$adpdir/$rev.000/",
			"subdir"       => "$newworkdir",
			);
		
		last SWITCH;
	}
	
	# either 2 or 4 digits as the version number
	if ($dataset =~ /([0-9]{4})_([0-9]{2})\.PAF/) {
		my $rev           = $1;
		my $vers          = $2;
		my $dirname       = "$orgdir/$rev/";
		my $adpDirname    = "$adpdir/$rev.000/";
		my $newworkdirrev = "$newworkdir/$rev.000/";
		my @prevpafs      = glob("$orgdir/$rev/*PAF");
		if (scalar(@prevpafs) >= 1) {
			
			$prevpafs[$#prevpafs] = &File::Basename::fileparse($prevpafs[$#prevpafs], '\..*');
			$prevpafs[$#prevpafs] .= ".PAF";
			
			&ISDCPipeline::WriteAlert(
				"step"    => "adp - PAF replan alert",
				"message" => "$dataset received while $prevpafs[$#prevpafs] exists\;  replan alert",
				"level"   => 2,
				"subdir"  => "$newworkdir",
				"id"      => "500",
				);
		}	  
		
		&ISDCPipeline::PipelineStep (
			"step"           => "adp - Convert PAF file",
			"program_name"   => "convertattitude",
			"par_Infile"     => "$dataset",
			"par_OutfilePRE" => "attitude_predicted_$vers.fits",
			"par_OutfileSNA" => "attitude_snapshot.fits",
			"par_OutfileHIS" => "attitude_historic.fits",
			"par_Outdir"     => "./",
			"subdir"         => "$newworkdir",
			);
		
		#  Must make sure a snapshot exists:
		my @snapshots = `$myls $adpDirname/attitude_snapshot*fits* 2> /dev/null`;
		@snapshots = `$myls $newworkdirrev/attitude_snapshot.fits* 2> /dev/null`
			unless (scalar(@snapshots) >0 );
		# Remember, now (@snapshots) will return 1!
		if (scalar(@snapshots) > 0) {
			print "*******     There are ".scalar(@snapshots)." snapshots already:\n".@snapshots."\n";
		} else {
			print "*******     There are no snapshots yet;  copying dummy.\n";
			my @dummys = sort(glob("$ENV{REP_BASE_PROD}/aux/adp/0000.000/attitude_snapshot*.fits*"));
			if ( ( -e "$dummys[$#dummys]" ) && ( $dummys[$#dummys] =~ /gz$/ ) ) {
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - copy gzipped dummy snapshot file",
					"program_name" => "$mycp $dummys[$#dummys] attitude_snapshot_0000.fits.gz",
					"subdir"       => "$newworkdirrev",
					"needfiles"    => 1,
					); 

				&UnixLIB::Gunzip ( "$newworkdirrev/attitude_snapshot_0000.fits.gz" );
			} 
			elsif ( -e "$dummys[$#dummys]" ) {
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - copy dummy snapshot file",
					"program_name" => "$mycp $dummys[$#dummys] attitude_snapshot_0000.fits",
					"subdir"       => "$newworkdirrev",
					"needfiles"    => 1,
					); 
			} 
			else {
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - ERROR",
					"program_name" => "ERROR",
					"error"        => "no snapshot attitude dummy found in $ENV{REP_BASE_PROD}/aux/adp/0000.000/",
					);				   
			}
		} # end if no snapshots on rep or here.

		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset $newworkdir/*/*fits*",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a PAF file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$dirname",
			"subdir"       => "$newworkdir",
			);

		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy attitude file from PAF to $rev",
			"program_name" => "COPY",
			"filename"     => "attitude_predicted_$vers.fits",
			"newdir"       => "$adpDirname",
			"subdir"       => "$newworkdirrev",
			"overwrite"    => 1,
			);
		
		if (-e "$newworkdirrev/attitude_snapshot_0000.fits") {
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Copy empty snapshot attitude file to $rev",
				"program_name" => "COPY",
				"filename"     => "attitude_snapshot_0000.fits",
				"newdir"       => "$adpDirname",
				"subdir"       => "$newworkdirrev",
				"overwrite"    => 1,
				); 
			
			&ISDCPipeline::LinkUpdate(
				"root"    => "attitude_snapshot",
				"subdir"  => "$adpdir/$rev.000",
				"ext"     => ".fits",
				);			       
		}
		
		last SWITCH;
	}
		
	if ($dataset =~ /([0-9]{4})_([0-9]{4})\.AHF/) {
		my $rev           = $1;
		my $newworkdirrev = "$newworkdir/$rev.000";
		
		&ISDCPipeline::PipelineStep (
			"step"           => "adp - Convert AHF file",
			"program_name"   => "convertattitude",
			"par_Infile"     => "$dataset",
			"par_OutfilePRE" => "attitude_predicted.fits",
			"par_OutfileSNA" => "attitude_snapshot.fits",
			"par_OutfileHIS" => "attitude_historic.fits",
			"par_Outdir"     => "./",
			"subdir"         => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset $newworkdir/*/*fits*",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a AHF file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgdir/$rev/",
			"subdir"       => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy attitude files from AHF to $rev",
			"program_name" => "COPY",
			"filename"     => "attitude_historic.fits",
			"newdir"       => "$adpdir/$rev.000/",
			"subdir"       => "$newworkdirrev",
			);
		
		# check if rev ready for archive ingesting
		&RevArcCheck("$rev");
		
		last SWITCH;
	}
	
	if ($dataset =~ /([0-9]{4})_([0-9]{4})\.ASF/) {
		my $rev           = $1;
		my $vers          = $2;
		my $adpDirname    = "$adpdir/$rev.000/";
		my $newworkdirrev = "$newworkdir/$rev.000";
		my @atts          = sort(glob("$adpDirname/attitude_predicted_*.fits*"));
		#  Don't give empty filename parameter, so check:
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy predicted attitude file from repository rev $rev",
			"program_name" => "COPY",
			"filename"     => "$atts[$#atts]",
			"newdir"       => "$rev.000",
			"subdir"       => "$newworkdir",
			"needfiles"    => 1,
			) if (@atts);

		my $filestocopy = "$adpDirname/attitude_snapshot.fits";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy last snapshot attitude file from repository rev $rev",
			"program_name" => "COPY",
			"filename"     => "$filestocopy",
			"newdir"       => "$rev.000",
			"subdir"       => "$newworkdir",
			"needfiles"    => 0,
			) if (-e "$filestocopy");

		&ISDCPipeline::RunProgram( "$mychmod +w $newworkdir/$rev.000/attitude_snapshot.fits*") and 
			&ISDCPipeline::PipelineStep (
				"step"         => "ERROR",
				"error"        => "Cannot execute command \'$mychmod +w $newworkdir/$rev.000/attitude_snapshot.fits*\':  $!\n",
				"program_name" => "ERROR",
				);
		
		my $timestamp = &TimeLIB::MyTime();
		$timestamp =~ s/:|-|T//g;
		
		&ISDCPipeline::RunProgram(
			"$mymv $newworkdir/$rev.000/attitude_snapshot.fits "
				."$newworkdir/$rev.000/attitude_snapshot_$timestamp"."_$vers.fits") and 
			&ISDCPipeline::PipelineStep (
				"step"         => "ERROR",
				"error"        => "Cannot execute command \'$mymv $newworkdir/$rev.000/attitude_snapshot.fits "
					."$newworkdir/$rev.000/attitude_snapshot_$timestamp"."_$vers.fits\':  $!\n",
				"program_name" => "ERROR",
				);
		
		&ISDCPipeline::PipelineStep (
			"step"           => "adp - Convert ASF file",
			"program_name"   => "convertattitude",
			"par_Infile"     => "$dataset",
			"par_OutfilePRE" => "attitude_predicted.fits",
			"par_OutfileSNA" => "attitude_snapshot_$timestamp"."_$vers.fits",
			"par_OutfileHIS" => "attitude_historic.fits",
			"par_Outdir"     => "./",
			"subdir"         => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset $newworkdir/*/*fits*",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy a ASF file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgdir/$rev/asf/",
			"subdir"       => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy attitude files from ASF to $rev",
			"program_name" => "COPY",
			"filename"     => "attitude_snapshot_$timestamp"."_$vers.fits",
			"newdir"       => "$adpDirname",
			"subdir"       => "$newworkdirrev",
			"overwrite"    => 2,
			);
		
		&ISDCPipeline::LinkUpdate(
			"root"    => "attitude_snapshot",
			"subdir"  => "$adpdir/$rev.000",
			"ext"     => ".fits",
			);			       
		
		last SWITCH;
	}
	
	if ($dataset =~ /orbita.*/) {
		#
		if ( $ENV{ADP_UNIT_TEST} =~ /TRUE/ ) {
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Convert an orbita file",
				"program_name" => "convertorbit",
				"par_Infile"   => "$dataset",
				"par_outfileP" => "orbit_predicted.fits",
				"par_OutfileH" => "orbit_historic.fits",
				"par_Outdir"   => "./",
				"par_SCID"     => "$ENV{SCID}",
				"par_RCD_ID"   => "321",
				"subdir"       => "$newworkdir",
				);
		} else {
			chomp ( my $date = `$mydate -u "+%Y-%m-%dT%H:%M:%S"` );
			
			my $cur_revno = &ISDCPipeline::ConvertTime(
				"informat"  => "UTC",
				"intime"    => "$date",
				"outformat" => "revnum",
				);
			
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Convert an orbita file",
				"program_name" => "convertorbit",
				"par_Infile"   => "$dataset",
				"par_outfileP" => "orbit_predicted.fits",
				"par_OutfileH" => "orbit_historic.fits",
				"par_Outdir"   => "./",
				"par_RevP"     => "$cur_revno",
				"par_SCID"     => "$ENV{SCID}",
				"par_RCD_ID"   => "321",
				"subdir"       => "$newworkdir",
				);
		}
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/*/orbit*fits* $newworkdir/$dataset",
			"stoponerror"  => 0,
			);			       
		
		foreach my $oneRev ( glob ( "$newworkdir/[0-9]*" ) ) {
			$oneRev = "$oneRev/orbit_historic.fits";
			
			print "$oneRev not found\n" && next unless ( -r "$oneRev" ) ;
			next unless ( -r "$oneRev" ) ;
			print "$oneRev found.  Processing ...\n";

			my @result = &ISDCLIB::GetColumn( "$oneRev"."[AUXL-ORBI-HIS]", "ORBIN" ) ;

			my $low;
			my $high;
			print "Searching dal_dump output for completeness.\n";
			foreach (@result) {
				chomp;
				next unless ( /^\s*[\d\.\+-E]+\s*$/ );
				$low  = $_ if ( !defined ($low)  || ( $_ < $low  ) );
				$high = $_ if ( !defined ($high) || ( $_ > $high ) );
			} 
			my $diff = $high - $low;
			&Error ( "$oneRev : $high - $low = $diff < ( 1 - $ENV{ORB_TOLERANCE} ) \n" ) if ( $diff < ( 1 - $ENV{ORB_TOLERANCE} ) );
			&Error ( "$oneRev : $high - $low = $diff > ( 1 + $ENV{ORB_TOLERANCE} ) \n" ) if ( $diff > ( 1 + $ENV{ORB_TOLERANCE} ) );
		}

		foreach my $oneRev ( glob ( "$newworkdir/[0-9]*" ) ) {
			my $tmprev = $oneRev;
			$tmprev =~ s/(\d{4}).*/$1/;
			&CheckRevTrigger($oneRev,$dataset);
			$oneRev = "$oneRev/orbit_historic.fits";
			$oneRev =~ /.*([0-9]{4})/;

			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Check rev $1 in repository",
				"program_name" => "CHECK",
				"filename"     => "$oneRev",
				"newdir"       => "$adpdir/$1.000/",
				"needfiles"    => 0,
				);
		}

		my $lastrev ;

		foreach my $oneRev ( sort ( glob ( "$newworkdir/[0-9]*" ) ) ) {
			$oneRev = "$oneRev/orbit_historic.fits";
			$oneRev =~ /.*([0-9]{4})/;
			$lastrev = $1;
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Copy $lastrev.000 to repository",
				"program_name" => "COPY",
				"filename"     => "$oneRev",
				"newdir"       => "$adpdir/$lastrev.000/",
				"subdir"       => "$newworkdir",
				"overwrite"    => 0,
				"needfiles"    => 0,
				);
		}
		foreach my $oneRev ( sort ( glob ( "$newworkdir/[0-9]*" ) ) ) {
			$oneRev = "$oneRev/orbit_predicted.fits";
			$oneRev =~ /.*([0-9]{4})/;
			$lastrev = $1;
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - Copy $lastrev.000 to repository",
				"program_name" => "COPY",
				"filename"     => "$oneRev",
				"newdir"       => "$adpdir/$lastrev.000/",
				"subdir"       => "$newworkdir",
				"overwrite"    => 2,
				"needfiles"    => 0,
				);
		}
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy an orbita file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgdir/ref/orbita/",
			"subdir"       => "$newworkdir",
			);
		
		# move the log file into the logs directory of the last rev processed
		`$mymkdir -p $adpdir/$lastrev.000/logs/` if (!(-d "$adpdir/$lastrev.000/logs/"));
		die "*******     ERROR:  cannot mkdir $adpdir/$lastrev.000/logs/." if ($?);
		&ISDCPipeline::MoveLog(
			"$adpdir/$dataset"."_log.txt",
			"$adpdir/$lastrev.000/logs/$dataset"."_log.txt",
			"$ENV{LOG_FILES}/$opuslink"
			);
		
		# check revolution completeness for archiving for each revolution that
		#  came out with an orbit_historic
		my @neworbhists = glob("$newworkdir/*/orbit_historic.fits*");
		print "revolutions to check are @neworbhists\n";
		foreach my $oneRev (@neworbhists) {
			$oneRev =~ /(\d{4})\.000/;
			$oneRev = $1;
			print "Checking rev $oneRev\n";
			&RevArcCheck("$oneRev");
		}
		
		last SWITCH;
	}
	
	if ($dataset =~ /revno_(.*)/) {
		my $date    = $1;
		my $dirname = "$orgdir/ref/";
		if (!(-e $dirname)) {
			`$mymkdir -p $dirname`;
			die "*******     ERROR:  cannot mkdir $dirname." if ($?);
		}
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Convert revno file",
			"program_name" => "convertrevolution",
			"par_Infile"   => "$dataset",
			"par_Outfile"  => "revolution_$date.fits",
			"subdir"       => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/*.fits* $newworkdir/$dataset ",
			);			       
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy an revno file to org",
			"program_name" => "COPY",
			"filename"     => "$newdataset",
			"newdir"       => "$orgdir/ref/revno/",
			"subdir"       => "$newworkdir",
			);
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - Copy revolution.fits",
			"program_name" => "COPY",
			"filename"     => "$newworkdir/revolution_$date.fits",
			"newdir"       => "$adpdir/ref/revno",
			"subdir"       => "$newworkdir",
			);
		
		## immediately trigger archive ingest		
		my $trigger = "$ENV{ARC_TRIG}/aux_revolution_$date.trigger";
		my $temptrigger = "$trigger"."_temp";
		open(AIT,">$temptrigger") or die "*******     ERROR:  cannot open $temptrigger to write!";
		print AIT "$trigger AUX $adpdir/ref/revno/revolution_$date.fits";
		close(AIT);
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write trigger for archive ingest",
			"program_name" => "$mymv $temptrigger $trigger",
			);
		
		last SWITCH;
	}

	if ($dataset =~ /TSF_([0-9]{4})_.*([0-9]{4})\.INT/) {
		my $rev = $1;
		my $version = $2;
		my @prevtsfs = glob("$orgdir/$rev/TSF*");
		if (scalar(@prevtsfs) >= 1) {
			$prevtsfs[$#prevtsfs] = &File::Basename::fileparse($prevtsfs[$#prevtsfs], '\..*');
			$prevtsfs[$#prevtsfs] =~ /TSF_[0-9]{4}_.*([0-9]{4})$/;
			#  Use only the previous version number.
			$prevtsfs[$#prevtsfs] = $1;
			&ISDCPipeline::WriteAlert(
				"step"    => "adp - TSF replan alert",
				"message" => "$dataset received while $prevtsfs[$#prevtsfs] exists\;  replan alert",
				"level"   => 2,
				"subdir"  => "$newworkdir",
				"id"      => "500",
				);
		}	  
		
		&ISDCPipeline::PipelineStep (
			"step"             => "adp - Convert TSF file",
			"program_name"     => "convertprogram",
			"par_Infile"       => "$dataset",
			"par_OutfileTCOR"  => "",
			"par_OutfileOOL"   => "",
			"par_OutfilePROGH" => "",
			"par_OutfilePROGP" => "timeline_summary_$version.fits",
			"par_Outdir"       => "./",
			"par_Toler"        => "$ENV{TSF_TOLERANCE}",
			"par_MOCAlert"     => "Summary",
			"subdir"           => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - copy POD files from repository to workdir",
			"program_name" => "COPY",
			"filename"     => "$adpdir/$rev.000/pod_${rev}_*.fits",
			"newdir"       => "$rev",
			"subdir"       => "$newworkdir",
			"needfiles"    => "1",
			);
		
		#  Get time stamp in first line of OLF file:
		chomp ( my $first_time = `$myhead -1 $dataset` );
		#  always very first line, no whitespace, then e.g.
		#  2002-06-24T07:30:12Z TIME_CORRELATION     3f75019c29ac0004 ...
		#
		$first_time =~ s/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z\s.*/$1/;
		#  This isn't used, but it has to be given something.  So if the first
		#  time isn't correct, don't error but put in junk:
		$first_time = &TimeLIB::MyTime() unless ($1);
		&ISDCPipeline::PipelineStep (
			"step"                    => "adp - Program Definition File",
			"program_name"            => "createpdef",
			"par_ProgramDOL"          => "$rev/timeline_summary_$version.fits[AUXL-PROG-PRE]",
			"par_PODDOL"              => "$rev/pod_${rev}_",
			"par_PDEFfilename"        => "pointing_definition_predicted_$version.fits",
			"par_AlertFilterStart"    => "$first_time",
			"par_AlertFilterDuration" => "0",
			"subdir"                  => "$newworkdir",
			);
		
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/*fits* $newworkdir/*/*fits* $newworkdir/$dataset",
			);			       
		
		my $adpDirname = "$orgdir/$rev/";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - copy the TSF file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$adpDirname",
			"subdir"       => "$newworkdir",
			);
		$adpDirname = "$adpdir/$rev.000/";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - copy the TSF results to adp - timeline",
			"program_name" => "COPY",
			"filename"     => "$rev/timeline_summary_$version.fits", 
			"newdir"       => "$adpDirname",
			"subdir"       => "$newworkdir",
			);
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - copy the TSF results to adp - pointing",
			"program_name" => "COPY",
			"filename"     => "pointing_definition_predicted_$version.fits",
			"newdir"       => "$adpDirname",
			"subdir"       => "$newworkdir",
			);
		last SWITCH;
	}
	
	if ($dataset =~ /THF_(\d{6})_\d{4}/) {
		my $date;
		my $copy = 0;
		
		chdir "$newworkdir" or die "*******     ERROR:  cannot chdir to $newworkdir";
		
		#  Check the length of the file:
		my $length = `$mywc $dataset`;
		chomp $length;
		my @length = split('\s+',$length);
		#  This returns ("","length",...), so the 2nd, number 1, is the length
		$length = $length[1];  
		
		print "*******     Length is $length\n";
		
		#  date is yymmdd annoying form.  use first line of file
		open(THF,"$dataset") or die "*******     ERROR:  cannot open $dataset to read";
		while (<THF>) {  
			$date = $_;  
			print "*******     First line is:\n$date\n";
			#  First line must read:
			#  THF_020607_0003.DAT  <whitespace>  2002.158.07.55.45  2002.158.09.01.39  01:10:00 
			if ($date =~ s/$dataset\s+(\S+)\s+\S+\s+\S+.*/$1/) {
				$copy++;
				last;
			}
			else {
				print "*******     ERROR:  cannot parse first line of THF file.\n";
				if ($length < 2) {
					print "*******     WARNING:  Length $length less than 2;  deleting effectively empty file.\n";
				}
				else {
					&Error( "file contains data (length is $length lines) but header cannot be parsed" );
				} # end if data
				
			} # end if date parsable
			
		} # only the first line matters
		close THF;
		
		if (!($copy)) {
			#  This is the above case where the header couldn't be parsed, but 
			#   the file contains no data anyway, so not worth throwing an error:
			#
			#  In this case, we delete the input and all associated OPUS/log files
			
			print "*******    Deleting $dataset and associated files:\n";
			chdir "$ENV{WORKDIR}" or die "*******     ERROR:  cannot chdir $ENV{OPUS_WORK}";
			my ($retval, @result) = &ISDCPipeline::RunProgram("$myrm -rf $ENV{OSF_DATASET}");
			die "*******     ERROR:  cannot remove $ENV{WORKDIR}/$ENV{OSF_DATASET}:  @result" if ($retval);
			
			chdir "$ENV{LOG_FILES}" or die "*******     ERROR:  cannot chdir $ENV{LOG_FILES}";
			unlink "${dataset}_log.txt" or die "*******     ERROR:  cannot delete log ${dataset}_log.txt";
			unlink "$ENV{OSF_DATASET}.log" or die "*******     ERROR:  cannot delete log $ENV{OSF_DATASET}.log";
			
			exit 5; #  tells OPUS to delete the OSF.
			
		} # end if ! $copy
		
		#  Turn 2002.158.07.55.45 into 200215807:
		$date =~ s/(\d{4})\.(\d{3})\.(\d{2}).*/$1$2$3/;
		
		#  Get the revolution number:
		my $revno = &ISDCPipeline::ConvertTime(
			"informat"  => "YYYYDDDHH",
			"intime"    => "$date",
			"outformat" => "revnum",
			);
		$revno = sprintf("%04d",$revno);
		my $orgpath = "$ENV{REP_BASE_PROD}/aux/org/$revno/thf/";
		print "*******     Moving THF to $orgpath\n";
		if (! -d "$orgpath") {
			`$mymkdir -p $orgpath`;
			die "*******     ERROR:  couldn't make dir with command \'$mymkdir -p $orgpath\'" if ($?);
		}
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - write protect files",
			"program_name" => "$mychmod 444 $newworkdir/$dataset",
			);			       
		
		# Then move it
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - copy the THF file to org",
			"program_name" => "COPY",
			"filename"     => "$dataset",
			"newdir"       => "$orgpath",
			"subdir"       => "$newworkdir",
			);
		&ISDCPipeline::RunProgram("$mymkdir -p $adpdir/$revno.000/logs/thf") 
			unless (-d "$adpdir/$revno.000/logs/thf");
		
		#  Move the log file:
		&ISDCPipeline::MoveLog(
			"$ENV{LOG_FILES}/${dataset}_log.txt",
			"$adpdir/$revno.000/logs/thf/${dataset}_log.txt",
			"$ENV{LOG_FILES}/$opuslink"
			);
		
		last SWITCH;
		
	}
	
	if ($dataset =~ /^(\d{9})\.OLF/) {
		my $date = $1;
		chdir "$newworkdir" or die "*******     ERROR:  cannot chdir to $newworkdir";
		my $vers = $dataset;
		$vers =~ s/.OLF//;  # (Yes, a dot isn't a dot;  it may be an underscore.)

		my $revno = &ISDCPipeline::ConvertTime(
			"informat"  => "YYYYDDDHH",
			"intime"    => "$date",
			"outformat" => "revnum",
			);

		$revno = sprintf("%04d",$revno);
		my $prevrev = sprintf("%04d",$revno-1);
		my $first_olf_in_rev;

		unless ( -d "$ENV{REP_BASE_PROD}/aux/org/$revno/olf/" ) {
			#	ie.  this is the first OLF of the revolution
			#	check that the previous revolution received enough TIME_CORRELATION records
			$first_olf_in_rev = 1;
		}
		##################################

		if (!-s "$dataset") {
			print "*******     OLF is empty;  skipping processing.\n";
			
			my $revno = &ISDCPipeline::ConvertTime(
				"informat"  => "YYYYDDDHH",
				"intime"    => "$date",
				"outformat" => "revnum",
				);
			$revno = sprintf("%04d",$revno);
			my $orgpath = "$ENV{REP_BASE_PROD}/aux/org/$revno/olf/";
			print "*******     Moving empty OLF to $orgpath\n";
			if (-d "$orgpath") {
				`$mymkdir -p $orgpath`;
				die "*******     ERROR:  cannot mkdir $orgpath." if ($?);
			}
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - write protect files",
				"program_name" => "$mychmod 444 $newworkdir/$dataset",
				);			       
			
			# Then move it
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - copy the OLF file to org",
				"program_name" => "COPY",
				"filename"     => "$dataset",
				"newdir"       => "$orgpath",
				"subdir"       => "$newworkdir",
				);
			
			chdir "$ENV{WORKDIR}" or die "*******     Cannot chdir to $ENV{WORKDIR}";
			# Now just remove the log file and scratch dir: 
			my ($retval,@result) = &ISDCPipeline::RunProgram("$myrm -rf $ENV{WORKDIR}/$ENV{OSF_DATASET}");
			die ">>>>>>>     Cannot remove $ENV{WORKDIR}/$ENV{OSF_DATASET}:\n@result" if ($retval);
			
			($retval,@result) = &ISDCPipeline::RunProgram("$myrm -f $ENV{LOG_FILES}/${dataset}_log.txt $ENV{LOG_FILES}/$opuslink");
			die ">>>>>>>     Cannot remove $ENV{LOG_FILES}/${dataset}_log.txt $ENV{LOG_FILES}/$opuslink:\n@result" if ($retval);
			
			# Exit special value to turn to ccc
			exit 3;
			
		}
		
		else {
			
			&ISDCPipeline::PipelineStep (
				"step"             => "adp - Convert OLF file",
				"program_name"     => "convertprogram",
				"par_Infile"       => "$dataset",
				"par_OutfileTCOR"  => "time_correlation.fits",
				"par_OutfileOOL"   => "moc_out_of_limits.fits",
				"par_OutfilePROGH" => "observation_log.fits",
				"par_OutfilePROGP" => "",
				"par_Outdir"       => "./",
				"par_Toler"        => "$ENV{OLF_TOLERANCE}",
				"par_MOCAlert"     => "Summary",
				"subdir"           => "$newworkdir",
				);
			
			# figure out the rev number
			my $rev   = "";
			foreach my $file ( glob ( "$newworkdir/*" ) ) {
				if (-d $file) {
					$file =~ /.*([0-9]{4})/;
					$rev = $1;
				}
			}
			# Now we know what rev number we have, 
			# alert if rev has already been triggered for archive ingest
			&CheckRevTrigger("$rev",$dataset);
			
			#  Construct name of output time correlation, using both time stamp and
			#   OLF version.  (Do it here, before checking for previous tcor.)
			my $timestamp = &TimeLIB::MyTime();
			$timestamp =~ s/:|-|T//g;
			
			# Copy the files from the repository if they exist
			my $rmlist;
			foreach (`$myls $rev/*fits* 2> /dev/null`) { chomp;  $rmlist .= "$_ "; }
			foreach (`$myls *alert* 2> /dev/null`) { chomp;  $rmlist .= "$_ ";}
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - rm the old ones",
				"program_name" => "$myrm -f $rmlist",
				"subdir"       => "$newworkdir",
				);
			my $adpDirname = "$adpdir/$rev.000/";
			
			my @timecors = sort(glob("$adpDirname/time_correlation_*fits*"));
			if (@timecors) {
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - copy time_correlation file from repository",
					"program_name" => "COPY",
					"filename"     => "$timecors[$#timecors]",
					"newdir"       => "$rev",
					"subdir"       => "$newworkdir",
					"needfiles"    => "0",
					"overwrite"    => "1",
					);
				my $oldtimecor = &File::Basename::fileparse($timecors[$#timecors], '\..*');
				
				rename "$newworkdir/$rev/$oldtimecor.fits","$newworkdir/$rev/time_correlation_${timestamp}_${vers}.fits" 
					or die "Cannot rename $newworkdir/$rev/$oldtimecor.fits to $newworkdir/$rev/time_correlation_${timestamp}_${vers}.fits";
			}
			if (-e "$adpDirname/moc_out_of_limits.fits") {
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - copy moc_out_of_limits file from repository",
					"program_name" => "COPY",
					"filename"     => "$adpDirname/moc_out_of_limits.fits",
					"newdir"       => "$rev",
					"subdir"       => "$newworkdir",
					"needfiles"    => "0",
					"overwrite"    => "1",
					);
			}
			
			if (-e "$adpDirname/observation_log.fits") {
				
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - copy observation_log file from repository",
					"program_name" => "COPY",
					"filename"     => "$adpDirname/observation_log.fits",
					"newdir"       => "$rev",
					"subdir"       => "$newworkdir",
					"needfiles"    => "0",
					"overwrite"    => "1",
					);
			}
			# make sure they are writeable
			&ISDCPipeline::RunProgram("$mychmod +w $newworkdir/$rev/*") and
			&ISDCPipeline::PipelineStep (
				"step"         => "ERROR",
				"error"        => " - Cannot change files $newworkdir/$rev.000/* to writeable:  $!\n",
				"program_name" => "ERROR",
				) if (`$myls $newworkdir/$rev/*fits* 2> /dev/null`);
			
			&ISDCPipeline::PipelineStep (
				"step"             => "adp - Convert OLF file",
				"program_name"     => "convertprogram",
				"par_Infile"       => "$dataset",
				"par_OutfileTCOR"  => "time_correlation_${timestamp}_$vers.fits",
				"par_OutfileOOL"   => "moc_out_of_limits.fits",
				"par_OutfilePROGH" => "observation_log.fits",
				"par_OutfilePROGP" => "",
				"par_Outdir"       => "./",
				"par_Toler"        => "$ENV{OLF_TOLERANCE}",
				"par_MOCAlert"     => "Summary",
				"subdir"           => "$newworkdir",
				);
						
			if (-e "$rev/observation_log.fits") {
				#  Get time stamp in first line of OLF file:
				my $first_time = `$myhead -1 $dataset`;
				chomp $first_time;
				#
				$first_time =~ s/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z\s.*/$1/;
				&ISDCPipeline::PipelineStep (
					"step"         => "adp - ERROR",
					"program_name" => "ERROR",
					"error"        => "OLF first line not recognized:\n$first_time",
					) unless ($1);
				
				&ISDCPipeline::PipelineStep (
					"step"                    => "adp - Program Definition File",
					"program_name"            => "createpdef",
					"par_ProgramDOL"          => "$rev/observation_log.fits[AUXL-PROG-HIS]",
					"par_PODDOL"              => "$adpDirname/pod_${rev}_",
					"par_PDEFfilename"        => "$rev/pointing_definition_historic.fits[AUXL-PDEF-HIS]",
					"par_AlertFilterStart"    => "$first_time",
					"par_AlertFilterDuration" => "$ENV{ALERT_FILTER_DURATION}",
					"subdir"                  => "$newworkdir",
					);
				
			} # if observation log created
			else {
				&Message ( "adp - observation log not created;  skipping createpdef" );
			}
			
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - write protect files",
				"program_name" => "$mychmod 444 $newworkdir/*/*fits* $newworkdir/$dataset",
				);			       
			
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - copy the OLF file to org",
				"program_name" => "COPY",
				"filename"     => "$dataset",
				"newdir"       => "$orgdir/$rev/olf/",
				"subdir"       => "$newworkdir",
				);
			
			# move the log file.  We couldn't put this in the right place in the 
			# beginning because we didn't know what rev this was for.
			`$mymkdir -p $adpdir/$rev.000/logs/olf` if (!(-d "$adpdir/$rev.000/logs/olf"));
			die "*******     ERROR:  cannot mkdir $adpdir/$rev.000/logs/olf." if ($?);
			
			&ISDCPipeline::MoveLog(
				"$ENV{LOG_FILES}/${dataset}_log.txt",
				"$adpdir/$rev.000/logs/olf/${dataset}_log.txt",
				"$ENV{LOG_FILES}/$opuslink"
				);
			
			&ISDCPipeline::PipelineStep (
				"step"         => "adp - copy the OLF results to adp",
				"program_name" => "COPY",
				"filename"     => "$rev/*.fits",
				"newdir"       => "$adpdir/$rev.000/",
				"subdir"       => "$newworkdir",
				"overwrite"    => "2",
				);
			
			&ISDCPipeline::LinkUpdate(
				"root"    => "time_correlation",
				"subdir"  => "$adpdir/$rev.000",
				"ext"     => ".fits",
				);			       
			
		}

		if ( $first_olf_in_rev ) {
			#	ie.  this is the first OLF of the revolution
			#	check that the previous revolution received enough TIME_CORRELATION records
			my ($root,$path,$suffix);
			my $timecorrcount = 0;
			my $timecorrminok = 20000;		#	this number should trigger all currently processed revs
			if ( $ENV{TIME_CORRELATION_MIN_OK} ) { $timecorrminok = $ENV{TIME_CORRELATION_MIN_OK}; }

			&Message ( "Checking TIME_CORRELATION records from $prevrev using min $timecorrminok" );

			foreach my $OLF ( glob ( "$ENV{REP_BASE_PROD}/aux/org/$prevrev/olf/*OLF" ) ) {
				my ($root,$path,$suffix) = &File::Basename::fileparse($OLF,'\..*');
				open CURFILE, "<$OLF";
				while (<CURFILE>) {
					$timecorrcount++ if ( /TIME_CORRELATION/ );
				}
				close CURFILE;
			}

			&Message ( "$timecorrcount TIME_CORRELATION records in $prevrev" );

			&ISDCPipeline::WriteAlert(
				"step"    => "adp - OLF TIME_CORRELATION check alert",
				"message" => "Revolution $prevrev has low number ($timecorrcount) of TIME_CORRELATION records",
				"level"   => 2,
				"subdir"  => "$newworkdir",
				"id"      => "510",
				) if ( $timecorrcount < $timecorrminok );
		}
		
		last SWITCH;
	} 
	
	if ($dataset =~ /(\d{4})_arc_prep/) {
		
		# additional possible way to cause rev archiving;  meant for telling 
		#  pipeline to check again when first check wasn't long enough after
		#  last link update;  also useful for manually triggering
		
		my $revno = $1;
		print "******************************************************************\n";
		print "          ARC CHECK $revno...\n";
		&RevArchiving($revno);
		# Now just remove scratch dir (should be empty):
		chdir "$ENV{WORKDIR}" or die "*******     Cannot chdir to $ENV{WORKDIR}";
		my ($retval,@result) = &ISDCPipeline::RunProgram("$myrm -rf $ENV{WORKDIR}/$ENV{OSF_DATASET}");
		die ">>>>>>>     Cannot remove $ENV{WORKDIR}/$ENV{OSF_DATASET}:\n@result" if ($retval);
		
		#  Tells it to set it immediately to ccc and not try to run finish
		#   (since log is now write protected)
		exit 3;
	}
	
	########################################################################
	# unknown
	
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - error exit",
		"error"        => "dataset $dataset is unknown",
		"program_name" => "ERROR",
		"subdir"       => "$ENV{WORKDIR}",
		);
}

########################################################################
########                   FINISHING
########################################################################

# Can't do this if this rev was already archived, as perhaps done above 
#  for AHF and orbit (which archiving step will have done this):

&ISDCPipeline::PipelineStep (
	"step"           => "adp - Copy Alerts",
	"program_name"   => "am_cp",
	"par_OutDir"     => "$ENV{ALERTS}",
	"par_OutDir2"    => "",
	"par_Subsystem"  => "ADP",
	"par_DataStream" => "realTime",
	"par_ScWIndex"   => "",
	"subdir"         => "$newworkdir",
	) if (`$myls $newworkdir/*alert 2> /dev/null`);

&ISDCPipeline::PipelineStep (
	"step"         => "adp - cleanup scratch",
	"program_name" => "$myrm -rf $newworkdir",
	"subdir"       => "$ENV{ADP_INPUT}",
	);

&ISDCPipeline::PipelineStep (
	"step"         => "adp - done",
	"program_name" => "NONE",
	);
exit 0;

########################################################################
########                   END
########################################################################



########################################################################

=item B<CheckRevTrigger> ( $revnum, $dataset )

check if rev has already been triggered for archive ingest;  should not have any new files tiggered if so!  Error off immediately.  Only used for orbit and OLF, where rev isn't known during adpst.  here, exits with an error if so.  give revolution number and current dataset (for loggin)

=cut

sub CheckRevTrigger {
	my ($revnum,$dataset) = @_;
	my $alert;
	print "checking previous triggers for revnumber $revnum\n";
	
	if (-e "$ENV{ARC_TRIG}/aux_${revnum}0000.trigger") {
		
		&ISDCPipeline::PipelineStep (
			"step"         => "ERROR adp",
			"program_name" => "ERROR",
			"error"        => "Revolution $revnum already triggered for archiving!",
			);
	}
	else {
		return;
	}
} #sub CheckRevTrigger



########################################################################

=item B<RevArcCheck> ( $revnum )

determine if a directory is ready for archiving;  
send alerts if it's incomplete at the time the historic files are present
if all passes,  write arc_prep trigger
give revolution number only

=cut

sub RevArcCheck {
	my $revnum = "@_";
	my %olfosfs;
	my @osfs;
	my ( $retval, @result );
	
	print "\n******************************************************************\n";
	print "          CHECKING COMPLETENESS OF REVOLUTION $revnum\n";
	
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check for rev $revnum",
		"program_name" => "NONE",
		);
	my @revfiles = sort(glob("$adpdir/$revnum.000/*.fits*"));
	my $string = join(' ',@revfiles);
	###
	###  first pass to check historic orbit and attitude
	###
	if (($string =~ /orbit_historic/) && ($string =~ /attitude_historic/)) {
		print "*********      historic orbit and attitude information available;  checking revolution $revnum for completeness\n";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - archiving check:  historic orbit and attitude information available ",
			"program_name" => "NONE",
			);
	}
	else {
		print "*********      historic orbit and attitude information not yet available;  \n*********      no archive triggering yet.\n";
		print "******************************************************************\n";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - archiving check:  not all historic data yet available;  done.",
			"program_name" => "NONE",
			);
		return;
	}
	##
	### second pass of other required contents which ought to be there by the
	### time the historic files were;  if one is missing, alert.
	##
	my $missing;
	$missing .= "orbit_predicted"     unless ($string =~ /orbit_predicted/);
	$missing .= "attitude_predicted " unless ($string =~ /attitude_predicted/);
	$missing .= "observation_log "    unless ($string =~ /observation_log/);
	$missing .= "time_correlation "   unless ($string =~ /time_correlation/);
	$missing .= "attitude_snapshot "  unless ($string =~ /attitude_snapshot/);
	$missing .= "timeline_summary"    unless ($string =~ /timeline_summary/);
	$missing .= "pod_${revnum}_"      unless ($string =~ /pod_${revnum}_/);
	
	if ($missing) {
		&Message ( "Previously found adp fits files in $adpdir/$revnum.000/ : \n@revfiles\n" );
		@revfiles = sort(glob("$adpdir/$revnum.000/*.fits*"));
		&Message ( "Just now found adp fits files in $adpdir/$revnum.000/ : \n@revfiles\n" );
		@revfiles = sort(glob("$adpdir/*"));
		&Message ( "Just now found adp revdirs in $adpdir/ : \n@revfiles\n" );

		# send alert if one of these is missing and the historics are there.
		&ISDCPipeline::WriteAlert(
			"step"    => "adp - revolution check alert",
			"message" => "Revolution $revnum missing $missing",
			"level"   => 2,
			"subdir"  => "$newworkdir",
			"id"      => "505",
			);
		return;
	}
	##
	### next check: (PAF version = TSF version) and (POD version = OPP version)
	##
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check:  minimum contents present",
		"program_name" => "NONE",
		);
	
	#  (each of these are named with the version of the original file)
	my @pafs = sort(glob("$adpdir/$revnum.000/attitude_predicted*"));
	my @tsfs = sort(glob("$adpdir/$revnum.000/timeline_summary_*"));
	my @pods = sort(glob("$orgdir/$revnum.000/pod*"));
	my @opps = sort(glob("$orgdir/$revnum.000/opp*"));
	
	my $alert;
	my $alertid;
	
	$pafs[$#pafs] =~ /attitude_predicted_(\d{2})/;
	print "*********      last PAF:  $pafs[$#pafs]\n";
	my $pafvers = $1;
	$tsfs[$#tsfs] =~ /pointing_definition_predicted_\d{2}(\d{2})/;
	print "*********      last TSF: $tsfs[$#tsfs]\n";
	my $tsfvers = $1;
	if ($pafvers != $tsfvers) {
		print "*********      last predicted attitude version $pafvers not same as last timeline summary version $tsfvers; sending alert\n";
		$alert = "Last predicted attitude version $pafvers not same as last timeline summary version $tsfvers for rev $revnum. "; 
		$alertid = "503";
	}
	$pods[$#pods] =~ /pod_\d{4}_(\d{4})/;
	print "*********      last POD:  $pods[$#pods]\n";
	my $podvers = $1;
	# OPPs look like, e.g. for rev 0013, opp_0013_0002_0020.tar,
	#   and we want to look at those last four digits, here version 0020.
	$opps[$#opps] =~ /opp_${revnum}_\d{4}_(\d{4})\./;
	print "*********      last OPP:  $opps[$#opps]\n"; 
	my $oppvers = $1;
	if ($podvers != $oppvers) {
		print "*********      Last pod version $podvers not same as last opp vers $oppvers;  sending alert";
		$alert .= "Last pod version $podvers not same as last opp vers $oppvers for rev $revnum.";
		$alertid = "502";
	}
	
	if ($alert) {
		&ISDCPipeline::WriteAlert(
			"step"    => "adp - revolution check alert",
			"message" => "$alert",
			"level"   => 2,
			"subdir"  => "$newworkdir",
			"id"      => "$alertid",
			);
		return;
	}
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check:  POD/OPP and PAF/TSF versions agree",
		"program_name" => "NONE",
		);
	
	##
	### forth check that there were no errors for files in this rev
	##
	print "*********      checking status of all OSFs on blackboard\n";
	($retval,@osfs) = &ISDCPipeline::RunProgram("osf_test -p adp.path -pr dataset");
	my $num;
	if ($retval) {
		die "*********      Cannot run \'osf_test -p adp.path -pr dataset\':  @osfs";
	}
	else {
		my $osf;
		foreach $osf (@osfs) {
			$/ = " \n"; # just so chomp takes off newline and space printed by 
			#  the osf_test tool 
			chomp $osf;
			# the current OSF is obviously not complete yet
			next if ($osf =~ /$ENV{OSF_DATASET}/);
			# match the ones we know are in this rev
			($osf =~ /opp_(\d{4})/) 
			or ($osf =~ /pod_([0-9]{4})/) 
			or ($osf =~ /([0-9]{4})_[0-9]{2}_PAF/) 
			or ($osf =~ /([0-9]{4})_[0-9]{4}_(ASF|AHF)/) 
			or ($osf =~ /TSF_([0-9]{4})/);
			# just store OLFs in a hash for now
			$olfosfs{"$osf"} = 1 if ($osf =~ /OLF/);
			# now, we only care about things of this revolution number
			next unless ( $1 && ($1 == $revnum));
			# check the status of the finish step
			($retval,@result) = &ISDCPipeline::RunProgram("osf_test -p adp.path -pr FI -f $osf");
			chomp($result[0]);
			print "*********      status of $osf is $result[0]\n";
			$num++ unless ($result[0] =~ /c/);
		} # foreach stat
	} # if no error checking osfs
	
	#  if there were any incomplete, we can't archive yet. 
	if ($num) {
		print "*********      $num files for rev $revnum incomplete;  not archiving\n";
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - archiving check: $num files for rev $revnum incomplete",
			"program_name" => "NONE",
			);
		return;
	}
	
	#  otherwise, we continue:
	
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check:  rev OSFs all complete",
		"program_name" => "NONE",
		);
	
	##
	###  also have to check olfs;  more difficult because don't know rev number
	###  and additionally painful to handle names of YYYYDDDHH format;  
	##
	###  use both OSFs and files found in repository
	
	print "*********      the OLFs found were: ",sort(keys(%olfosfs)),"\n";
	
	my $prevrev = sprintf("04d",$revnum-1);
	my $nextrev = sprintf("04d",$revnum+1);
	my @olfs = sort(glob("$orgdir/$revnum/olf/*"));
	die "Cannot find OLFs in org:  $orgdir/$revnum/olf/" unless (@olfs);
	my @prevolfs = sort(glob("$orgdir/$prevrev/olf/*"));
	my @nextolfs = sort(glob("$orgdir/$nextrev/olf/*"));
	my $end;
	my $start;
	my ($hex,$status,$osfname,$type,$dcf,$com);
	# start with last successful OLF from previous revolution 
	$start = $prevolfs[$#prevolfs] if (@prevolfs);
	#  or first from current
	$start = $olfs[0] unless (@prevolfs);
	# end with first from next
	$end = $nextolfs[0] if (@nextolfs);
	# or last from current if none yet for next
	$end = $olfs[$#olfs] unless (@nextolfs);
	# parse YYYYDDDHH format
	$start =~ s/.*(\d{9})\.OLF/$1/; 
	$end  =~ s/.*(\d{9})\.OLF/$1/;
	print "*********      checking OLFs from $start to $end\n";
	my $i;
	print "*********      testing all OLFs\n";
	#  Run numerically from start YYYYDDDHH to end, as found in repositorty,
	#   and check the OSFs.  Yeah, it's ugly.  
	for ($i=$start;$i<=$end;$i++) {
		# because the hours only go 01 to 24, ignore numbers like ddddddd25
		$i =~ /\d{7}(\d{2})/;
		next if ($1 > 23);
		$i =~ /(\d{4})(\d{3})\d{2}/;
		next if ( $2 == 000 );		#	040708 - Jake - SPR 3413 (There is no day 0)
		# leap years?  Should be good enough to check divisible by 4.
		# So, don't check numbers over 365 if year *not* divisible by 4:
		next if (($2 > 365) && ($1 % 4)) ;
		# Otherwise, don't check over 366.  
		next if ($2 > 366);
		# see if there is an OSF for every hour in the range
		#  (Remember the OSFs now have . replaced with _.)
		if (!$olfosfs{"${i}_OLF"}) {
			$missing .= "${i}_OLF";
			print "*********      missing ${i}_OLF\n";
			next;
		}
		else {
			print "*********      found ${i}_OLF;  testing...\n";
			#  Since these get cleaned up automatically, the OSF may have 
			#   already been deleted.  If the OSF exists, it should be complete.
			@result = `$myls $ENV{OPUS_WORK}/adp/obs/*${i}_OLF* 2> /dev/null`;
			die "*******     ERROR:  I'm confused, as there appear to be more than one OSF matching ${i}_OLF:  @result" if ($#result > 0);
			if (@result) {
				($hex,$status,$osfname,$type,$dcf,$com) = &OPUSLIB::ParseOSF($result[0]);
				$num++ unless ($status =~ /c$/);
			} # end if OSF exists
			else {
				print "*******     OSF ${i}_OLF not found;  must have been cleaned and was therefore complete.\n";
			}
		}
	} # for each olf in range
	
	if ($missing) {
		&ISDCPipeline::WriteAlert(
			"step"    => "adp - revolution check alert",
			"message" => "missing OLFs in rev $revnum:  $missing",
			"level"   => 2,
			"subdir"  => "$newworkdir",
			"id"      => "506",
			);
		return;
	}
	elsif ($num) {
		&ISDCPipeline::PipelineStep (
			"step"         => "adp - archiving check: $num OLF files for rev $revnum incomplete",
			"program_name" => "NONE",
			);
		return;
	}
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check:  associated OLFs look complete",
		"program_name" => "NONE",
		);
	
	#
	#  Everything passed, write arc_prep trigger.
	#
	
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check PASSED;  writing arc_prep trigger",
		"program_name" => "$mytouch $ENV{ADP_INPUT}/${revnum}_arc_prep.trigger",
		);
	
}  #  end of RevArcCheck


########################################################################

=item B<RevArchiving> ( $revnum )

=cut

sub RevArchiving {
	
	my $revnum = "@_";
	my ($retval,@result);
	
	######################################################################
	##           ARCHIVING:  Checks passed;  now do it.
	######################################################################
	
	##  
	### clean up time corr and snapshot att versions;  remove all but last,
	### and move last to replace link
	##

	($retval,@result) = &ISDCPipeline::LinkReplace(
		"root"   => "time_correlation",
		"subdir" => "$ENV{OUTPATH}/aux/adp/$revnum.000",
		"proc"   => "adp",
		);

	&ISDCPipeline::PipelineStep (
		"step"         => "adp - ERROR during revolution archiving",
		"program_name" => "ERROR",
		"error"        => "Problem cleaning time correlation files:  @result",
		) if ($retval);

	# Make sure file is writeable:
	&ISDCPipeline::RunProgram( "$mychmod a+w $ENV{OUTPATH}/aux/adp/$revnum.000/time_correlation.fits*");

	($retval, @result) = &ISDCPipeline::PipelineStep (
	        "step"           => "adp - run tcor_flag on last time_correlation.fits",
	        "program_name"   => "tcor_flag",
	        "par_tcor_dol"   => "$ENV{OUTPATH}/aux/adp/$revnum.000/time_correlation.fits",
	        );

	&ISDCPipeline::PipelineStep (
	        "step"         => "adp - ERROR running tcor_flag.",
	        "program_name" => "ERROR",
	        "error"        => "Problem running tcor_flag:  @result",
	    ) if ($retval);

	($retval,@result) = &ISDCPipeline::LinkReplace(
		"root"   => "attitude_snapshot",
		"subdir" => "$ENV{OUTPATH}/aux/adp/$revnum.000",
		"proc"   => "adp",
		);

	&ISDCPipeline::PipelineStep (
		"step"         => "adp - ERROR during revolution archiving",
		"program_name" => "ERROR",
		"error"        => "Problem cleaning attitude_snapshot files:  @result",
		) if ($retval);

	&ISDCPipeline::PipelineStep (
		"step"         => "adp - archiving check: snapshot attitudes and time correlation files cleaned successfully",
		"program_name" => "NONE",
		);
	
	&RevContentsCheck($revnum);
	
	print "*********      All contents ready for rev $revnum;  writing archive ingest trigger\n"; 

	&UnixLIB::Gzip ( "$adpdir/$revnum.000/*.fits" );
	
	&ISDCPipeline::PipelineStep (  
		"step"         => "adp - write protect rev directory recursively", 
		"program_name" => "NONE",  
		); 
		
	($retval,@result) = &ISDCPipeline::RunProgram("$mychmod -R -w $adpdir/$revnum.000");
	# but if there's an error, you can try:
	&ISDCPipeline::PipelineStep (
		"step"         => "adp - ERROR",
		"program_name" => "ERROR",
		"error"        => "problem write protecting rev $revnum;  result @result",
		) if ($retval); 
	
	# write the trigger file (tmp name first, then mv)
	open(AIT,">$ENV{ARC_TRIG}/aux_${revnum}0000.trigger_temp") 
		or die "*******        ERROR:  cannot create trigger file $ENV{ARC_TRIG}/aux_${revnum}0000.trigger_temp"; 
	print AIT "$ENV{ARC_TRIG}/aux_${revnum}0000.trigger AUX $adpdir/${revnum}.000"; 
	close(AIT); 
	
	($retval,@result) = &ISDCPipeline::RunProgram(
		"$mymv $ENV{ARC_TRIG}/aux_${revnum}0000.trigger_temp $ENV{ARC_TRIG}/aux_${revnum}0000.trigger"
		); 
	die "*******        ERROR:  cannot create trigger file $ENV{ARC_TRIG}/aux_${revnum}0000.trigger" if ($retval);
	
	#  Set all associated OSFs for cleaning.  Not so easy in ADP.
	#  First, look in logs subdir and compile a list of OSFs.  There
	#   is no other way to find out everything associated with this 
	#   revolution.  
	my $osflist;
	foreach my $log (`$myls $adpdir/$revnum.000/logs/*`) {
		#  ls will return blanks and e.g. "olf:" lines indicating the subdirs
		next unless ($log =~ /\w/);  
		next if ($log =~ /:/);  
		next if ($log =~ /arc_prep/);  # don't do this one
		
		chomp $log;
		$log = &File::Basename::basename ( $log );
		
		#  Split for each type, unfortunately:
		$log =~ s/^(.*)\.(PAF|ASF|AHF|OLF|DAT)_log\.txt/${1}_$2/;
		$log =~ s/^(TSF_.*)\.INT_log\.txt/${1}_INT/;
		$log =~ s/^(opp_.*)\.tar_log\.txt/${1}_tar/;
		#  Anything without a ., for example orbita_YYYYMMDD, revno_YYYYMMDD,
		#   pod_RRRR_VVVV, 
		$log =~ s/^(pod_.*)_log\.txt/${1}_fits/;
		$log =~ s/^(.*)_log\.txt/$1/;
		
		#  Note that IOP, OCS, PAD, and ARC type datasets don't correspond to
		#   a revolution, so they'll get cleaned up on the timeout.  
		
		$osflist .= "$log " if (`$myls $ENV{OPUS_WORK}/adp/obs/*$log* 2> /dev/null`);
		
	} # end foreach log
	
	if ($osflist) {
		#  This function by default updates all these to CL==0 for cleaning.
		&ISDCPipeline::BBUpdate("list" => "$osflist");
	}
	else {
		print ">>>>>>>     WARNING:  no OSFs to update.\n";
	}
	
	return 1;
} # sub RevArchiving   



########################################################################

=item B<RevContentsCheck> ( $revnum )

subroutine to check the contents of a revolution and ensure that
no junk files remain before the entire directory is blindly 
archived.  This function will exit with an error if a junk file
is found.
	
=cut

sub RevContentsCheck {
	my ( $revnum ) = @_;
	my ( $retval, @result );
	my ( $root, $path, $suffix );
	
	my @contents =  glob( "$adpdir/$revnum.000/*" );
	push @contents, glob( "$adpdir/$revnum.000/*/*" );
	push @contents, glob( "$adpdir/$revnum.000/*/*/*" ); 
	
	foreach my $one (@contents) {
		chomp $one;
		print "*******     File:  $one\n";
		#  Note that here, we want the root to possibly contain a "." (the logs
		#   will be e.g. 0043_0001.AHF_log.txt)  Using '\.\w*' gives the correct
		#   split, i.e. root "0043_0001.AHF_log" and suffix ".txt".  

		#	050719 - Jake - SPR 4271
		#	What if there was a failure in a previous run?
		#	What if the files are gzipped already?
		#	strip off potential trailing gz extension to help a rerun
		$one =~ s/\.gz\s*$//;

		($root,$path,$suffix) = &File::Basename::fileparse($one,'\.\w*');
		$path =~ s/\///;
		print "*******      Root is $root, suffix is $suffix, path is $path\n";
		
		next if ($root =~ /^logs$/);
		next if ( ($root =~ /^(olf|asf|thf)$/) && ($path =~ /logs\/$/));
		
		next if ( ($root =~ /^attitude_(predicted_\d{2}|snapshot|historic)$/) && ($suffix =~ /^\.fits$/) );
		##
		
		next if ( ($root =~ /^(box|shot)plan_$revnum\d{4}_\d{4}$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^moc_out_of_limits$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^observation_log$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^orbit_(predicted|historic)$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^pod_${revnum}_\d{4}$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^pointing_definition_(predicted_\d{4}|historic)$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^time_correlation$/) && ($suffix =~ /^\.fits$/) );
		
		next if ( ($root =~ /^timeline_summary_\d{4}$/) && ($suffix =~ /^\.fits$/) );
		
		if ( ($path =~ /logs\/$/) && ($suffix =~ /^\.txt$/) ) {
			
			next if ($root =~ /^${revnum}_\d{4}\.AHF_log$/);
			
			next if ($root =~ /^${revnum}_\d{2}\.PAF_log$/);
			
			next if ($root =~ /^TSF_${revnum}_\d{4}\.INT_log$/);
			
			next if ($root =~ /^opp_${revnum}_\d{4}_\d{4}\.tar_log$/);
			
			next if ($root =~ /^pod_${revnum}_\d{4}_log$/);
			
			next if ($root =~ /^${revnum}_arc_prep_log$/);
			
			next if ($root =~ /^orbita_\d{8}_log$/);
			
		}
		
		next if ( ($root =~ /^${revnum}_\d{4}\.ASF_log/) && ($path =~ /logs\/asf\/$/) && ($suffix =~ /^\.txt$/) );
		next if ( ($root =~ /^\d{9}\.OLF_log/) && ($path =~ /logs\/olf\/$/) && ($suffix =~ /^\.txt$/) );
		next if ( ($root =~ /^THF_\d{6}_\d{4}\.DAT_log/) && ($path =~ /logs\/thf\/$/) && ($suffix =~ /^\.txt$/) );
		
		#  If you got here, you're junk:
		
		print "*******     ERROR:  Found junk file $one\n";
		my $proc = &ProcStep();
		&Message ( "ERROR before archiving;  Found junk file $one" );
		
	} # foreach contents
	
	return;
	
} # end sub RevContentsCheck

=back

=head1 ACTIONS

Note that the patterns below are Perl regular expressions.
Documentation on these can be found in Programming Perl, Second
Edition, Larry Wall et al, Page 57.

In all cases below the environment variable AUXDIR is used to
reference to top level of the Auxiliary repository.  This variable is
set in the I<adp.resource> file and that references an entry in the
I<adp.path> file.


=over 5

=item B<pad_([0-9]{2})_.*>

This is a pad file which has a 2 digit AO number xx in the file name.
The file is copied both to the org/AOxx as well as the aux/AOxx
directory.

=item B<iop_([0-9]{2}).*>

This is an iop file which has a 2 digit AO number in the file name.
The file is copied both to the org/AOxx as well as the aux/AOxx
directory.

=item B<ocs_([0-9]{2}).*>

This is a ocs file which has a 2 digit AO number in the file name.
The file is copied both to the org/AOxx as well as the aux/AOxx
directory.

=item B<pod_([0-9]){4})>

This is a pod file which has a 4 digit revolution number rrrr in the
file name.  The file is copied both to the org/rrrr as well as the
aux/rrrr.000 directory.

=item B<opp_([0-9]){4})>

This is an opp file which has a 4 digit revolution number rrrr in the
file name.  First the file is untarred and oconvertopp is called for
each one of the .*opp_nnnnnnnn.txt files stored in the .tar archive.
This results in a boxplan_nnnnnnnn.fits and shotplan_nnnnnnnn.fits
file for each .txt file.  Then the opp file is copied both to the
org/rrrr and all of the boxplan and shotplan files are copied to the
aux/rrrr.000 directory.

=item B<([0-9]{4})_([0-9]{2})\.PAF>

This is a PAF file which has a 4 digit revolution number rrrr in the
file name.  convertattitude is run on this file and the resulting
attitude file is copied to aux/rrrr.000.  The original PAF file is
copied to org/rrrr.

=item B<([0-9]{4})_([0-9]{4})\.(ASF|AHF)>

This is either a ASF or an AHF file which has a 4 digit revolution
rrrr number in the file name.  convertattitude is run on this file and
the resulting attitude file is copied to aux/rrrr.000.  The original
file is copied to org/rrrr if it is an AHF file or org/rrrr/asf if it
is an ASF file.  Missing from this step is cnrtattupdate which should
be run but has yet to be delivered.


=item B<orbita>

This is the orbita named orbita_YYYYMMDD.  First convertorbit is run
producing a list of directories which this orbita file contained data
for.  Then all of the resulting orbit*.fits files are copied from the
just created rrrr directories into the cooresponding adp/rrrr.000
directories in the adp repository.  Finally the orbita file is copied
to org/ref.  NB: convertorbit stores the last orbit processed for both
predicted and historic in the .par file.  Therefore if you try to
rerun the same orbita file twice, even if the have different file
names, the second run will not produce anything.

=item B<revno>

This is the revno file named to revno_YYYYMMDD.  Convertrevolution is
run to produce a revolution_yyyymmdd.fits file and this is copied into
adp/ref/revno.  For current software compatibility a soft link is made
to the file adp/ref/revolution.fits but in the future this should go
away.  Once all of this is done then revno_YYYYMMDD file is copied to
org/ref/revno.

=item B<TSF_([0-9]{4})_.*_([0-9]{4}).*INT>

This is a TSF file which has a 4 digit revolution number rrrr followed
by a 4 digit version number vvvv in the file name.  The first step is
to run convertprogram.  Then createpdef is run on the resulting 
timeline_summary file and using the same version of the POD file in the 
repository to create a corresponding 
pointing_definition_predicted_vvvv.fits file with the same version number.  
NB: Currently the pod
version number MUST agree with the TSF version number.  Since it is
possible that alerts were generated the alerts are copied, then the
TSF file is copied to the org/rrrr directory.  Then the
timeline_summary_vvvv.fits file and the pointing_definition_vvvv.fits
files are both copyied to the adp/rrrr.000 directory.

=item B<.*OLF>

This is a OLF file.  It is not possible to derive the proper directory
from the file name since the revolution number is not contained in the
file name.  The first step is to run convertprogram which will create
a rev directory rrrr which is the rev this OLF file is for.  Then copy
the time_correlation.fits, moc_out_of_limits.fits, the
observation_log.fits from the aux/rrrr.000 directory if they exist and
then rerun convertprogram.  This way the existing files are updated.
Once this is done get the latest pod for this revolution and run
createpdef.  Since alerts could have been generated copy the alerts to
the alerts directory, copy the OLF file to the org/rrrr/olf directory,
and move the log file which was previously put in the wrong place by
I<adpst.pl> because it is not possible to derive the rev number rrrr
from the file name to the right place in org/rrrr/olf.  Finally copy
all of the resulting *.fits files to the proper adp/rrrr.000
directory.

=back

Once all of this is done remove the scratch directory which held all
of the work files.

=head1 RESOURCE FILE

The resource file I<adp.resource> contains all the environment
variables for B<adp.pl>.  All of these reference items in the
I<adp.path> path file.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<ALERTS> 

This is the directory which the alert mangement daemon will monitor
for adp pipeline alerts.

=item B<AUXDIR> 

This is the location of the auxiliary data repository.

=item B<ADP_INPUT> 

Where the files are put by IFTS.

=item B<PARFILES>

This is the location of the saved .par files for the different
executables. Make sure that you clean this directory out if you need
to rerun an orbita file.

=item B<WORKDIR>

This is the location of a scratch work directory.

=back

=head1 PATH FILE ENTRIES USED

Path file entries are hard coded directories and will have to be
manually edited to move the pipeline to another set of directories.
There are two sets of directories in the currently delivered path
files.  The first start with /isdc/nrt and these are to point to ISDC
repositories.  Make them point to the right repositories.  The second
currently point to /isdc/scratch and this is a work area for opus to
work in.  /isdc/scratch is not the most pleasing name so possibly
another should be chosen.

=over 5

=item B<rii_aux>

This points to the current top of the auxiliary repository.  This is
assigned to the B<AUXDIR> environment variable by B<OPUS>.

=item B<adp_work>

This points to a scratch working directory for the B<ADP> pipeline.  This is
assigned to the B<WORKDIR> environment variable by B<OPUS>.

=item B<alerts>

This points to a scratch directory to hold alerts for the alert
managment daemon. Assigned to B<ALERTS> by B<OPUS>.

=item B<parfiles>

This points to a scratch directory to hold par files.  Assigned to the
B<PARFILES> environment variable by B<OPUS>

=back

=head1 RESTRICTIONS

Any changes in the input file names will require changes in this
script since it matches on the file names.  Note that corresponding
changes will also have to be made in I<adpst.pl>.




=head1 REFERENCES

For further information on B<adpst.pl> or B<adpfin.pl> please run
perldoc on those files, ie, C<perldoc adpst.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

=head1 AUTHORS

Bruce O'Neel <bruce.oneel@obs.unige.ch>

Tess Jaffe <Theresa.Jaffe@obs.unige.ch>

Jake Wendt <jake.wendt@obs.unige.ch>

=cut

