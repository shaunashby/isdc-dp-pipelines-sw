#!/usr/bin/perl

=head1 NAME

adpmon.pl - ISDC Auxiliary Data Preparation Pipeline, file monitor task.

=head1 SYNOPSIS

I<adpmon.pl> - Run from within B<OPUS>.  The only purpose of this task is to correct a mismatch between B<OPUS> and the files that ADP needs to process.  

=head1 DESCRIPTION

I<adpmon.pl> - Run from within B<OPUS>.  The fist purpose of this task is to correct a mismatch between B<OPUS> and the files that ADP needs to process.  B<OPUS> can only trigger on a file extension so therefore it won't properly trigger on files named either orbita or revno. This task watches for those files and renames them.

The second purpose of this task is to monitor the contents of the input directory and send an alert if a known file type is not matched. 

=cut


use strict;
use warnings;

use ISDCPipeline;
use OPUSLIB;
use ISDCLIB;
use UnixLIB;
use TimeLIB;

&ISDCPipeline::EnvStretch("OUTPATH","ADP_INPUT","LOG_FILES","ALERTS","WORKDIR","PARFILES");

my @allfiles = sort(glob("$ENV{ADP_INPUT}/*"));

foreach my $file (@allfiles) {

	print "Processing $file\n";
	
	if (($file =~ /revno$/) || ($file =~ /orbita$/)) {
		my $revnofile  = "$ENV{ADP_INPUT}/revno";
		my $orbitafile = "$ENV{ADP_INPUT}/orbita";
		
		if (-e $orbitafile) {
			my $newfile = "$orbitafile.orbita";
			`$mymv -f $orbitafile $newfile`;
			print "Moved $orbitafile to $newfile\n";
		}  
		
		if (-e $revnofile) {
			my $newfile = "$revnofile.revno";
			`$mymv -f $revnofile $newfile`;
			print "Moved $revnofile to $newfile\n";
		}  
	}
	# other known file types triggered normally by adpst
	elsif ($file =~ /pad_([0-9]{2})_.*/){}
	elsif ($file =~ /THF_(\d{6}_\d{4})/){}
	elsif ($file =~ /iop_([0-9]{2}).*/) {}
	elsif ($file =~ /pod_([0-9]{4})_.*/) {}
	elsif ($file =~ /opp_([0-9]{4})_.*/) {}
	elsif ($file =~ /ocs_([0-9]{2}).*/) {}
	elsif ($file =~ /([0-9]{4})_([0-9]{2})\.PAF/) {}
	elsif ($file =~ /([0-9]{4})_([0-9]{4})\.(ASF|AHF)/) {}
	elsif ($file =~ /orbita.*/) {}
	elsif ($file =~ /revno.*/) {}
	elsif ($file =~ /.*OLF/) {}
	elsif ($file =~ /TSF_([0-9]{4})_.*_([0-9]{4}).*INT/) {}
	elsif ($file =~ /(_processing|_work|_bad)/) {}
	elsif ($file =~ /.*\/odd$/) {}
	elsif ($file =~ /lock/) {}
	elsif ($file =~ /arc_prep/) {}
	else {
		$ENV{OSF_DATASET} = "odd_files";
		# move odd file out of trigger directory
		print "Got an odd file $file;  moving to $ENV{ADP_INPUT}/odd and sending alert\n";
		&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ADP_INPUT}/odd" ) unless ( -d "$ENV{ADP_INPUT}/odd");
		my ($mvresult,@mvoutput) = &ISDCPipeline::RunProgram("$mymv $file $ENV{ADP_INPUT}/odd","quiet");

		&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{WORKDIR}/adpmon" ) unless ( -d "$ENV{WORKDIR}/adpmon");
		chdir("$ENV{WORKDIR}/adpmon") or die "Cannot chdir to $ENV{WORKDIR}/adpmon";
		# must clean out old alerts first, because need to run am_cp from same
		#  place whenever we get an odd file.  
		&ISDCPipeline::RunProgram("$myrm -f *alert*",1);
		
		if ($mvresult) {
			&ISDCPipeline::WriteAlert(
				"message" => "Unknown file $file appeared in ADP input directory and could not be moved to odd/",
				"level"   => 1,
				"subdir"  => "$ENV{WORKDIR}/adpmon",
				"id"      => "511",
				);
		} else {
			&ISDCPipeline::WriteAlert(
				"message" => "Unknown file $file appeared in ADP input directory",
				"level"   => 1,
				"subdir"  => "$ENV{WORKDIR}/adpmon",
				"id"      => "501",
				);
		}

		my ($retval,@output) = &ISDCPipeline::RunProgram("am_cp OutDir=$ENV{ALERTS} OutDir2= Subsystem=ADP DataStream=realTime");
		die "Cannot copy alert: @output" if ( $retval );
		die "Cannot move $file: @mvoutput" if ( $mvresult );

		my $time = &TimeLIB::MyTime();
		print "File $file moved to $ENV{ADP_INPUT}/odd and alert sent at $time\n";
		delete $ENV{COMMONLOGFILE};
	} # else no known type
} # foreach $file ...

#############################################################################
#
#  Clean blackboard:
#############################################################################
print "Checking BB for old OSFs to clean.\n";

&ISDCPipeline::BBUpdate(
	"agelimit"  => "$ENV{OSF_AGELIMIT_DEFAULT}",
	"matchstat" => "$osf_stati{ADP_COMPLETE}",
	"exclude"   => "arc_prep",
	);

# THF OSFs are useless, so delete more quickly
&ISDCPipeline::BBUpdate(
	"agelimit"  => "$ENV{OSF_AGELIMIT_THF}",
	"matchstat" => "$osf_stati{ADP_COMPLETE}",
	"type"      => "thf",
	);

#  Note:  for ADP, errors are never cleaned.  They must be fixed.
&ISDCPipeline::BBUpdate(
	"agelimit"  => "$ENV{OSF_AGELIMIT_ARCHIVED}",
	"matchstat" => "$osf_stati{ADP_COMPLETE}",
	"match"     => "arc_prep",
	);

exit 0;

=head1 ACTIONS

=over 5

=item B<orbita>

When an orbita file is found in the B<ADP_INPUT> directory it is renamed to orbita.orbita.

=item B<revno>

When an revno file is found in the B<ADP_INPUT> directory it is renamed to revno.revno.

=item B<fits|PAF|ASF|AHF|INT|OLF|opp|tar>

Any of these file types will be ignored and left for adpst.pl to trigger on.

=item B<odd>

If any file type is received which does not match one of the above patterns, it will be moved to an "odd" subdirectory and an alert will be sent.

=head1 RESOURCE FILE

The resource file for I<adpmon.pl> makes most sense if it causes I<adpmon.pl> to be triggered on a regular basis.  Currently this trigger is every 30 secs.  I<adpmon.pl> does not take long to run so this should not cause any system load problems.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<OUTPATH>

This is the top of the repository, set to the B<rii> entry in the path file.

=item B<LOG_FILES>

This is the central log file directory, set to the B<log_files> entry in the path file.

=item B<ALERTS>

This is where to write alerts, set to the B<alerts> entry in the path file.

=item B<ADP_INPUT> 

This is the input directory where IFTS deposits Auxiliary Data, set to the B<adp_input> entry in the path file.  

=back


=head1 RESTRICTIONS

Only one copy of this script should run on the whole operations cluster of machines per I<ADP_INPUT> directory.  In general this means that only one copy of the ADP pipeline should be running unless one is quite careful.



=head1 REFERENCES

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

=head1 AUTHORS

Bruce O'Neel <bruce.oneel@obs.unige.ch>

Tess Jaffe <Theresa.Jaffe@obs.unige.ch>

Jake Wendt <jake.wendt@obs.unige.ch>

=cut

