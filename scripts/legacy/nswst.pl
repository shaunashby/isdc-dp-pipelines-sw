#!/usr/bin/perl

=head1 NAME

nswst.pl - NRT Science Window Pipeline Start

=head1 SYNOPSIS

I<nswst.pl> - Run from within B<OPUS>.  This is the first step of a six stage pipeline which processes science windows.  

=head1 DESCRIPTION

This process recieves a trigger file in the input directory, written by the NRT Input pipeline using the science window ID.  It simply creates the OSF and initialized the log file to start the pipeline going.  It checks first if the science window ID has already been triggered for archive ingest, and errors off if so.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location of the repository, i.e. REP_BASE_PROD usually.

=item B<WORKDIR>

This is set to the B<nrt_work> entry in the path file.  It is the location of the working directory, i.e. OPUS_WORK.

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the scw part of the repository.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the location of all log files seen by OPUS.  The real files are located in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the location of the pipeline parameter files.  

=item B<ARC_TRIG>

This is set to the B<arcingest> entry in the path file and is where to write or check for archive ingest triggers.

=item B<SCW_INPUT>

This is set to the B<rii_input> entry in the path file and is where the input triggers are written by the NRT Input pipeline.  

=back

=cut


use strict;
use warnings;

use File::Basename;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use OPUSLIB qw(:osf_stati);

my $retval;

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","SCW_INPUT","ARC_TRIG","SCW_INPUT","INP_INPUT","REV_INPUT");

my ($scwid, $path, $suffix) = &File::Basename::fileparse($ENV{EVENT_NAME}, '\..*');

my $proc = &ISDCLIB::Initialize();

########################################################################
#########              check that not already triggered
my @trigger = glob("$ENV{ARC_TRIG}/scw_$scwid*");
my $status = ( @trigger ) ? $osf_stati{SCW_ST_X} : $osf_stati{SCW_ST_C};
my $revno = &ISDCPipeline::RevNo("$scwid");


##########################################################################
#  
#  Check CONS case for RRRR_rev.done file:
if ($proc =~ /CONS/) {
	
	if (-e "$ENV{REV_INPUT}/${revno}_rev.done") {
		print ">>>>>>>     Found ${revno}_inp.done;  status will be $status.\n";
	}
	else {
		# don't replace other status types like xww or ccw.  Only if cww
		$status = $osf_stati{SCW_DP_H} if ($status =~ /^$osf_stati{SCW_ST_C}/); 	
		print ">>>>>>>     Did NOT find ${revno}_inp.done;  status will be $status.\n";
	}
	
} # end if cons



########################################################################
#########             Initialize log file

# copy inp log to beginning of scw log
my $input_log_file = "$ENV{SCWDIR}/$revno/$scwid.000/${scwid}_inp.txt";
my $scw_log_file   = "$ENV{SCWDIR}/$revno/$scwid.000/${scwid}_scw.txt";

&ISDCPipeline::RunProgram("$mycp $input_log_file $scw_log_file ") 
	if (-e "$input_log_file");
&ISDCPipeline::RunProgram("$mychmod u+w $scw_log_file") 
	if (-e "$scw_log_file");


########################################################################
#########              Create OSF
$retval = &ISDCPipeline::PipelineStart(
	"pipeline" => "$proc ScW Start",
	"state"    => "$status",
	);
if ( $retval ) {
	#	050617 - Jake - added all these debugging lines because we've had some failed PipelineStarts and don't know why
	print "#######\n";
	print "#######\n";
	print "#######\n";
	print "#######     DEBUG : in nrtscw/nswst.pl after PipelineStart\n";
	print "#######     DEBUG : \$proc is +$proc+\n";
	print "#######     DEBUG : \$revno is +$revno+\n";
	print "#######     DEBUG : \$scwid is +$scwid+\n";
	print "#######     DEBUG : \$input_log_file is +$input_log_file+\n";
	print "#######     DEBUG : \$scw_log_file is +$scw_log_file+\n";
	print "#######     DEBUG : \$status is +$status+\n";
	print "#######     DEBUG : \$retval is +$retval+\n";
	print "#######\n";
	print "#######\n";
	print "#######\n";
	die ">>>>>>>     ERROR:  cannot start pipeline";
}

# now reset it:
$retval = 1 if ($status =~ /x/);

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - ERROR",
	"program_name" => "ERROR",
	"error"        => "science window $scwid already triggered for archive ingest",
	"logfile"      => "$ENV{LOG_FILES}/$scwid.log",
	"dataset"      => "$scwid",
	) if ($retval);

exit;

########################################################################

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

