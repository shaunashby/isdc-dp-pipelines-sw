#!perl

=head1 NAME

nrvst.pl - NRT Revolution Pipeline Start

=head1 SYNOPSIS

I<nrvst.pl> - Run from within B<OPUS>.  This is the first step of a 
six stage pipeline which processes files written into the revolution
directory of the repository, i.e. RRRR/rev.000/raw/.  

=head1 DESCRIPTION

This process recieves a trigger file in the input directory, written by
preprocessing with a specific format readable by OPUS.  (The file name
is too long for OPUS v 1.4, so it is truncated and the revolution number
also encoded in the trigger.  See the File Name Tech note.)  It then 
determines the file type and begins the processing of recognized files
by creating the OSF the rest of the pipeline processes will trigger on.
It ignores files which require no processing and errors when it encounteres
an unknown process.  Before processing, it checks whether the revolution
has already been triggered for processing, in which case it sends an alert
and quits.

This process is also responsible for triggering the previous revolution for
archive ingest.  It creates the OSF for the input file with a status of "p".  
Then, on receipt of a file for revolution N, this process checks 
if a trigger exists for revolution N-1.  If not, it determines if that 
revolution is ready for archiving by checking the finish status of all 
science windows and revolution files.  If they are all "c", it triggers.  
Otherwise, the check will be done again on receipt of the next rev file.  
When a problem is encountered in the checking or in writing the trigger, the 
OSF of the file which triggered the check is set to "x" and the errors 
written to its log.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OUTPATH>

This is set to the B<rii> entry in the path file.  It is the location 
of the repository, i.e. REP_BASE_PROD usually.

=item B<WORKDIR>

This is set to the B<rev_work> entry in the path file.  It is the 
location of the working directory, i.e. OPUS_WORK.

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the 
scw part of the repository.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the 
location of all log files seen by OPUS.  The real files are located
in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the 
location of the pipeline parameter files.  

=item B<ARC_TRIG>

This is set to the B<arcingest> entry in the path file and is where
to write archive ingest triggers.

=item B<REV_INPUT>

This is set to the B<rev_input> entry in the path file and is the input 
directory where triggers are written by preprocessing.

=item B<ALERTS>

This is set to the B<alerts> entry in the path file and is where to 
write alerts. 

=back

=cut

use strict;
use File::Basename;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use OPUSLIB;
use TimeLIB;
use Datasets;

my @results;
my $retval;
my @output;
my $scw;
my $time = &TimeLIB::MyTime();


my ($osfname,$path,$suffix) = &File::Basename::fileparse($ENV{EVENT_NAME}, '\..*');

print "\n========================================================================\n";


##########################################################################
# machinations to get correct environment variables through path file
##

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","ARC_TRIG","ALERTS","REV_INPUT","SCW_INPUT");

########################################################################
# machinations to get correct log file, link, and OSF name
##

my ($dataset,$type,$revno,$prevrev,$nextrev,$use,$vers) = &Datasets::RevDataset($osfname);

die "*******     ERROR:   Cannot find revolution $revno!" unless (-d "$ENV{SCWDIR}/$revno/rev.000/");
my $logfile = "$ENV{LOG_FILES}/$osfname.log";
my $reallogfile = "$ENV{SCWDIR}/$revno/rev.000/logs/$dataset";
$reallogfile =~ s/\.fits/_log\.txt/;
$reallogfile .= "_log.txt" if ( ($type =~ "arc") || ($type =~ "iii" ) );		#	040621 - Jake - SPR 3710
#	if (!-d "$ENV{SCWDIR}/$revno/rev.000/logs/") {
#		`$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/logs`;
#		die "*******     ERROR:  connot mkdir $ENV{SCWDIR}/$revno/rev.000/logs\n" if ($?);
#	}
&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{SCWDIR}/$revno/rev.000/logs" ) unless ( -d "$ENV{SCWDIR}/$revno/rev.000/logs" );
print "Dataset is $dataset.\nOSF is $osfname.\n";

my $error;
my $status;

#my $proc;
#$proc = "NRT" if ($ENV{PATH_FILE_NAME} =~ /nrtrev/);
#$proc = "CONS" if ($ENV{PATH_FILE_NAME} =~ /consrev/);
my $proc = &ISDCLIB::Initialize();
#	my $proc = &ProcStep();

##########################################################################
#  Look at what we got
##
if ($dataset) {
	
	print "\n***************************************************************\n"
		."----- $time:  Trigger $osfname.trigger received.\n"; 
	
	if ($use)  {
		print "----- $time:  Trigger $osfname;  processing beginning on file $dataset.\n";
	}
	else {
		# these aren't touched;  update rev triggering check below too!!
		print "----- $time:  Trigger $osfname.trigger recognized and ignored\n";
		print "***************************************************************\n\n";
		# code to set trigger to almost done;  run finish for archiving check.
		$status = "$osf_stati{REV_GEN_C}";
	}
	#########################################################################
	#  check that this revolution hasn't already been triggered for archiving
	#
	
	my @triggers =  sort(glob("$ENV{ARC_TRIG}/scw_${revno}rev0000.trigger*"));
	if (@triggers) {
		#  if so, quit with an error OSF;  don't want to process this file.
		print "----- $time:  ERROR  Revolution $revno already triggered when "
			."$dataset received!\nWriting OSF as error\n";
		$status = "$osf_stati{REV_ST_X}";
		$error = "revolution $revno already triggered for archive ingest",
	}
	else {
		$status = "$osf_stati{REV_ST_C}" unless ($status);
	}
} # end of if dataset recognized
else { # dataset not recognized
	$error = "OSF $osfname not recognized";
	$status = "$osf_stati{REV_ST_X}";
	$type = "err";
	$reallogfile = $logfile;
}


##########################################################################
#  
#  Check CONS case for RRRR_inp.done file:
if ($proc =~ /CONS/) {
	
	if (-e "$ENV{OPUS_WORK}/consrev/input/${revno}_inp.done") {
		print ">>>>>>>     Found ${revno}_inp.done;  status will be $status.\n";
	}
	else {
		# don't replace other status types like xww or ccw.  Only if cww
		$status = "$osf_stati{REV_GEN_H}" 
			if ($status =~ /^$osf_stati{REV_ST_C}/); 
		print ">>>>>>>     Did NOT find ${revno}_inp.done;  status will be $status.\n";
	}
} # end if cons



##########################################################################
#  Create the OSF to either start the pipeline or show the error
##

$retval = &ISDCPipeline::PipelineStart(
	"state"       => "$status",
	"type"        => "$type",
	"reallogfile" => "$reallogfile",
	"logfile"     => "$logfile",
	);

die "******     ERROR:  could not start Rev pipeline:  $retval" if ($retval);

&ISDCPipeline::PipelineStep(
	"step"         => "Rev startup ERROR",
	"program_name" => "ERROR",
	"error"        => "$error",
	"type"         => "$type",
	"logfile"      => "$logfile",
	) if ($status eq "$osf_stati{SCW_ST_X}");

print "\n========================================================================\n";

# write protect in case of ignored files:
&ISDCPipeline::PipelineStep(
	"step"         => "Rev startup:  write protection",
	"program_name" => "$mychmod -w $ENV{SCWDIR}/$revno/rev.000/raw/$dataset",
	"type"         => "$type",
	"logfile"      => "$logfile",
	) unless ($use);

exit 0;

##########################################################################
#                                DONE
##########################################################################

__END__ 

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level
Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

