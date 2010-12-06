#!perl

=head1 NAME

nrvfin.pl - NRT Revolution Pipeline FIN step script

=head1 SYNOPSIS

I<nrvfin.pl> - Run from within B<OPUS>.  This is the last step of a 
six stage pipeline which processes files written into the revolution
directory of the repository, i.e. RRRR/rev.000/raw/.  

=head1 DESCRIPTION

This process is triggered by the completion of the B<nrvosm> pipeline step
through the OSF. For each file type, it  moves the trigger file to "_done", 
removes the working subdirectory in the WORKDIR, and is finished.   

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

=item B<REV_INPUT>

This is set to the B<rev_input> entry in the path file and is the input 
directory where triggers are written by preprocessing.

=back

=cut

use strict;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use Datasets;
use lib "$ENV{ISDC_OPUS}/nrtrev/";
use Archiving;


##########################################################################
# machinations to get correct environment variables through path file
##
&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","REV_INPUT","SCW_INPUT","ARC_TRIG","ALERTS");

#########              set processing type:  NRT or CONS
my $proc = &ISDCLIB::Initialize();
#	my $proc = &ProcStep();
my $path = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "consrev" : "nrtrev";

#if ($ENV{PATH_FILE_NAME} =~ /cons/) {
#	$path = "consrev";
#}
#else {
#	$path = "nrtrev";
#}

#	040820 - Jake - SCREW 1533
#&Message ( "STARTING" );

##########################################################################
# machinations to get correct log file, link, and OSF name
##
my $osfname = $ENV{OSF_DATASET};
my ($dataset,$type,$revno,$prevrev,$nexrev,$use,$vers) = &Datasets::RevDataset("$osfname");
print "Dataset is $dataset\nOSF is $osfname\n";
my $logfile = "$ENV{LOG_FILES}/$osfname.log";
my $reallogfile = "$ENV{SCWDIR}/$revno/rev.000/logs/${dataset}_log.txt";
$reallogfile =~ s/\.fits//;


#  If NRT, nothing to do.  Arc checking done in nrvmon.
#  Otherwise, here we do checking for archiving in Cons.  This will probably
#   have to be redone as we figure out in reality how this will work.

if ($dataset =~ /arc_prep/) {
	#	050301 - Jake - What does this next line mean?
	# Can't do next steps after this one, which locks rev dir. 
	
	&Message ( "cleaning and archiving revolution $revno" );
	
	&Archiving::RevArchiving("$revno");
	
	print "\n========================================================================\n";
	exit;
} # if arc_prep


########################################################################
# Otherwise, just  finish up this trigger. 
#
&ISDCPipeline::PipelineStep(
	"step"         => "$proc - mv trigger file to _done",
	"program_name" => "$mymv $ENV{REV_INPUT}/$osfname.trigger_processing "
		."$ENV{REV_INPUT}/$osfname.trigger_done",
	) if (-e "$ENV{REV_INPUT}/$osfname.trigger_processing");

&ISDCPipeline::PipelineStep(
	"step"         => "$proc - mv trigger file to _done",
	"program_name" => "$mymv $ENV{REV_INPUT}/$osfname.trigger_bad "
		."$ENV{REV_INPUT}/$osfname.trigger_done",
	) if (-e "$ENV{REV_INPUT}/$osfname.trigger_bad");

&ISDCPipeline::PipelineFinish(); 

#	050301 - Jake - Why is this chmod after PipelineFinish?
&ISDCPipeline::RunProgram("$mychmod -w $reallogfile"); 

print "\n========================================================================\n";

#	unlink "/tmp/rfinjunk$$" if (-e "/tmp/rfinjunk$$"); #	060111 - Jake - don't think this is used anymore

exit 0;

########################################################################
##     DONE
########################################################################



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

