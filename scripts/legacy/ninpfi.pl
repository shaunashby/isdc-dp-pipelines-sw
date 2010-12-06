#!perl

=head1 NAME

ninpfi.pl - NRT Input Pipeline Finish

=head1 SYNOPSIS

I<ninpfi.pl> - Run from within B<OPUS>.  This is the last step of a three 
stage pipeline which processes raw science windows written by Pre-Processing.

=head1 DESCRIPTION

This process simply closes the log file,  sets the status of the observation
to completed, and writes a trigger for the Science Window Pipeline.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

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

=item B<SCW_INPUT>

This is set to the B<scw_input> entry in the path file and is where the
triggers are written for the Science Window Pipeline.  

=back

=cut


use strict;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;

&ISDCPipeline::EnvStretch ( "SCWDIR", "LOG_FILES", "PARFILES", "SCW_INPUT" );

my $scw_trigger = "$ENV{SCW_INPUT}/$ENV{OSF_DATASET}.trigger";

my $proc = &ISDCLIB::Initialize();
#my $proc = &ProcStep();
my $path = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "consinput" : "nrtinput";
#my $path = ( $ENV{PATH_FILE_NAME} =~ /consscw/ ) ? "consinput" : "nrtinput";		#	I think that this is a typo

my $inp_trigger = "$ENV{OPUS_WORK}/$path/input/$ENV{OSF_DATASET}";

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - trigger the ScW pipeline with $scw_trigger",
	"program_name" => "$mytouch $scw_trigger",
	"type"         => "inp",
	);

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - rename scw trigger file to _done",
	"program_name" => "$mymv $inp_trigger.trigger_processing $inp_trigger.trigger_done",
	"type"         => "inp",
	) if ( -e "$inp_trigger.trigger_processing" );

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - rename scw trigger file to _done",
	"program_name" => "$mymv $inp_trigger.trigger_bad $inp_trigger.trigger_done",
	"type"         => "inp",
	) if ( -e "$inp_trigger.trigger_bad" );

&ISDCPipeline::PipelineFinish (
	"pipeline" => "Input Pipeline",
	"type"     => "inp",
	);

my $revno   = &ISDCPipeline::RevNo ( $ENV{OSF_DATASET} );
my $logfile = "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/$ENV{OSF_DATASET}_inp.txt";
print "logfile is $logfile;  locking\n";
&ISDCPipeline::RunProgram ( "$mychmod -w $logfile" );
print "logfile locked;  finished\n\n";

exit 0;


######################################################################


__END__ 

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the Input Pipeline, please see the Input 
Pipeline ADD.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

