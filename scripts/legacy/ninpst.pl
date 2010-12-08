#!/usr/bin/perl

=head1 NAME

ninpst.pl - NRT Input Pipeline Start

=head1 SYNOPSIS

I<ninpst.pl> - Run from within B<OPUS>.  This is the first step of a three 
stage pipeline which processes raw science windows written by Pre-Processing.  

=head1 DESCRIPTION

This process receives a trigger file in the input directory, written by 
Pre-Processing, using the science window ID.  It simply  creates the OSF
and initialized the log file to start the pipeline going. 

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

=item B<INPUT>

This is set to the B<nrtinput_input> entry in the path file and is where the
input triggers are written by the NRT Input pipeline.  

=back

=cut

use strict;
use warnings;

use ISDCPipeline;
use OPUSLIB qw(:osf_stati);

&ISDCPipeline::EnvStretch ( "SCWDIR", "LOG_FILES", "PARFILES" );

my $status = &ISDCPipeline::PipelineStart(
	"pipeline" => "Input Pipeline Start",
	"state"    => "$osf_stati{INP_ST_C}",
	"type"     => "inp",
	);

die "*******     ERROR:  cannot start pipeline" 
	if ( $status );

exit 0;

######################################################################

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

