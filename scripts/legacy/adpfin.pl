#!/usr/bin/perl

=head1 NAME

adpfin.pl - ISDC Auxiliary Data Preparation Pipeline, finish task

=head1 SYNOPSIS

I<adpfin.pl> - Run from within B<OPUS>.  This is the last step of a
three stage pipeline which does Auxiliary Data Preparation.  The
first step is B<adpst.pl> and the second step is B<adp.pl>.

=head1 DESCRIPTION

I<adpfin.pl> - Run from within B<OPUS>.  This is the last step of the
three step ADP pipeline.  The purpose of this step is write the
closing log entries and then exit.  If it turns out that additional
cleanup needs to be done for the B<ADP> pipeline then these steps
could be put here.

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;

&ISDCPipeline::EnvStretch ( "OUTPATH", "WORKDIR", "AUXDIR", "LOG_FILES", "PARFILES" );

my $opuslink = "$ENV{OSF_DATASET}.log";

print "OPUS link is $opuslink\n";

&ISDCPipeline::PipelineStep (
	"step"         => "adpfin",
	"program_name" => "NONE",
	"type"         => "adp",
	"pipeline"     => "adp",
	);

&ISDCPipeline::PipelineFinish (
	"pipeline" => "ADP",
	"type"     => "adp",
	);

my $reallogfile = readlink ( "$ENV{LOG_FILES}/$opuslink" );

&ISDCPipeline::RunProgram ( "$mychmod 444 $reallogfile" );

exit 0;

=back

=head1 RESOURCE FILE

The resource file for I<adpfin.pl> contains all the environment
variables which are used from the path file for the B<ADP> pipeline.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<OUTPATH>

This is the top of the repository, set to the B<rii> entry in the path file.

=item B<AUXDIR> 

This is the location of the auxiliary data repository.

=item B<WORKDIR>

This is the location of a scratch work directory.

=item B<LOG_FILES>

This is the central log file directory, set to the B<log_files> entry in the 
path file.

=back

=head1 RESTRICTIONS

No current restrictions.




=head1 REFERENCES

For further information on B<adpst.pl> or B<adp.pl> please run
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


