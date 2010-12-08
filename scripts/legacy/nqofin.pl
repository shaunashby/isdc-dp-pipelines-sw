#!/usr/bin/perl

=head1 NAME

I<nqlfin.pl> - nrtqla pipeline FIN step script

=head1 SYNOPSIS

I<nqlfin.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

Write protect all data, add this OSF to the GNRL-OBSG-GRP-IDX and copy alerts.

This also will move the trigger to trigger_done, which I found completely unnecessary.  Especially since this single trigger is associated with 3 actual OSFs which means that it is probably NOT actually done.  I have also modified the cleanup script to delete the trigger file when the first OSF is cleaned.  I think that this functionality should be removed or the whole idea of nrtqla processing should be rethought.  

Also, the triggers for the slews, since we only ever process the pointings continue to remain in the input directory.  This should also be rethought.

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use QLALIB;

my ($retval,@result);

print "\n========================================================================\n";
print "*******     Trigger $ENV{OSF_DATASET} received\n";

&ISDCPipeline::EnvStretch ( "LOG_FILES","OUTPATH","WORKDIR","OBSDIR","PARFILES","ALERTS" );

my ( $obsid, $revno, $inst, $INST, $og, $pdefv ) = &QLALIB::ParseOSF ( $ENV{OSF_DATASET} );
my $proc = &ProcStep()." $INST";

&Message ( "$proc - STARTING" );

print "*******     Obs is $obsid;  Instrument is $INST;  group is $og.\n";

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - write protect results",
	"program_name" => "$mychmod -R -w *fits scw", 
	"subdir"       => "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000",
	);

#  Move alerts into logs subdirectory
&ISDCPipeline::PipelineStep (
	"step"         => "$proc - move alerts to logs dir",
	"program_name" => "$mymv *alert* logs",
	"subdir"       => "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/",
	) if ( `$myls $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/*alert* 2> /dev/null` );

&Message ( "$proc - write protect obs dir" );

##  Now recursively write protect, and hereafter log  only to process log
( $retval, @result ) = &ISDCPipeline::RunProgram ( "$mychmod -R -w $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/" );

die "*******  ERROR:  cannot write protect $ENV{OBSDIR}/$ENV{OSF_DATASET}.000:\n@result" if ( $retval );

exit;

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
