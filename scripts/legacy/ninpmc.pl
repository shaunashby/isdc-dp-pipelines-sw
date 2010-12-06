#!perl

=head1 NAME

ninpmc.pl - NRT Input Monitor for Cleaning

=head1 SYNOPSIS

I<ninpmc.pl> - Run from within B<OPUS>.  This is a monitoring process
within the Input pipeline.  

=head1 DESCRIPTION

This script examines the creation dates of all OSFs on teh blackboard and 
marks those older than the age limit (see below) for cleaning by the 
cleanopus and cleanosf processes.  

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<LOG_FILES>

This is where it logs the alerts sent, set to /isdc/log/nrtinput.  

=item B<ALERTS>

This is where to write alerts, set to the "rttm_alerts" entry in the path file.

=item B<PARFILES>

=item B<OSF_AGELIMIT>

These are a set of variables controlling the age limits of OSFs, i.e.  OSFs older than these are marked for cleaning.

=back

=cut

use strict;
use ISDCPipeline;
use OPUSLIB;
use ISDCLIB;

&ISDCPipeline::EnvStretch ( "LOG_FILES", "ALERTS", "PARFILES" );

&ISDCPipeline::BBUpdate (
	"agelimit"  => "$ENV{OSF_AGELIMIT_DEFAULT}",
	"matchstat" => "$osf_stati{INP_COMPLETE}",
	);

&ISDCPipeline::BBUpdate (
	"agelimit" => "$ENV{OSF_AGELIMIT_ERRORS}",
	"errors"   => 1,
	);

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

