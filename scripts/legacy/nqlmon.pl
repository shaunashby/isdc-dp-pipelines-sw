#!/usr/bin/perl

=head1 NAME

nqlmon.pl - NRT QLA Pipeline Monitor

=head1 SYNOPSIS

I<nqlmon.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

This process monitors the ScW blackboard and marks OSFs for deletion after their respective timeouts.  See below.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over

=item B<OSF_AGELIMIT>

Days to keep around datasets of each type.  

=back

=cut

use strict;
use warnings;

use ISDCPipeline;
use OPUSLIB qw(:osf_stati);
use QLAMOS;

&ISDCPipeline::EnvStretch ( "OUTPATH", "WORKDIR", "LOG_FILES", "MOSAICS", "OBSDIR" );

&ISDCPipeline::BBUpdate (
	"agelimit"  => "$ENV{OSF_AGELIMIT_DEFAULT}",
	"matchstat" => "$osf_stati{QLA_COMPLETE}",
	);

&ISDCPipeline::BBUpdate (
	"agelimit" => "$ENV{OSF_AGELIMIT_ERRORS}",
	"errors"   => 1,
	);

my @triggers = glob ( "$ENV{OPUS_WORK}/nrtqla/input/????????????.trigger*" );
my ( $rev ) = ( $triggers[$#triggers] =~ /.*\/(\d{4})\d{8}\.trigger.*/ );

&QLAMOS::Mosaic ( sprintf ( "%04d", $rev-2 ) );
&QLAMOS::Mosaic ( sprintf ( "%04d", --$rev ) );
&QLAMOS::Mosaic ( sprintf ( "%04d", ++$rev ) );

exit 0;

##########################################################################

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

