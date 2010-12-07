#!/usr/bin/perl -w

use strict;
use lib "/isdc/integration/isdc_int/sw/dev/prod/opus/pipeline_lib";
use SSALIB;

foreach ( qw/0000_Crab_isgri.trigger 0000_Plane_isgri.trigger/ ) {
	print "$_\n\n";
	print SSALIB::Trigger2OSF ( $_ );
	print "\n------------------\n\n\n";
}
