#!perl -w

use strict;
use lib "$ENV{ISDC_OPUS}/pipeline_lib/";
use lib "$ENV{ISDC_OPUS}/nrtqla/";
use QLALIB;

my ( $obsid, $revno, $inst, $INST, $og, $pdefv ) = &QLALIB::ParseOSF ( "qmj2_0557_04200340023_0007" );
print "pdefv: $pdefv\n" if $pdefv;

( $obsid, $revno, $inst, $INST, $og, $pdefv ) = &QLALIB::ParseOSF ( "qsj1_055800240010" );
print "pdefv: $pdefv\n" if $pdefv;

