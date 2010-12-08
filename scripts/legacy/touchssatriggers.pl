#!/usr/bin/perl

foreach ( `cat scws_Crab_all-sorted.txt` ) {
	chomp;
	next if ( /^\s*\#/ );	#	comment
	next if ( /^\s*$/ );		#	blank line
	my ( $scwid ) = /(\d{12})/;
	print "touch $ENV{OPUS_WORK}/consssa/input/${scwid}_jmx1.trigger\n";
	print `touch $ENV{OPUS_WORK}/consssa/input/${scwid}_jmx1.trigger`;
}
