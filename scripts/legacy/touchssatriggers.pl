#!perl

foreach ( `cat scws_Crab_all-sorted.txt` ) {
	chomp;
	next if ( /^\s*\#/ );	#	comment
	next if ( /^\s*$/ );		#	blank line
#	print "$_\n";
	my ( $scwid ) = /(\d{12})/;
#	print "${scwid}_jmx1\n";
	print "touch $ENV{OPUS_WORK}/consssa/input/${scwid}_jmx1.trigger\n";
	print `touch $ENV{OPUS_WORK}/consssa/input/${scwid}_jmx1.trigger`;
}
