package TimeLIB;

=head1 NAME

I<TimeLIB.pm> - generic Time related functions

=head1 SYNOPSIS

use I<TimeLIB.pm>;
used by many B<ISDC> scripts due to it's generic, er, osity?

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut


#	This function does not and should not ever need an other ISDC perl package or module.
#	This way, it will remain simple and easily maintainable without any crossover.

use Time::Local;

sub TimeLIB::MyTime;
sub TimeLIB::MyTimeSec;
sub TimeLIB::UTCops;
sub TimeLIB::HexTime2Local;

$| = 1;


##############################################################################

=item B<MyTime> ( )

idiotic function to get the time in a reasonable format

This calls the Perl function localtime and prints it in a format consistent with RIL output.  (This will be GMT on the Operational network and local time on the Office.)

returns ( $date )

=cut

sub MyTime {
	my @date = localtime;  #  This is GM time on Ops, local on Office.
	$date[5] = $date[5] + 1900;
	$date[4] = $date[4] + 1;
	#  force two digit format
	foreach (@date){ $_ = "0${_}" if ($_ < 10); }
	# removed "(UTC)" when RIL did same;  SPR 493.
	my $date = "$date[5]-$date[4]-$date[3]T$date[2]:$date[1]:$date[0]";
	return $date;
}


##############################################################################

=item B<MyTimeSec> ( )

second idiotic function to get time in an unreasonable formate, i.e straight non-leap seconds since Jan 1, 1970 UTC

returns ( time )

=cut

sub MyTimeSec {
	my $result = time;
	return $result;
}

##############################################################################

=item B<UTCops> ( %att )

returns join '', (@cdate)[5,4,3,2,1,0];

=cut

sub UTCops {
	#
	#  UTCops("now" => "time in seconds","delta" => "seconds to subtract")
	# 
	#  or
	#
	#  UTCops("utc" => "YYYYMMDDHHMMSS","delta" => "seconds to subtract")
	#
	#  Returns UTC format YYYYMMDDHHMMSS, minus given delta seconds [optional].  
	#     If no start time is given, it calls time to get the non-leap seconds 
	#     since Jan1, 1970, UTC.
	#
	#  Note that the delta parameter can be negative, to *add* seconds.  
	#
	my %att = @_ ;
	my ($year,$mon,$mday,$hour,$min,$sec);
	$att{delta} = 0 unless (defined $att{delta});
	# non-leap seconds since Jan1, 1970, UTC
	$att{now} = time unless (defined $att{now} || defined $att{utc});

	if (defined $att{utc}) {
		#              1:YYYY   2:MM   3:DD   4:hh   5:mm    6:ss
		$att{utc} =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/;
		($year,$mon,$mday,$hour,$min,$sec) = ($1,$2,$3,$4,$5,$6);
		$year -= 1900;  #  timelocal takes a year which has 1900 subtracted
		$mon -= 1;  # timelocal takes months 0..11

		#  timelocal(ss,mm,hh,DD,MM,YY) 
		#  returns like time above, non-leap seconds since Jan1, 1970, UTC
		#  corresponding to the input.  
		$att{now} = &Time::Local::timelocal($sec,$min,$hour,$mday,$mon,$year);
	}

	#  Now, whether given nothing (now), a time in seconds to be called "now",
	#   or a time in UTC to be called "now", take that "now", apply the delta,
	#   and convert the result back to YYYYMMDDHHMMSS.  

	my @cdate = localtime ($att{now} - $att{delta} );  # converts to 
	# @cdate = (sec,min,hour,mday,mon,year,wday,yday,isdst)
	#  in local time zone (which is GMT on Ops net.)

	$cdate[5] += 1900;  #  since localtime returns year - 1900
	$cdate[4] += 1;  # since localtime returns months as 0..11
	#  Tack on zero if necessary
	foreach ($cdate[0],$cdate[1],$cdate[2],$cdate[3],$cdate[4]) {
		$_ = "0".$_ if (/^\d$/);
	}

	#  Returns string YYYYMMDDHHMMSS
	return join '', (@cdate)[5,4,3,2,1,0];
}  # end sub UTCops


##############################################################################

=item B<HexTime2Local> ( $hextime )

returns $utc[5].$utc[4].$utc[3].$utc[2].$utc[1].$utc[0];

=cut

sub HexTime2Local {
	my ($hextime) = @_;
   
	#  Can't use gmtime here, since that won't work on Office ne!
	my @utc = localtime  hex $hextime;
   
	$utc[5] = $utc[5] + 1900;
	$utc[4] = $utc[4] + 1;
	#  force two digit format
	foreach (@utc){ $_ = "0${_}" if ($_ < 10); }
	return $utc[5].$utc[4].$utc[3].$utc[2].$utc[1].$utc[0];
} # end HexTime2Local

1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
