#!/usr/bin/perl

=head1 NAME

I<sa_start.pl> - conssa script used to create the og and initiate processing

=head1 SYNOPSIS

I<sa_start.pl>

=head1 DESCRIPTION

This scripts reads the matching file from $OPUS_WORK/conssa/scratch and creates the OG followed by the OPUS trigger.

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use UnixLIB;
use ISDCLIB;

my ($obsid,$object,$split,$inst,$purpose,$purpose_master);
my @instruments;
my $ao;
my $command;
my ($retval,@result);
my $time;

########
#  Read in command line args:  
########
foreach (@ARGV) {
	if (/--h/) {
		print "\nUSAGE:  sa_start.pl obsid= [object=] [split=] [instrument=] [purpose=]\n\n"
			."\tDefault split is 001, instrument={IBIS,SPI,OMC,JMX1}, purpose=\"INST SA for AOn Obs. Object\", "
			."where og_create is called separately for each instrument.\n\n";
		#	Currently, putting multiple instruments in one OG is not supported.\n\n";
		exit;
	}
	elsif (/^obsid=(.*)$/) {
		$obsid = $1;
	}
	elsif (/^object=(.*)$/) {
		$object = $1;
	}
	elsif (/^split=(.*)$/) {
		$split = $1;
	}
	elsif (/^instrument=(.*)$/) {
		$inst = $1;
		$inst =~ tr/a-z/A-Z/;
	}
	elsif (/^purpose=(.*)$/) {
		$purpose = $1;
	}
	
} # foreach argument


############
#  Check the inputs
############

die ">>>>>>>     ERROR:  please specify at least an ObsID and either an Object name or a purpose." 
	unless ( (($object) || ($purpose)) && ($obsid));

if ($split) {
	$split = sprintf("%03d",$split);
}
else {
	$split = "001";
}

die ">>>>>>>     ERROR:  cannot find $ENV{OPUS_WORK}/conssa/scratch/${obsid}_${split}.txt" 
	unless (-e "$ENV{OPUS_WORK}/conssa/scratch/${obsid}_${split}.txt");

if ($purpose) {
	die ">>>>>>>     ERROR:  sorry, but for now, the purpose must be fewer than 32 characters.  "
		."Try again." if ($purpose =~ /^\S{32}.+$/);
}
elsif ($obsid =~ /^(\d{2})\d+$/) {
	$ao = $1;
	$ao =~ s/^0(\d)/$1/;
}
else {
	die ">>>>>>>     ERROR:  you didn't give a purpose, but I can't parse the AO number from "
		."the first two digits of the ObsID ${obsid}.  Try again.";
}

if ($inst) {
	@instruments=split ( ",", $inst );		#	040517 - Jake - SPR 3591
	foreach ( @instruments ) {					#	040517 - Jake - SPR 3591
		die ">>>>>>>     ERROR:  instrument must be IBIS, JMX1, JMX2, SPI, or OMC.  You specified $inst" 
			if ($_ !~ /^(ibis|jmx1|jmx2|spi|omc)$/i);
	}
}
else {
	@instruments = ("IBIS","JMX1","SPI","OMC");
}

chdir "$ENV{REP_BASE_PROD}" or die ">>>>>>>     ERROR:  cannot chdir to $ENV{REP_BASE_PROD}";

$purpose_master = $purpose;

############
#  Now loop over instruments
############

foreach $inst (@instruments) {
	my $in = &ISDCLIB::inst2in ( $inst );
	
	#
	# 17-Oct-2003 MB SPR-03258: correct handling of purpose with respect
	#                           to instrument
	#

	$purpose = ( $purpose_master ) ? $purpose_master : "${inst} SA for AO${ao} Obs. ${object}";

#	if ($purpose_master) {
#		$purpose=$purpose_master;
#	} else {
#		$purpose = "${inst} SA for AO${ao} Obs. ${object}";
#	}
	
	#  Ready to go:
	print ">>>>>>>     Running og_create for:\n";
	print ">>>>>>>                               ObsID==${obsid}\n";
	print ">>>>>>>                               ObsDir==obs\n";
	print ">>>>>>>                               Object==${object}\n";
	print ">>>>>>>                               Instrument==${inst}\n";
	print ">>>>>>>                               Split==${split}\n";
	print ">>>>>>>                               Purpose==${purpose}\n";
	
	$command = "og_create "
		."idxSwg=\"$ENV{OPUS_WORK}/conssa/scratch/${obsid}_${split}.txt\" "
		."instrument=\"${inst}\" "
		."ogid=\"so${in}_${obsid}_${split}\" "
		."baseDir=\"./\" "
		."obs_id=\"${obsid}\" "
		."obsDir=\"obs\" "
		."purpose=\"${purpose}\" "
		."keep=\"\" "
		."versioning=1";
	
	@result = &RunCom("$command");
	
	print ">>>>>>>     OG successfully created.  Now triggering pipeline.\n";
	$command = "$mytouch $ENV{OPUS_WORK}/conssa/input/so${in}_${obsid}_${split}.trigger";
	@result = &RunCom("$command");
	
	print ">>>>>>>     SA for OG so${in}_${obsid}_${split} started on ".&MyTime()."\n";
	
} # end foreach @instruments

print ">>>>>>>     DONE\n";



################################################################

=item B<RunCom> ( $com )

=cut

sub RunCom {
	
	my ($com) = @_;
	
	print ">>>>>>> ".&MyTime()." RUNNING:  \'$com\'\n";
	
	my @result;
	@result = `$com`;
	die ">>>>>>>     ERROR:\n@result" if ($?);
	
	open LOG,">>$ENV{OPUS_WORK}/conssa/logs/sa_start_log.txt" 
		or die ">>>>>>>     ERROR:  cannot open $ENV{OPUS_WORK}/conssa/logs/sa_start_log.txt to write!";
	print LOG "\n\n>>>>>>> ".&MyTime()." RUNNING:  \'$com\'\n";
	print LOG @result;
	print LOG ">>>>>>>     DONE.\n\n";
	close LOG;
	
	return;
	
}

################################################################

=item B<MyTime> ( )

Returns ( $date )

=cut

sub MyTime {
	my @date = localtime;  #  This is GM time on Ops, local on Office.
	$date[5] = $date[5] + 1900;
	$date[4] = $date[4] + 1;
	#  force two digit format
	foreach (@date){ $_ = "0${_}" if ($_ < 10); }
	# removed "(UTC)" when RIL did same;  SPR 493.
	#  my $date = "$date[5]-$date[4]-$date[3]T$date[2]:$date[1]:$date[0](UTC)";
	#  my $date = "$date[5]-$date[4]-$date[3]T$date[2]:$date[1]:$date[0]";
	my $date = "$date[3]/$date[4]/$date[5]";
	return $date;
}


=back

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

