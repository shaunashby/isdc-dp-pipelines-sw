#!/usr/bin/perl -s

=head1 NAME

I<run_iii_prep.pl> - process iii_prep rev data

=head1 SYNOPSIS

I<run_iii_prep.pl>

=head1 DESCRIPTION

=item

=cut

use warnings;

use File::Basename;
use ISDCLIB;
use UnixLIB;

#	Use this script as ops_cons on a machine that can write to $REP_BASE_PROD/scw like ...
#	ops_cons@anaB6:~>echo $REP_BASE_PROD/
#	/isdc/cons/ops_1/

#	This will require the following processes to be running
#	crvmon
#	crvst
#	crvfin
#	crviii
#	crvarc
#	cleanosf
#	cleanopus


my $FUNCNAME    = "run_iii_prep.pl";
my $FUNCVERSION = "1.0";
my ($v,$version,$h,$help,$dry);

if ( $v || $version ) {
	print "Log_1  : Version : $FUNCNAME $FUNCVERSION\n";
	exit 0;
}
if ( ( $h || $help ) || ( ! $r ) ) {
	&Usage();
	exit 0;
}
my $revno = $r;

die "Rev $revno not found in the archive." 
	unless ( -d "/isdc/arc/rev_2/scw/$revno" );

die "Rev $revno already exists in current REP_BASE_PROD: $ENV{REP_BASE_PROD}"
	if ( -d "$ENV{REP_BASE_PROD}/scw/$revno" );

RunCom ( "rm /reproc/run/trigger/cons_rev/scw_${revno}rev0000.trigger" )
	if ( -e "/reproc/run/trigger/cons_rev/scw_${revno}rev0000.trigger" );

my $revdir = "$ENV{REP_BASE_PROD}/scw/$revno";
RunCom ( "mkdir $revdir" );
die "Didn't mkdir $revdir"
	unless ( -e "$revdir" );

chdir $revdir or die "Couldn't chdir to $revdir";
foreach $rev ( glob ( "/isdc/arc/rev_2/scw/$revno/0*" ) ) {
	my ($root,$path,$ext) = &File::Basename::fileparse($rev,'\..*');
	RunCom ( "ln -s $rev" );
#	RunCom ( "ln -s $rev $root.000" );
}

my $srcrev = &ISDCLIB::FindDirVers("/isdc/arc/rev_2/scw/$revno/rev");
RunCom ( "cp -rp $srcrev rev.000" );
RunCom ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_pp.done" );
RunCom ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_inp.done" );
RunCom ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_iii_prep.trigger" );
#	RunCom ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_arc_prep.trigger" );
RunCom ( "chmod 755 $revdir/rev.000/*" );
RunCom ( "chmod 644 $revdir/rev.000/logs/${revno}_iii_prep_log.txt" );
RunCom ( "chmod 644 $revdir/rev.000/logs/${revno}_arc_prep_log.txt" );

RunCom ( "mkdir $ENV{REP_BASE_PROD}/idx/scw" ) unless ( -e "$ENV{REP_BASE_PROD}/idx/scw" );
chdir "$ENV{REP_BASE_PROD}/idx/scw" or die "Couldn't chdir $ENV{REP_BASE_PROD}/idx/scw";
RunCom ( "ln -s /isdc/arc/rev_2/idx/scw/GNRL-SCWG-GRP-IDX.fits" )
	unless ( -e "/isdc/arc/rev_2/idx/scw/GNRL-SCWG-GRP-IDX.fits" );

foreach my $file ( qw/spi_cal_se_spectra.fits.gz spi_gain_coeff.fits.gz spi_cal_se_results.fits.gz spi_cal_me_spectra.fits.gz spi_cal_me_results.fits.gz/ ) {
	if ( -e "$revdir/rev.000/aca/$file" ) {
		RunCom ( "chmod +w $revdir/rev.000/aca/$file" );
		RunCom ( "rm $revdir/rev.000/aca/$file" );
	}
}

foreach my $file ( qw/exposure_report.fits.gz spi_psd_adcgain.fits.gz spi_psd_performance.fits.gz spi_psd_efficiency.fits.gz spi_psd_si.fits.gz/ ) {
	if ( -e "$revdir/rev.000/osm/$file" ) {
		RunCom ( "chmod +w $revdir/rev.000/osm/$file" );
		RunCom ( "rm $revdir/rev.000/osm/$file" );
	}
}

foreach my $file ( qw/picsit_fault_list_index.fits.gz spi_psd_si_index.fits.gz spi_psd_adcgain_index.fits.gz spi_psd_performance_index.fits.gz spi_psd_efficiency_index.fits.gz/ ) {
	if ( -e "$revdir/rev.000/idx/$file" ) {
		RunCom ( "chmod +w $revdir/rev.000/idx/$file" );
		RunCom ( "rm $revdir/rev.000/idx/$file" );
	}
}

foreach my $file ( qw/picsit_fault_list_*.fits.gz/ ) {
	if ( -e "$revdir/rev.000/cfg/$file" ) {
		RunCom ( "chmod +w $revdir/rev.000/cfg/$file" );
		RunCom ( "rm $revdir/rev.000/cfg/$file" );
	}
}

open  ERRORS, ">> $ENV{OPUS_HOME_DIR}/ignored_errors.cfg";
print ERRORS  "${revno}_arc_prep           am_cp                   25051\n";
close ERRORS;

exit;


######################################################################

sub RunCom {
	my ( $command ) = @_;
	print "$command\n";
	system ( "$command" ) unless ( $dry );
}


sub Usage {
	print 
		"\n\n"
		."Usage:  $FUNCNAME [options] \n"
		."\n"
		."  -v, -version -> version number\n"
		."  -dry         -> don't really do anything\n"
		."  -h, -help    -> this help message\n"
		."  -r=REVNO     -> revolution number (REQUIRED)\n"
		."\n\n"
		; # closing semi-colon
	return 0;
}


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

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

