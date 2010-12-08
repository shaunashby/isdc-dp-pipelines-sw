#!/usr/bin/perl -s

=head1 NAME

I<rerun_frss_files.pl> - reprocess ingested jemx rev data

=head1 SYNOPSIS

I<rerun_frss_files.pl>

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
#	crvjmf
#	crvfin
#	crviii
#	crvarc
#	cleanosf
#	cleanopus


#	arc_prep will fail because of the 
#>>    Date: 20051107111430  OSF: 0039_arc_prep                          Stat: cxw     Last: idx_find               Exit: See next line...
#>>    ...Index selection REVOL == 0039 resulted in no match from index /reproc/cons/ops_1/idx/scw/GNRL-SCWG-GRP-IDX.fits
#
#	just osf_update to arc_prep to ccw


my $FUNCNAME    = "rerun_frss_files.pl";
my $FUNCVERSION = "1.4";
my ($v,$version,$h,$help);

if ( $v || $version ) {
	print "Log_1  : Version : $FUNCNAME $FUNCVERSION\n";
	exit 0;
}
if ( ( $h || $help ) || ( ! $r ) ) {
	&Usage();
	exit 0;
}

my $revno = $r;

#die "No -r REVNO given" unless ( $revno );
die "Revno $revno does not exist in /isdc/arc/rev_2/scw/" unless ( -d "/isdc/arc/rev_2/scw/$revno" );


#	version 1.1
#	copy entire /isdc/arc/rev_2/scw/${revno}/rev.??? to $ENV{REP_BASE_PROD}/scw/${revno}/rev.000

die "Revdir $revno already exists!!!" if ( -d "$ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/" );

my $revdir = &ISDCLIB::FindDirVers("/isdc/arc/rev_2/scw/$revno/rev");

die "Could not FindDirVers(/isdc/arc/rev_2/scw/$revno/rev)" unless ( $revdir );

print "Copying all the rev data from the archive.\n";
system ( "mkdir -p $ENV{REP_BASE_PROD}/scw/$revno" ) unless ( -d "$ENV{REP_BASE_PROD}/scw/$revno" );
system ( "cp -pr $revdir $ENV{REP_BASE_PROD}/scw/$revno/rev.000" );

print "Removing the jemx raw frss logs and the aca data and idx.\n";
system ( "$mychmod -R +w $ENV{REP_BASE_PROD}/scw/$revno/rev.000/logs" );
system ( "$myrm -f $ENV{REP_BASE_PROD}/scw/$revno/rev.000/logs/jemx?_raw_frss*.txt" );
system ( "$mychmod -R +w $ENV{REP_BASE_PROD}/scw/$revno/rev.000/aca" );
system ( "$myrm -f $ENV{REP_BASE_PROD}/scw/$revno/rev.000/aca/jemx?_aca_frss*.fits*" );
system ( "$mychmod -R +w $ENV{REP_BASE_PROD}/scw/$revno/rev.000/idx" );
system ( "$myrm -f $ENV{REP_BASE_PROD}/scw/$revno/rev.000/idx/jemx?_aca_frss_index*.fits*" );
system ( "$myrm -f $ENV{REP_BASE_PROD}/scw/$revno/rev.000/idx/jemx?_raw_frss_index*.fits*" );

system ( "$mychmod +w $ENV{REP_BASE_PROD}/scw/$revno/rev.000/raw" );

#	end version 1.1 addition


#system ( "mkdir -p $ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/" ) unless ( -d "$ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/" );

#`cmp /isdc/arc/rev_2/idx/scw/GNRL-SCWG-GRP-IDX.fits $ENV{REP_BASE_PROD}/idx/scw/GNRL-SCWG-GRP-IDX.fits`;
#system ( "cp /isdc/arc/rev_2/idx/scw/GNRL-SCWG-GRP-IDX.fits $ENV{REP_BASE_PROD}/idx/scw/" ) if ( $? );

system ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_pp.done" );

foreach my $file ( `/bin/ls $revdir/raw/*frss*` ) {
	chomp ( $file );
	#print "FILE: $file\n" ;	# $revdir/raw/jemx2_raw_frss_20030223130049_00.fits.gz
	my ($root,$path,$ext) = &File::Basename::fileparse($file,'\..*');
	#print "ROOT: $root\n" ;	# jemx2_raw_frss_20030224162745_00

	my ( $dateinfo ) = ( $root =~ /_(\d{14}_\d{2})/ );

	my $suffix = ( $root =~ /jemx1/ ) ? "jm1" : "jm2";

#	system ( "cp $file $ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/" );
	system ( "gunzip   $ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/$root*gz" );
	system ( "chmod +w $ENV{REP_BASE_PROD}/scw/${revno}/rev.000/raw/$root*" );

	# /reproc/run/pipelines/cons/consrev/input/0208_20040627140320_00_jm2.trigger
	print "touching $ENV{OPUS_WORK}/consrev/input/${revno}_${dateinfo}_${suffix}.trigger\n";
	system ( "touch $ENV{OPUS_WORK}/consrev/input/${revno}_${dateinfo}_${suffix}.trigger" );
}


sub Usage {
	print 
		"\n\n"
		."Usage:  $FUNCNAME [options] \n"
		."\n"
		."  -v, -version -> version number\n"
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

