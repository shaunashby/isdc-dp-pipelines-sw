#!/usr/bin/perl

use strict;
use warnings;

use File::Basename;

#	Handy script for quickly removing all the stray IDX files that build up.

my $FUNCNAME = "cleanpvphaseindices.pl";
my $FUNCVERSION = "1.0";
my ($root,$path,$ext);
my $num2keep = 2;
my @idxreals;

#	loop through all parameters that begin with - until one doesn't have a leading - 
if ( defined ($ARGV[0]) ) {
	while ($_ = $ARGV[0], /^-.*/) {
		if ( /-h/ ) {
			print 
				"\n\n"
				."Usage:  $FUNCNAME  [options] file(s)\n"
				."\n"
				."  -v, --v, --version -> version number\n"
				."  -h, --h, --help    -> this help message\n"
				."     by default, parcheck only compares parameter names\n"
				."\n\n"
			; # closing semi-colon
			exit 0;
		}
		elsif ( /-v/ ) {
			print "Log_1  : Version : $FUNCNAME $FUNCVERSION\n";
			exit 0;
		}
		else {  # all other cases
			print "ERROR: unrecognized option +$ARGV[0]+.  Aborting...\n";
			print "\n";
#			exit 1;
		}
		shift @ARGV;
	}     # while options left on the command line
} else {
	
}

chdir "/isdc/pvphase/nrt/ops_1/idx/rev" or die "Can't chdir to /isdc/pvphase/nrt/ops_1/idx/rev";

#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 GNRL-AVRG-GRP-IDX.fits -> GNRL-AVRG-GRP-IDX_20050530175529.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 IBIS-VCTX-GRP-IDX.fits -> IBIS-VCTX-GRP-IDX_20050530085410.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 IREM-CHNK-GRP-IDX.fits -> IREM-CHNK-GRP-IDX_20050601082302.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-CTPR-GRP-IDX.fits -> ISGR-CTPR-GRP-IDX_20050601060018.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-CTXT-GRP-IDX.fits -> ISGR-CTXT-GRP-IDX_20050530174617.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-DEAD-CFG-IDX.fits -> ISGR-DEAD-CFG-IDX_20050530085343.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-LUT.-GRP-IDX.fits -> ISGR-LUT.-GRP-IDX_20050530085410.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-NOIS-CPR-IDX.fits -> ISGR-NOIS-CPR-IDX_20050601080531.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-NOIS-CRW-IDX.fits -> ISGR-NOIS-CRW-IDX_20050601080535.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 ISGR-SWIT-STA-IDX.fits -> ISGR-SWIT-STA-IDX_20050601080533.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 JMX1-GAIN-CAL-IDX.fits -> JMX1-GAIN-CAL-IDX_20050601080859.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 JMX2-GAIN-CAL-IDX.fits -> JMX2-GAIN-CAL-IDX_20050530085344.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 PICS-CTXT-GRP-IDX.fits -> PICS-CTXT-GRP-IDX_20050530085410.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 PICS-FALT-STA-IDX.fits -> PICS-FALT-STA-IDX_20050530175525.fits
#lrwxrwxrwx    1 ops_nrt  isdc_ops       37 Jun  1 08:31 PICS-HEPI-GRP-IDX.fits -> PICS-HEPI-GRP-IDX_20050530085410.fits

foreach my $idxlink ( glob ( "*-IDX.fits" ) ) {
	#	using .f as opposed to just . because of ISGR-LUT.-GRP-IDX.fits
	($root,$path,$ext) = File::Basename::fileparse($idxlink,'\.f.*');
	print "Cleaning up $idxlink\n";
	
	@idxreals = sort glob ( "${root}_*fits" );
	for ( my $i=0; $i <= $#idxreals-$num2keep; $i++ ) {
		unlink "$idxreals[$i]" or print "unlink of $idxreals[$i] failed\n";
	}
}

chdir "/isdc/pvphase/nrt/ops_1/idx/scw" or die "Can't chdir to /isdc/pvphase/nrt/ops_1/idx/scw";
print "Cleaning up scw\n";
@idxreals = sort glob ( "GNRL-SCWG-GRP-IDX_*fits" );
for ( my $i=0; $i <= $#idxreals-$num2keep; $i++ ) {
	unlink "$idxreals[$i]" or print "unlink of $idxreals[$i] failed\n";
}

chdir "/isdc/pvphase/nrt/ops_1/idx/scw/raw" or die "Can't chdir to /isdc/pvphase/nrt/ops_1/idx/scw/raw";
print "Cleaning up scw raw\n";
@idxreals = sort glob ( "GNRL-SCWG-GRP-IDX_*fits" );
for ( my $i=0; $i <= $#idxreals-$num2keep; $i++ ) {
	unlink "$idxreals[$i]" or print "unlink of $idxreals[$i] failed\n";
}

chdir "/isdc/pvphase/nrt/ops_1/idx/scw/prp" or die "Can't chdir to /isdc/pvphase/nrt/ops_1/idx/scw/prp";
print "Cleaning up scw prp\n";
@idxreals = sort glob ( "GNRL-SCWG-GRP-IDX_*fits" );
for ( my $i=0; $i <= $#idxreals-$num2keep; $i++ ) {
	unlink "$idxreals[$i]" or print "unlink of $idxreals[$i] failed\n";
}

chdir "/isdc/pvphase/nrt/ops_1/idx/scw/osm" or die "Can't chdir to /isdc/pvphase/nrt/ops_1/idx/scw/osm";
print "Cleaning up scw osm\n";
@idxreals = sort glob ( "GNRL-SCWG-GRP-IDX_*fits" );
for ( my $i=0; $i <= $#idxreals-$num2keep; $i++ ) {
	unlink "$idxreals[$i]" or print "unlink of $idxreals[$i] failed\n";
}


exit;

