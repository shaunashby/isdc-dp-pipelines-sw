#!perl -T

use Test::More tests => 24;

use lib 'blib/lib/legacy';

BEGIN {
    use_ok("Archiving");
    use_ok("CRVLIB");
    use_ok("CleanLIB");
    use_ok("CorLIB");
    use_ok("Datasets");
    use_ok("IBISLIB");
    use_ok("ISDCLIB");
    use_ok("ISDCPipeline");
    use_ok("JMXLIB");
    use_ok("OMCLIB");
    use_ok("OPUSLIB");
    use_ok("QLALIB");
    use_ok("QLAMOS");
    use_ok("RevIBIS");
    use_ok("RevIREM");
    use_ok("RevJMX");
    use_ok("RevOMC");
    use_ok("RevSPI");
    use_ok("SATools");
    use_ok("SPILIB");
    use_ok("SSALIB");
    use_ok("TimeLIB");
    use_ok("UnixLIB");
    use_ok("ISDC::Level");
}

diag( "Testing ISDC legacy pipeline modules, Perl $], $^X" );
