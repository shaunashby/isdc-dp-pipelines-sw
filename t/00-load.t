#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'ISDC::DataProcessing::API' );
}

diag( "Testing ISDC::DataProcessing::API $ISDC::DataProcessing::API::VERSION, Perl $], $^X" );
