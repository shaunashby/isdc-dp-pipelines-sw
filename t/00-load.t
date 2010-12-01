#!perl -T

use Test::More tests => 2;

BEGIN {
	use_ok( 'ISDC::DataProcessing::API' );
	use_ok( 'ISDC::DataProcessing::Pipeline' );
}

diag( "Testing ISDC::DataProcessing::API $ISDC::DataProcessing::API::VERSION, Perl $], $^X" );
