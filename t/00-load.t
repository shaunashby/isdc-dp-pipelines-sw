#!perl -T

use Test::More tests => 4;

BEGIN {
	use_ok( 'ISDC::DataProcessing::API' );
	use_ok( 'ISDC::DataProcessing::Pipeline' );
	use_ok( 'ISDC::DataProcessing::Pipeline::Configuration' );
	use_ok( 'ISDC::DataProcessing::Pipeline::Configuration::Stage' );
}

diag( "Testing ISDC::DataProcessing::API $ISDC::DataProcessing::API::VERSION, Perl $], $^X" );
