#!perl -T

use Test::More tests => 1;

use ISDC::DataProcessing::API qw(:config);

cmp_ok(CONFIG1,'eq','Value1',"API symbol import working.");
