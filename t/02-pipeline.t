#!perl -T

use Test::More tests => 1;

use ISDC::DataProcessing::Pipeline;

my $pipeline = ISDC::DataProcessing::Pipeline->new({ name => "test" });

cmp_ok(ref($pipeline),'eq','ISDC::DataProcessing::Pipeline',"ISDC::DataProcessing::Pipeline");
