#!perl -T

use Test::More tests => 1;

use ISDC::DataProcessing::Pipeline::Configuration;

my $config = ISDC::DataProcessing::Pipeline::Configuration->new({ file => "t/pipelines-test-config.yml" });
cmp_ok(ref($config),'eq','ISDC::DataProcessing::Pipeline::Configuration',"ISDC::DataProcessing::Pipeline::Configuration");
