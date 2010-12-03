#!perl -T

use Test::More tests => 2;

use ISDC::DataProcessing::Pipeline;

my $pipeline = ISDC::DataProcessing::Pipeline->new({ name => "adp", config => 't/pipelines-test-config.yml' });
cmp_ok(ref($pipeline),'eq','ISDC::DataProcessing::Pipeline',"ISDC::DataProcessing::Pipeline");

my $config = $pipeline->configuration;

cmp_ok(ref($config),'eq','ISDC::DataProcessing::Pipeline::Configuration::Pipeline',"ISDC::DataProcessing::Pipeline::Configuration::Pipeline");
