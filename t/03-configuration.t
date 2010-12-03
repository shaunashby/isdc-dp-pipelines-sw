#!perl -T

use Test::More tests => 7;

use ISDC::DataProcessing::Pipeline::Configuration;

my $config = ISDC::DataProcessing::Pipeline::Configuration->new({ file => "t/pipelines-test-config.yml" });
cmp_ok(ref($config),'eq','ISDC::DataProcessing::Pipeline::Configuration',"ISDC::DataProcessing::Pipeline::Configuration");

can_ok($config,'pipelines');
can_ok($config,'getPipeline');

my $pipeline = $config->getPipeline("adp");
can_ok($pipeline,'name');
cmp_ok(ref($pipeline),'eq',"ISDC::DataProcessing::Pipeline::Configuration::Pipeline","Configuration object has correct type.");

my $processlist = $pipeline->processes;
cmp_ok(ref($processlist->[0]),'eq',"ISDC::DataProcessing::Pipeline::Configuration::Process","Process object has correct type.");

can_ok($processlist->[0],'host');
