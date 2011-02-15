#!perl -T

use Test::More tests => 14;

use ISDC::DataProcessing::Pipeline::Configuration;

my $config = ISDC::DataProcessing::Pipeline::Configuration->new({ file => "t/pipelines-test-config.yml" });
cmp_ok(ref($config),'eq','ISDC::DataProcessing::Pipeline::Configuration',"ISDC::DataProcessing::Pipeline::Configuration");

can_ok($config,'pipelines');
can_ok($config,'getPipeline');

can_ok($config,'ldapbasedn');
cmp_ok($config->ldapbasedn,'eq','dc=integral,dc=ops',"LDAP bas DN is correctly returned.");

my $pipeline = $config->getPipeline("adp");

can_ok($pipeline,'name');
can_ok($pipeline,'class');

cmp_ok(ref($pipeline),'eq',"ISDC::DataProcessing::Pipeline::Configuration::Pipeline","Configuration object has correct type.");
cmp_ok($pipeline->class,'eq','ADP',"Configuration object correctly returns pipeline class.");

my $processlist = $pipeline->processes;
cmp_ok(ref($processlist->[0]),'eq',"ISDC::DataProcessing::Pipeline::Configuration::Process","Process object has correct type.");

can_ok($processlist->[0],'name');
can_ok($processlist->[0],'host');
can_ok($processlist->[0],'resource');
can_ok($processlist->[0],'active');
