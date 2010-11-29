use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'ISDC::DataProcessing::API',
    AUTHOR              => 'Shaun ASHBY <Shaun.Ashby@unige.ch>',
    VERSION_FROM        => 'lib/ISDC/DataProcessing/API.pm',
    ABSTRACT_FROM       => 'lib/ISDC/DataProcessing/API.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'GPL+/Artistic')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'isdc-dp-pipelines-sw-*' },
);
