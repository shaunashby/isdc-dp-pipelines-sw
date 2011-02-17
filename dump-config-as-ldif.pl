#!perl
#____________________________________________________________________ 
# File: dump-config-as-ldif.pl
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2011-02-15 15:03:32+0100
# Revision: $Id$ 
#
# Copyright: 2011 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
use warnings;
use strict;

use ISDC::DataProcessing::Pipeline;

for my $pipeline (qw( nrtinput nrtscw nrtrev nrtqla adp consinput consscw consrev ) ) {
    my $obj = ISDC::DataProcessing::Pipeline->new({ name => "$pipeline", config => "t/pipelines-test-config.yml" });
    print $obj->to_ldif(),"\n";    
}


