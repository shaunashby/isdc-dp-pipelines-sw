#!/opt/local/bin/perl
#____________________________________________________________________ 
# File: test.pl
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-03 11:24:26+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
use warnings;
use strict;

use ISDC::DataProcessing::Pipeline;

my $pipeline = ISDC::DataProcessing::Pipeline->new({ name => "nrtrev", config => "t/pipelines-test-config.yml" });

print "$pipeline\n";
