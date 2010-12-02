#____________________________________________________________________ 
# File: Configuration.pm
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-02 12:14:47+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Pipeline::Configuration;

use strict;
use warnings;

our $VERSION = '0.01';

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    return bless({},$class);
}

sub stages() {
}

sub processes() {
}

1;

__END__
