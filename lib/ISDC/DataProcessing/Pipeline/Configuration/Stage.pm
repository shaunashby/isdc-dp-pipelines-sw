#____________________________________________________________________ 
# File: Stage.pm
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-02 12:41:36+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Pipeline::Configuration::Stage;

use strict;
use warnings;

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    return bless({}, $class);
}

sub title() { return shift->{title} }

sub description() { return shift->{description} }

1;

__END__
