#____________________________________________________________________ 
# File: Pipeline.pm
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-11-29 13:31:07+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Pipeline;

use strict;
use warnings;

use Carp qw(croak);

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    return bless({}, $class);
}

1;

__END__
