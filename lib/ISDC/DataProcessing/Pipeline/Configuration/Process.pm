#____________________________________________________________________ 
# File: Process.pm
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-03 13:41:05+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Pipeline::Configuration::Process;

use strict;
use warnings;

use Carp qw(croak);

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{name}) && exists($_[0]->{resource}))
	? shift : croak(__PACKAGE__." constructor needs a name and resource parameter.")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");
    
    return bless($self, $class);
}

sub name() { return shift->{name} }

sub resource() { return shift->{resource} }

1;

__END__
