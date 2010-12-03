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

use overload q{""} => \&to_string;

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{name}) && exists($_[0]->{resource}) && exists($_[0]->{host}))
	? shift : croak(__PACKAGE__." constructor needs name, host and resource parameters.")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");
    
    return bless($self, $class);
}

sub name() { return shift->{name} }

sub host() { return shift->{host} }

sub resource() { return shift->{resource} }

sub to_string() {
    my $self = shift;
    return sprintf("%-10s %-10s\n",$self->{name},$self->{host});
}

1;

__END__
