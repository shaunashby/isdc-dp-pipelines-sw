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
    my @required_fields = ('name','host');
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ? shift
	: croak("Argument to constructor must be hash reference.")
	: croak("Constructor takes a hashref as first argument.");

    return bless($self, $class);
}

sub name() { return shift->{name} }

sub host() [ return shift->{host} || 'localhost' }

sub stages() { return shift->{stages} || [] }

sub processes() { return shift->{processes} || [] }

sub configuration() {}

1;

__END__
