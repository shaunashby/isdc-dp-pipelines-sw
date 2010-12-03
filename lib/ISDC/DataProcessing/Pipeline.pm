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

use ISDC::DataProcessing::Pipeline::Configuration;

our $VERSION = '0.01';

use overload q{""} => \&to_string;

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{name}) && exists($_[0]->{config}))
	? shift : croak(__PACKAGE__." constructor needs a name and config parameter.")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");
    
    $self->{configuration} = ISDC::DataProcessing::Pipeline::Configuration->new(
	{ file => $self->{config} }
	)->getPipeline($self->{name});
    
    return bless($self, $class);
}

sub configuration() {
    my $self = shift;
    return $self->{configuration};
}

sub to_string() {
    my $self = shift;
    my $string = "";
    map {
	$string.="$_";
    } @{ $self->{configuration}->processes };
    return $string;
}

1;

__END__
