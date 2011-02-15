#____________________________________________________________________ 
# File: Pipeline.pm<2>
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-03 13:26:25+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Pipeline::Configuration::Pipeline;

use strict;
use warnings;

use Carp qw(croak);

use ISDC::DataProcessing::Pipeline::Configuration::Process;

our $VERSION = '0.01';

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{name}) && 
	 exists($_[0]->{processes}))
	? shift : croak(__PACKAGE__." constructor needs a name and processes parameter")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");

    my $processes = [];
    for my $process (@{ $self->{processes} }) {
	push(@$processes,ISDC::DataProcessing::Pipeline::Configuration::Process->new($process));
    }
    $self->{processes} = $processes;
    
    return bless($self, $class);
}

sub name() { return shift->{name} }

sub processes() { return shift->{processes} || [] }

sub class() { return shift->{class} || undef }

1;

__END__
