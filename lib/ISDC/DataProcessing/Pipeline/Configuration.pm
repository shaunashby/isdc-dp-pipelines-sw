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

use Carp qw(croak);

use YAML::Syck qw(Load);
use Path::Class::File;

use ISDC::DataProcessing::Pipeline::Configuration::Pipeline;

our $VERSION = '0.3.2';

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{file})) ? shift : croak(__PACKAGE__." constructor needs a file parameter")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");

    my $config = Path::Class::File->new( $self->{file} )->slurp;
    my $fileconfig = Load($config);

    croak(sprintf("No pipelines defined in the configuration file %s",$self->{file})) unless
	(exists($fileconfig->{pipelines}) && ref($fileconfig->{pipelines}) eq 'HASH');

    $self->{configuration} = { pipelines => {} };
    $self->{ldapbasedn} = $fileconfig->{ldapbasedn} || 'dc=local';
    
    foreach my $pipeline (keys %{ $fileconfig->{pipelines} }) {
	$self->{configuration}->{pipelines}->{$pipeline} = 
	     ISDC::DataProcessing::Pipeline::Configuration::Pipeline->new(
		 {
		     name => $pipeline,
		     %{ $fileconfig->{pipelines}->{$pipeline} },
		 }
	     );
    }

    return bless($self,$class);
}

sub stages() {}

sub pipelines() { return shift->{configuration}->{pipelines} }

sub getPipeline() {
    my ($self, $name) = @_;
    croak("Pipeline $name: configuration not found/unknown pipeline.") unless
	(exists($self->{configuration}->{pipelines}->{$name}));
    return $self->{configuration}->{pipelines}->{$name};
}

sub ldapbasedn() { return shift->{ldapbasedn} };

1;

__END__
