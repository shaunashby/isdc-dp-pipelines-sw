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

our $VERSION = '0.3';

use overload q{""} => \&to_string;

sub new() {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = (@_ == 1) ? (ref($_[0]) eq 'HASH') ?
	(exists($_[0]->{name}) && exists($_[0]->{config}))
	? shift : croak(__PACKAGE__." constructor needs a name and config parameter.")
        : croak("Argument to constructor must be hash reference.")
        : croak("Constructor takes a hashref as first argument.");
    
    $self->{configuration} = ISDC::DataProcessing::Pipeline::Configuration->new( { file => $self->{config} } );
    
    return bless($self, $class);
}

sub name { return shift->{name} }

sub configuration() {
    my $self = shift;
    return $self->{configuration}->getPipeline($self->{name});
}

sub to_string() {
    my $self = shift;
    my $string = "";
    
    map {
	$string .= sprintf("%-10s %-10s %-10s\n",$_->name,$self->name,$_->host);
    } @{ $self->configuration->processes };

    return $string;
}

sub to_ldif() {
    my $self = shift;
    my $string = "";
    my $ldapbasedn = $self->{configuration}->ldapbasedn;
    
    $string .= sprintf("dn: pipelineName=%s,ou=Pipelines,ou=Services,%s\n",$self->name,$ldapbasedn);
    $string .= "objectClass: integralPipeline\n";
    $string .= "objectClass: top\n";
    $string .= "objectClass: pipelineProcessList\n";
    $string .= sprintf("pipelineClass: %s\n",$self->configuration->class);
    $string .= sprintf("pipelineName: %s\n",$self->name);
    map {
	$string .= sprintf("pipelineMemberProcess: processName=%s,ou=Processes,ou=Services,%s\n",$_->name,$ldapbasedn);
    } @{ $self->configuration->processes };
    $string .= "\n";

    map {
	$string .= sprintf("dn: processName=%s,ou=Processes,ou=Services,%s\n",$_->name,$ldapbasedn);
	$string .= "objectClass: top\n";
	$string .= "objectClass: pipelineProcess\n";
	$string .= sprintf("processName: %s\n",$_->name);
	$string .= sprintf("processHost: %s\n",$_->host);
	$string .= sprintf("processActive: %s\n",$_->active);
	$string .= sprintf("processResource: %s\n",$_->resource);
	$string .= "\n";
    } @{ $self->configuration->processes };
    
    return $string;
}

1;

__END__
