#____________________________________________________________________ 
# File: ADP.pm
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-06 12:00:15+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------
package ISDC::DataProcessing::Datasets::ADP;

use strict;
use warnings;

use base qw(Exporter);

use vars qw( $VERSION @EXPORT_OK %EXPORT_TAGS );

$VERSION='0.01';

%EXPORT_TAGS = ( 'all' => [ qw(
ahf
arc
asf
iop
ocs
olf
opp
orb
pad
paf
pod
rev
thf
tsf
) ] );

@EXPORT_OK=( @{ $EXPORT_TAGS{'all'} } );

1;

__END__
