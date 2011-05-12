=head1 NAME

ISDC::DataProcessing::API - Entrypoint module to ISDC data processing pipeline software

=head1 VERSION

Version 0.01

=cut

package ISDC::DataProcessing::API;

use warnings;
use strict;

use base qw(Exporter);

use vars qw( $VERSION @EXPORT_OK %EXPORT_TAGS );

%EXPORT_TAGS = ( 'all' => [ qw(config pipeline process) ] );

@EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

$VERSION = '0.3.3';

# Context exported as global variable using :context tag:
%EXPORT_TAGS = (
    'config'   => [ qw( CONFIG1  ) ],
    'pipeline' => [ qw( PLA1 ) ],
    'process'  => [ qw( PRA1 ) ]
    );

@EXPORT_OK = ( @{ $EXPORT_TAGS{'config'} } );

# Configuration constants:
use constant CONFIG1 => 'Value1';

# Pipeline constants:
use constant PLA1 => 'Value2';

# Process constants:
use constant PRA1 => 'Value3';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use ISDC::DataProcessing::API;

    my $foo = ISDC::DataProcessing::API->new();
    ...

=head1 AUTHOR

Shaun ASHBY, C<< <Shaun.Ashby at unige.ch> >>

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ISDC::DataProcessing::API


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=isdc-dp-pipelines-sw>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/isdc-dp-pipelines-sw>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/isdc-dp-pipelines-sw>

=item * Search CPAN

L<http://search.cpan.org/dist/isdc-dp-pipelines-sw/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2010 Shaun ASHBY, all rights reserved.

This program is released under the following license: GPL+/Artistic


=cut

1; # End of ISDC::DataProcessing::API
