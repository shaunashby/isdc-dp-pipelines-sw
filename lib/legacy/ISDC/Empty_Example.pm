
package ISDC::PACKAGE_NAME;

use strict;
use warnings;

use Carp;

our $AUTOLOAD;
$ISDC::PACKAGE_NAME::VERSION = "1.0";

=head1 NAME

I<ISDC/PACKAGE_NAME.pm>

=head1 SYNOPSIS

use I<ISDC::PACKAGE_NAME>;

=head1 USAGE

=head1 METHODS

=over

=cut


$| = 1;

#	fields that are made accessible for getting and setting 
#	without writing the individual get and set functions.
my %fields = (
#       name        => 'test',
#       period      => 6.28,
#       file        => undef,
#		instrument => undef,
#		current    => undef
);

sub AUTOLOAD {  #       from http://perldoc.perl.org/perltoot.html
	my $self = shift;
	my $type = ref($self)
		or croak "$self is not an object";
	my $name = $AUTOLOAD;
	$name =~ s/.*://;   # strip fully-qualified portion
	unless (exists $self->{_permitted}->{$name} ) {
		croak "Can't access `$name' field in class $type";
	}
	if (@_) { $self->{$name} = shift; }
	return $self->{$name};
}

sub DESTROY { }

sub new {
	my $class = shift;
	my $self  = {
		_permitted => \%fields,
		%fields,
	};
	bless ($self, $class);

	return $self;
}

=item B<METHOD_NAME>

returns ...

=cut

