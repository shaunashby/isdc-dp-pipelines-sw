
package ISDC::Time;

our $AUTOLOAD;
$ISDC::Time::VERSION = "1.0";

use strict;
use Carp;

=head1 NAME

I<ISDC/Time.pm> - 

=head1 SYNOPSIS

use I<ISDC::Time>;

=head1 DESCRIPTION

=cut


$| = 1;

my %fields = (
#	name        => 'test',
#	period      => 6.28,
#	file        => undef,
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

1;
