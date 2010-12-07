package ISDC::Level;

use strict;
use warnings;

use Carp;

our $AUTOLOAD;
$ISDC::Level::VERSION = "1.0";

=head1 NAME

I<ISDC/Level.pm>

=head1 SYNOPSIS

use I<ISDC::Level>;

=head1 USAGE

use strict;
use ISDC::Level;

print "$ISDC::Level::VERSION\n";

my $l = new ISDC::Level('isgri','COR');
print "Instrument: ",$l->instrument,"\n";
print "Previous: ",$l->previous,"\n";
print "Current: ",$l->current,"\n";
print "Next: ",$l->next,"\n\n";

$l = new ISDC::Level('isgri','DEAD');
print "Instrument: ",$l->instrument,"\n";
print "Previous: ",$l->previous,"\n";
print "Current: ",$l->current,"\n";
print "Next: ",$l->next,"\n\n";

$l = new ISDC::Level('isgri','CLEAN');
print "Instrument: ",$l->instrument,"\n";
print "Previous: ",$l->previous,"\n";
print "Current: ",$l->current,"\n";
print "Next: ",$l->next,"\n\n";

=head1 METHODS

=over

=cut


$| = 1;

my %levels = (
	"isgri" => [ qw/COR GTI DEAD BIN_I BKG_I CAT_I IMA IMA2 BIN_S CAT_S SPE LCR COMP CLEAN/ ],
	"jemx"  => [ qw/COR GTI DEAD CAT_I BKG BIN_I IMA SPE LCR BIN_S BIN_T IMA2/ ],
	"omc"   => [ qw/COR GTI IMA IMA2/ ],
);

my %fields = (
		instrument => undef,
		current    => undef
);

sub AUTOLOAD {
	my $self = shift;
	my $type = ref($self)
		or croak "$self is not an object";
	my $name = $AUTOLOAD;
	$name =~ s/.*://;
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
		instrument => shift,
		current      => shift
	};
	bless ($self, $class);

	$self->{instrument} =~ tr/A-Z/a-z/;
	die "instrument :$self->{instrument}: doesn't exist\n" unless ( exists $levels{$self->{instrument}} );

	$self->{lref} = [];
	push(@{ $self->{lref} },@{$levels{$self->{instrument}}});
	
	die "current :$self->{current}: doesn't exist\n" unless ( $self->_index_of_ >= 0 );

	return $self;
}

=item B<current>

returns the given level

=cut

#sub current    { return shift->{current}; }

=item B<instrument>

returns the given instrument

=cut

#sub instrument { return shift->{instrument}; }

sub next {
	my $self = shift;
	my $index = $self->_index_of_();
	return ( $index < $#{$self->{lref}} ) ? $self->{lref}->[$index+1] : $self->{lref}->[$index];
}

sub previous {
	my $self = shift;
	my $index = $self->_index_of_();
	return ( $index > 0 ) ? $self->{lref}->[$index-1] : $self->{lref}->[$index];
}

sub _index_of_ {
	my $self = shift;
	for ( my $i = 0; $i < $#{$self->{lref}}; $i++ ) {
		return $i if ( $self->{lref}->[$i] eq $self->{current} );
	}
	return -1;	#	no match found
}

1;
