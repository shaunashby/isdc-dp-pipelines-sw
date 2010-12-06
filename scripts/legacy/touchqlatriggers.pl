#!perl -w
   
use strict;

my @revs;
for ( my $i=0; $i<=$#ARGV; $i++ ) {
	#if     last one      or    next one is not ".."
	if ( ( $i == $#ARGV ) || ( $ARGV[$i+1] !~ /\.\./ ) ) {
		push @revs, $ARGV[$i];
	} else {
		push @revs, ( $ARGV[$i] .. $ARGV[$i+2] );
		$i+=2;
	}
}

foreach ( @revs ) {
	my $rev = sprintf ( "%04d", $_ );
#	foreach my $scw ( `ls -1d "$ENV{REP_BASE_PROD}/scw/$rev/0*0.*" | awk -F/ '{print \$NF}' | awk -F. '{print \$1}'` ) {
	foreach my $scw ( glob "$ENV{REP_BASE_PROD}/scw/$rev/0*0.*" ) {
		$scw =~ s/^.+(\d{12}).+$/$1/;
		print "$scw\n";
		`touch $scw.trigger`;
	}
}


=head1 NAME

I<touchqlatriggers.pl> - misc tool to touch qla triggers for points

=head1 SYNOPSIS

I<touchqlatriggers.pl>

=head1 DESCRIPTION

=item

=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level
Architectural Design Document.

=head1 AUTHORS

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

