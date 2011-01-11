#!perl

use strict;
use warnings;

use Test::More tests => 7;

use File::Path qw(mkpath rmtree);

my $rep_base_prod = "./t/ops_1";
my @test_revolutions = ( '0998', '0999', '1000', '1001', '1002' );

foreach my $dir (@test_revolutions) {
    mkpath("$rep_base_prod/scw/$dir");
    ok((-d "$rep_base_prod/scw/$dir"),"Test rev dir for $dir exists.");
}

# Get list of revs and find the latest:
opendir (DIR, "$rep_base_prod/scw") || die "$rep_base_prod: cannot read: $!\n";
my @revs = map { $_ } sort { $b <=> $a } grep ($_ ne "." && $_ ne ".." && (-d "$rep_base_prod/scw/$_"), readdir(DIR) );
ok($#revs == 4,"Found 5 revolutions.");
ok($revs[0] == 1002, "First entry is 1002");
closedir (DIR);

# Cleanup:
rmtree($rep_base_prod);
