#!perl

use strict;
use warnings;

use Test::More tests => 49;

my $path = './scripts/legacy';
opendir (DIR, $path) || die "$path: cannot read: $!\n";
my @scripts = map { "$path/$_" } grep ($_ ne "." && $_ ne ".." && $_ !~ /\.sh$/ && $_ !~ /.*check_remote_rttm/, readdir(DIR) );
closedir (DIR);

my $output;

foreach my $script (@scripts) {
    eval {
	$output=`$^X -Iblib/lib/legacy -c $script 2>&1`;
	chomp($output);
    };
    
    is($output,"$script syntax OK","$script compiled correctly.");
}
