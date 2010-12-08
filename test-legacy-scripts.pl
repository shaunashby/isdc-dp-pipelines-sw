#!perl
#____________________________________________________________________ 
# File: test-legacy-scripts.pl
#____________________________________________________________________ 
#  
# Author: Shaun ASHBY <Shaun.Ashby@gmail.com>
# Update: 2010-12-08 16:11:17+0100
# Revision: $Id$ 
#
# Copyright: 2010 (C) Shaun ASHBY
#
#--------------------------------------------------------------------

use strict;
use warnings;

my $path = 'scripts/legacy';
opendir (DIR, $path) || die "$path: cannot read: $!\n";
my @scripts = map { "$path/$_" } grep ($_ ne "." && $_ ne ".." && $_ !~ /\.sh$/, readdir(DIR) );   
closedir (DIR);

foreach my $script (@scripts) {
    my $output=`$^X -Iblib/lib/legacy -c $script`;
    chomp($output);
}
