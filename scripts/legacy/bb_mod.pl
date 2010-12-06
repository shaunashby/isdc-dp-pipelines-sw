#! /bin/sh
eval '  exec perl -x $0 ${1+"$@"} '
#! perl -w

=head1 NAME

I<bb_mod.pl> - used to update OSF statii

=head1 SYNOPSIS

I<bb_mod.pl>

=head1 DESCRIPTION

Utility for modifying OSFs outside of OPUS OMG.

=cut

use strict;
use lib "$ENV{ISDC_OPUS}/pipeline_lib/";	#  do I really need this since the libs are in the same place?
use ISDCPipeline;
use OPUSLIB;
use UnixLIB;
use ISDCLIB;
use File::Basename;

my ( $pipeline, $revno, $current_status, $new_status, $askdcf, $match, $type, $newcom );
my ( $osf, @osfs );
my ( $hextime, $osfstatus, $dataset, $thistype, $dcfnum, $command );
my $retval;
my $delete = 0;
my $curcom;
my $donotconfirm = 0;
my $rootcommand;
my $return = 0;
my @result;  
my $agelimit = 0;  #  Need numeric

###########################################################################
#  Check the parameter given:
###########################################################################

foreach ( sort @ARGV ) {
	if (/--h/) {
		print "USAGE:  bb_mod.pl --path=  [--command=] [--new_status=] [--delete] [--type=] [--revno=] [--dataset=] "
			."[--current_status=] [--match=] [--age_limit=]\n\nNote that OPUS_WORK must be set.  "
			."Specify a given new status (including all columns) or --delete to delete the matching OSFs.  "
			."OSFs are selected based on the requested type, revolution number (revno), current status, and/or "
			."any string to match within the OSF dataset name.  You will be given a list of OSFs to confirm the modification.\n";
		exit 0;
	}
	elsif ( /--p\w*\=(.*)$/ ) {
		$pipeline = $1;
		print "$prefix1 Path:  $pipeline\n";
	}
	elsif ( /--t\w*\=(.*)$/ ) {
		$type = $1;
		print "$prefix1 Type:  $type\n";
	}
	elsif ( /--cu\w*\=(.*)$/ ) {
		$current_status = $1;
		print "$prefix1 Current status:  $current_status\n";
		$current_status = "^${current_status}\$";
		if ( $current_status =~ /p/ ) {
			print "WARNING!\nWARNING! -----------------  Be aware that BBUpdate will skip all osf's matching 'p'!\nWARNING!\n";
		}
	}
	elsif ( /--n\w*\=(.*)$/ ) {
		$new_status = $1;
		print "$prefix1 New status:  $new_status\n";
	}
	elsif ( /--m\w*\=(.*)$/ ) {
		$match = $1;
		print "$prefix1 Match:  $match\n";
	}
	elsif ( /--r\w*\=(.*)$/ ) {
		#  Since params sorted, we know match would have been set first.
		$revno = $1;
		$match = "" unless ($match);  # avoids annoying warnings
		print "$prefix1 Revno:  $revno\n";
		#  It would be a bit risky, but match and revno could be used
		#   at the same time.
		$match = "^${revno}.*${match}" unless ($pipeline =~ /nrtqla|consssa/);
		$match = "^\\w{5}${revno}.*${match}" if ($pipeline =~ /nrtqla|consssa/);
	}
	elsif ( /--da\w*=(.*)$/ ) {
		$dataset = $1;
		print "$prefix1 Dataset:  $dataset\n";
	}
	elsif ( /--co\w*=(.*)$/ ) {
		$newcom = $1;
		print "$prefix1 Command:  $newcom\n";
	}
	elsif ( /--dcf=(.*)$/ ) {
		$askdcf = $1;
		print "$prefix1 DCFnum:  $askdcf\n";
	}
	elsif ( /--age\w*=(.*)$/ ) {
		$agelimit = $1;
		print "$prefix1 AgeLimit:  $agelimit\n";
	}
	elsif ( /--delete/ ) {
		$delete++;
	}
	elsif ( /--donotconfirm/ ) {
		$donotconfirm++;
	}
	else {
		die "$prefix1 ERROR:  don't recognize parameter $_";
	}
} # end foreach ARGV

die "$prefix1 ERROR:  you must give at least a path!" unless ( $pipeline );
die "$prefix1 ERROR:  you must give either a new status, newcom, or specify --delete!" unless ( ( $new_status ) || ( $delete ) || ( $newcom ) );
die "$prefix1 ERROR:  can't find $ENV{OPUS_WORK}/${pipeline}/obs" unless ( -d "$ENV{OPUS_WORK}/${pipeline}/obs" );
die "$prefix1 ERROR:  newcom must be either suspend or resume" if ( ( $newcom ) && ( $newcom !~ /^susp|resu/ ) );

$type   = "" unless ( defined ( $type ) );		#	050831 - Jake - to avoid the "Use of uninitialized variable ..." message
$match  = "" unless ( defined ( $match ) );		#	050831 - Jake - to avoid the "Use of uninitialized variable ..." message
$askdcf = "" unless ( defined ( $askdcf ) );		#	050831 - Jake - to avoid the "Use of uninitialized variable ..." message
$newcom = "" unless ( defined ( $newcom ) );		#	050831 - Jake - to avoid the "Use of uninitialized variable ..." message

###########################################################################
# Get lists of things to modify
###########################################################################

if ( ( $dataset ) && ! ( $delete ) && ( $new_status ) ) {
	print "$prefix1 Updating only $dataset to new status $new_status\n";
	print "$prefix1 (NOTE:  you can run 'osf_update -p $pipeline -f $dataset -s $new_status' yourself, you know.)\n";
	@osfs = ( "$dataset" );
}
elsif ( ( $dataset ) && ( $delete ) ) {
	print "$prefix1 Deleting only $dataset.\n";
	print "$prefix1 (NOTE:  you can run 'osf_delete -p $pipeline -f $dataset' yourself, you know.)\n";
	@osfs = ("$dataset");
}
elsif ( ( $dataset ) && ( $newcom )) {
	print "$prefix1 ${newcom}'ing dataset $dataset\n";
	@osfs = ("$dataset");
}


if ( $newcom ) {
	#  BBUpdate doesn't do this.  Get list and then do it here.
	$return++;
}

@osfs = &ISDCPipeline::BBUpdate (
	"path"      => "$pipeline",
	"type"      => "$type",
	"match"     => "$match",
	"dcf"       => "$askdcf",
	"fullstat"  => "$new_status",
	"matchstat" => "$current_status",
	"delete"    => $delete,
	"return"    => $return,
	"agelimit"  => "$agelimit",
	"donotconfirm" => "$donotconfirm",
	);

unless ( $newcom ) {
	print "No new command given so quitting.\n";
	exit 0;
}

print "$prefix1 Found the following OSFs matching your input:\n".join("\n",@osfs)."\n";
unless ( $donotconfirm ) {
	print "$prefix1 Are you sure you want to $newcom these ".($#osfs + 1)." OSF(s)?  Type 'yes':  ";
	my $reply = <STDIN>;
	chomp $reply;
	if ($reply !~ /^yes$/) {
		print "$prefix1 You didn't type 'yes';  quitting.\n";
		exit 0;
	}
}


foreach $osf ( @osfs ) {
	
	($osf) = `$myls $ENV{OPUS_WORK}/${pipeline}/obs/*${osf}* 2> /dev/null`;
	die "$prefix1 ERROR:  cannot find OSF $osf" unless ($osf);
	chomp $osf;
	$osf = File::Basename::basename($osf);
	my $newosf = $osf;
	$curcom = $osf;
	$curcom =~ s/^.*(\w{4})$/$1/;
	
	$newcom =~ s/^(\w{4}).*/$1/;
	
	if ( $newcom =~ /^susp/ ) {
		if ($curcom !~ /____/) {
			print "$prefix1 WARNING:  OSF $osf is currently $curcom;  not suspending\n";
			next;
		}
		
		die "$prefix1 ERROR:  current status not ____ of OSF $osf" unless ($newosf =~ s/^(.*)____$/${1}susp/);
		
	} # end if ($newcom =~ /^susp/)
	
	#  Check above ensures that command was either susp or resu, so now resu:
	else {
		
		if ( $curcom !~ /susp/ ) {
			print "$prefix1 WARNING:  OSF $osf is currently $curcom;  not resuming\n";
			next;
		}
		
		$newosf =~ s/^(.*)susp$/${1}____/;
	} # end else (in other words, if we want to resume it
	
	$command = "$mymv $ENV{OPUS_WORK}/${pipeline}/obs/$osf $ENV{OPUS_WORK}/${pipeline}/obs/$newosf";
	
	#  osf_* don't ever print errors, but the mv might:
	print "$prefix1 Running \'$command\'\n";
	@result = `$command`;
	if ( $? ) {
		print "$prefix1 ERROR:  status $? from command '$command':\n@result\n$prefix1 quitting.\n";
		die "$prefix1 ERROR:  status $? from command '$command':\n@result\n$prefix1 quitting.";
	}
	
} # foreach osf

print "$prefix1 Done.\n";

exit;


=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

