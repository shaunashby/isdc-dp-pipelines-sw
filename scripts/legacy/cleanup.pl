#!/usr/bin/perl

use strict;
use warnings;
use Cwd;
use File::Basename;

use UnixLIB;
use ISDCLIB;
use CleanLIB;

#	cleanup.pl --DEBUG --do_not_confirm --path=consscw --level=raw --dataset=002400090010
#	actually deletes the scw directory.  Should make like everything else and just use &MoveData(), No?
#
#	CleanRAW seems a bit over complicated at this point.
#

if  (($#ARGV < 2) && !defined($ENV{PATH_FILE_NAME})) {
    print "\nUSAGE: cleanup.pl --path=<path> --dataset=<dataset> --level=<level> [--log=<log>] [--bydate] [--match] [--DEBUG] [--FORCE]\n\n";
    print "\tType \'perldoc $0\' to see the full help.\n";
    exit 0;
}


##############################################################################
##
##                        MAIN
##
##############################################################################

my $hashline  = "#####################################################################################";
my $dashline  = "-------------------------------------------------------------------------------------";
my $equalline = "=====================================================================================";
my $grtrline  = ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";
my $halfdashline = "----------------------------------------------";

#	don't use dprint here cause there is only 1 at this point
print "$hashline\n";
print "-- Running $0 @ARGV\n";
print "$dashline\n";

my %pars = &CleanLIB::GetPars;

my @osfs;
my $answer;
my @remainders;
my $file;
my @indices;  
my $type;
my $root;
my $subdir;
my $index;
my ( $retval, @result );
my $temp;
my $oldout;

if ( !(defined($pars{log}))){
    $pars{log} = "$ENV{OPUS_HOME_DIR}/clean_$pars{dataset}_$pars{level}_$$.log" unless (defined($ENV{OSF_DATASET}));
}

if (!(defined($ENV{OSF_DATASET}))) {
    open(LOG,">$pars{log}") or die "$prefix1 ERROR 001:  cannot open log $pars{log}";
    $oldout = select(LOG);
}

&CleanLIB::SetDEBUG ( $pars{DEBUG} );

$pars{env} = &CleanLIB::SetEnvVariable ( %pars );

&CleanLIB::ValidateRequest ( %pars );

my %paths = &CleanLIB::BuildPathsHashTable ( %pars );


########################################################################
#
#	So now, we just loop over paths and OSFs, calling our functions
#
#	This cleans the OPUS stuff ( and RAW if just a single SCW )
#
&dprint ( "$equalline\n" );
foreach ( keys ( %paths ) ) {
    chomp ( $pars{onepath} = $_ );
    &dprint ( "$dashline\n" );
    &dprint ( "$prefix1\n$prefix1 Processing path $pars{onepath}\n$prefix1\n" );
    
    if ( @{ $paths{$pars{onepath}} } ) {
	&dprint ( "$prefix1 Datasets to StartCleanup for path $pars{onepath}:\n$prefix4".join("$prefix4",@{ $paths{$pars{onepath}} })."\n" );
	
	foreach ( @{ $paths{$pars{onepath}} } ) { 
	    chomp ( $pars{onedataset} = $_ );
	    &dprint ( "$halfdashline\n" ) if ( $pars{DEBUG} );
	    $pars{onedataset} =~ s/\s+//g;
	    unless ( $pars{onedataset} ) {
		&dprint ( "$prefix1 Dataset from path $pars{onepath} is NULL after chomping and removing whitespace.\n" );
		next;
	    }
	    &dprint ( "$prefix1\n$prefix1 Processing dataset $pars{onedataset}\n$prefix1\n" ) if ( $pars{DEBUG} );
	    &CleanLIB::StartCleanup ( %pars ) unless ( $pars{dry_run} || $pars{"dry-run"} || $pars{dryrun} );
	    delete $pars{onedataset};
	}
    } else {
	&dprint ( "$prefix1 No datasets found in path $pars{onepath}\n" );
	&dprint ( "$prefix1 \@{ \$paths{\$pars{onepath}} } is @{ $paths{$pars{onepath}} }\n" ) if ( $pars{DEBUG} );
    }
    &dprint ( "$prefix1\n" );
    delete $pars{onepath};
}

#########################################################################
&dprint ( "$equalline\n" );

exit if ( $pars{dry_run} || $pars{"dry-run"} || $pars{dryrun} );


#########################################################################
#
#       For cleanup of entire revolutions, check for anything left under
#         OPUS_WORK area.
#
#########################################################################
#
#	Clean all revolution related opus stuff.  This is effectively all the time since there are only 2 levels now.  (no more prp)
#
if  (  ($pars{level} =~ /raw|opus/) && ($pars{path}  =~ /revol_(aux|scw|ssa)/) ){
    print "$grtrline\n$grtrline\n";
    print "$prefix1 OPUS cleanup done for OSFs on blackboard;  looking for anything else under OPUS_WORK.\n";
    print "$grtrline\n$grtrline\n";
    
    #  If we're in Cons and cleaning revol_scw, we've got .done files to remove,
    #   and we don't need to ask about it.
    
    if ( ( $pars{env} =~ /cons/ ) && ( $pars{path} =~ /revol_scw/ ) ) {
	foreach ("pp","inp","rev","scwdp","arc","ingest","sa","sa_ingest","ssa","ssa_ingest","sma","sma_ingest") {		
	    my $file = "$ENV{OPUS_WORK}/consrev/input/$pars{dataset}_$_.done";
	    if ( -e "$file" ) {
		print "$prefix1 Removing $file\n";
		unlink "$file" or die "$prefix1 ERROR 002:  cannot unlink $file";
		die "$prefix1 ERROR 003:  did not unlink $file" if ( -e "$file" );
	    }
	}

	foreach ( "scwdp", "sa", "ssa", "sma" ) {
	    my $file = "$ENV{OPUS_WORK}/consrev/input/$pars{dataset}_$_.started";
	    if ( -e "$file" ) {
		print "$prefix1 Removing $file\n";
		unlink "$file" or die "$prefix1 ERROR 004:  cannot unlink $file";
		die "$prefix1 ERROR 005:  did not unlink $file" if ( -e "$file" );
	    }
	}
    }
    
    foreach my $onepath (keys(%paths)) {
	my $thisdir = "$ENV{OPUS_WORK}/$onepath";
	push @remainders, `$myls $thisdir/$pars{dataset}* $thisdir/input/$pars{dataset}* $thisdir/scratch/$pars{dataset}* 2> /dev/null`;
    }
    
    if (@remainders) {
	&dprint ( "$prefix1 Found remainder under $ENV{OPUS_WORK}:\n\t".join("\t",@remainders) );
	
	$answer = "y";
	unless ( $pars{"do_not_confirm"} ) {
	    print STDOUT "$prefix1 Do want these remainders deleted? [y]: ";
	    while(<STDIN>) { $answer = $_; chomp $answer; last; }
	}
	
	if ( ($answer) && ($answer !~ /y/) ) {
	    print STDOUT "$prefix1 Skipping remainder removal.\n";
	} else {
	    print STDOUT "$prefix1 Removing remainders.\n";
	    foreach $file (@remainders) { 
		chomp $file;
		next unless ($file);
		print "$prefix1 Removing $file\n";
		my $command = "$myrm  $file";
		print "$prefix1 Running \'$command\'\n";
		`$command`;
		print "$prefix1 WARNING:  Cannot \'$command\'\n" if ($?);
		print "$prefix1 WARNING:  $file does not exist anymore anyway.\n" unless ( -e $file );
		print "$prefix1 WARNING:  $file still exists though.\n" if ( -e $file );
	    }
	}
    }
}


#########################################################################
#  
#		Now, for raw cleanup, wipe out data and *then* clean indices.
#		Actually, it doesn't look like it cleans any indices here.
#
#########################################################################

if ( $pars{level} =~ /raw/ ) {
    if ( ( $pars{path}  =~ /revol_/ ) || ( $pars{path}  =~ /nrtqla/ ) ) {
	&CleanLIB::CleanData ( %pars );
    } else {
	foreach ( keys ( %paths ) ) {
	    chomp ( $pars{onepath} = $_ );
	    &dprint ( "$dashline\n" );
	    &dprint ( "$prefix1 Processing Path: $pars{onepath} and Datasets: \n$prefix4".join("$prefix4",@{$paths{$pars{onepath}}})."\n" );
	    
	    if ( @{ $paths{$pars{onepath}} } ) {
		&dprint ( "$prefix1 Datasets to CleanData for path $pars{onepath}:\n$prefix4".join("$prefix4",@{ $paths{$pars{onepath}} })."\n" );
		
		foreach ( @{ $paths{$pars{onepath}} } ) { 
		    chomp ( $pars{onedataset} = $_ );
		    &dprint ( "$halfdashline\n" ) if ( $pars{DEBUG} );
		    $pars{onedataset} =~ s/\s+//g;
		    unless ( $pars{onedataset} ) {
			&dprint ( "$prefix1 Dataset from path $pars{onepath} is NULL after chomping and removing whitespace.\n" );
			next;
		    }
		    &dprint ( "$prefix1\n$prefix1 Processing dataset $pars{path} $pars{onedataset} to level $pars{level}\n$prefix1\n" ) if ( $pars{DEBUG} );
		    
		    &CleanLIB::CleanData ( %pars );
		    delete $pars{onedataset};
		}
	    } else {
		&dprint ( "$prefix1 No datasets found in path $pars{onepath}\n" );		
	    }
	    delete $pars{onepath};
	}
    }
}

if ($pars{log}){  close(LOG); }

print STDOUT "$prefix1 Done.\n";

if ( ($pars{path} =~ /revol/) && ($pars{level} =~ /raw/) ) {
    print STDOUT "$prefix1 \n";
    print STDOUT "$prefix1 Now, if you are sure, you can remove the data under REP_BASE_PROD/cleanup.\n";
    print STDOUT "$prefix1 \n";
}

if ($pars{log}){  close(LOG); }

exit 0;

#############################################################################
##
##                        END OF MAIN
##
#############################################################################

__END__ 

=head1 NAME

cleanup.pl - Perl script for cleaning up OPUS data and ISDC repositories.

=head1 SYNOPSIS

cleanup.pl is run either through OPUS or from the command line.  It 
will determine what files to clean based on the command line parameters or
environment variables passed by OPUS.  From the command line only, large
scale repository cleaning, including raw data, can also be specified.  

=head1 USAGE

B<command line:>  

cleanup.pl --path=<path> --dataset=<dataset> --level=<level> [--bydate] [--match] [--log=] [--DEBUG] [--FORCE] [--do_not_confirm] [--h]

ex. 

cleanup.pl --path=revol_ssa --level=opus --dataset=01 --inst=picsit
(This will remove all the opus stuff for consssa osfs matching "ip_01")

cleanup.pl --path=revol_ssa --level=raw --dataset=00 --inst=isgri --DEBUG
(This will remove all the opus stuff for consssa osf's matching "ii_00"
 AND move obs_isgri/00*.000 to cleanup/obs_isgri/ )


where the options are as follows:

=over 5

=item B<path>  

The pipeline to be cleaned, i.e. "adp", "nrtinput", 
"consinput", "nrtscw", "consscw", "nrtrev", "consrev", "nrtqla", 
"consssa", "conssa", or "arcdd". (arcdd, nrtqla, conssa and consssa
have not effectively been tested.)

Four additional "paths" are allowed on the command line:  "revol_scw", 
"revol_aux", "revol_qla' and "revol_ssa".  These are to be used only in the case that an 
entire revolution worth of data is to be deleted.  


=item B<dataset>  

The name of the dataset to be cleaned, as it appears on 
the blackboard, e.g. science window ID, ADP file name ("." replaced by "_"),
observation group ID, revolution file mnemonic, etc.  

In the case of the repository cleanup pseudo-path options "revol_scw",
"revol_aux", "revol_ssa" and "revol_qla", the dataset should be a revolution number.  

In the case of cleaning by date, i.e. the --bydate flag is specified, then
the dataset should be the time before which all datasets should be cleaned.  
See below.


=item B<level>  

The level to which the data should be cleaned, i.e. all levels down
to B<and including> the level specified are deleted.  

- "opus" means only the OPUS workspace files are to be deleted.  This is 
considered the "highest" level in this context, meaning the least cleaning.  

- "prp" means that all products produced in this pipeline are deleted 
from the repository and the raw data triggered for re-processing.  
(The naming of this level is analogous to ISDC data analysis levels,
where "PRP" or "prepared" is the level just above "RAW".)

This option is not applicable for either ADP or the Input pipeline, which
cannot be re-run once completed.  

(There isn't really a prp level in the new format.)

- "raw" means that not only the above but even the raw data itself is
deleted from the repository.  But using this option on selective pieces 
of data in the  
pipelines is to be done with care and only in the case of major processing 
problems for which no recovery is possible except going back to PP.  It 
may not be used for ADP.  


=item B<inst>  

For now, can be ...
	ibis, ib (for qla)
	isgri, isg
	picsit, pic
	jemx1, j1
	jemx2, j2
	omc, o
	spi, sp


=item B<bydate>

Specify the --bydate flag to clean files whose OSFs are time stamped before a
given time.  This flag then causes the dataset parameter to be interpreted as
a time stamp in the format YYYYMMDDHHMMSS, where you can specify as little 
precision as you like, i.e. you can say --dataset=20021103 to clean anything 
whose OSF was created before 3 November 2003.  


=item B<match>

TESTING:  If the --match flag is given, the aim is to remove all OSFs the match
the given --dataset string.  (Remember, its the thought that counts, right?)

=item B<FORCE>

Do cleaning even if processes are running.  Rather dangerous as it will clean indices.


=item B<DEBUG>

Gives much more output that could be useful in debugging the script, should
the need arise.

=item B<do_not_confirm>

Use of this switch will cause the script NOT to ask if you are sure about
your decision to clean certain items.


=back

B<OPUS:>

Via OPUS, the "CL" column of the dataset in question should be changed
in the OMG to one of the following values:  "o" corresponding to level=opus 
or "r" (as in "re-run") for level=prp  as described above.  (The command line level 
"raw" is not allowed via OPUS to prevent possibly disastrous mistakes.)  
These options will respectively trigger the "cleanopus" and "cleanprp" 
processes, which will both run this script.  The cleanup to perform is
then determined by the PROCESS_NAME environment variable.  


=head1 DESCRIPTION

This script can be called from any ISDC OPUS pipeline which has a "CL" 
column at the end.  If this column has a value of "o", an OPUS-only cleanup
is performed:  the OPUS log, scratch data, and OSF are deleted while the 
repository remains untouched.  If this option is "r", all resulting data products which
are produced by that pipeline are deleted so that it can be re-run.  (Any
indices which list those products are also updated.)  The raw or input
dataset is then triggered again for reprocessing.  

Both of these can also be done from the command line.  But the command
line allows additional options to specify large-scale cleanups in the case
that either the data is old and has long been archived, or that the raw
data itself was wrong and the everything must be wiped out and run again from 
PP.  See the USAGE above.  

At each step, the script performs a variety of checks to attempt to ensure
that mistakes are not made.  

=head1 OPUS-only cleanup

In the case of the OPUS-only cleanup, the affected files are the same for 
every pipeline:  

=over 5

=item B<log>

The central link to the log file, i.e. $OPUS_WORK/<path>/logs/<dataset>.log

=item B<trigger>

The input trigger, i.e. $OPUS_WORK/<path>/input/<dataset>*

B<Note> that in the case of ADP, this is the input dataset itself, which
has been moved into the scratch directory which itself will be deleted.  
(See next.)  In this case, the original file is moved first into a 
subdirectory ("deleted") in the workspace.  

=item B<scratch>

Any files located in the scratch space directory, i.e.
$OPUS_WORK/<path>/scratch/<dataset>/

=item B<OSF>

The OSF itself on the blackboard.  

=back

The check to be performe first in this case is simply that the OSF shows
no current processing on this dataset.  

=head1 PRP cleanup

In the case of the deletion of data products for a re-run, the affected
files depend on the pipeline.  In all cases, the log file is deleted from
the repository, along with other data as follows:

=over 5

=item B<NRT- or CONSREV>

Again, in the Revolution File Pipeline, the affected data depends on the
type of the file selected.  Indices located either in the revolution 
level or at the mission level (under "idx") are updated.  Note that they
may have already affected other processing, and that is not recoverable 
unless the other data is also reprocessed.  (E.g. science windows processed
while an incorrect PICsIT fault list was present in the revolution data 
directory will have incorrect OSM results and should then be re-run after
the PICsIT data is corrected in the Rev. File Pipeline.)

=item B<NRT- or CONSSCW>

In the science window pipeline case, all results except for raw data are
deleted.  The only difference between doing it this way and simply restarting
from the DP step is that this can be done even after the science window
has finished successfully.  The indices are updated to remove the science
window in question, and all necessary permissions reset to writeable.  

=item B<NRTQLA>

Remove all qla data associated with this revolution.
cleanup.pl --path=revol_qla --dataset=0024              --level=raw

Remove all qla data associated with revolutions 0100 - 0160
cleanup.pl --path=revol_qla --level=raw --dataset=01\[0123456]

=item B<CONSSA>

TBD.  

=item B<CONSSSA>

Remove all data associated with this science window for specified instrument.
cleanup.pl --path=consssa   --dataset=ssii_002400050010 --level=raw

Remove all data associated with this science window for all instruments.
cleanup.pl --path=consssa   --dataset=002400050010      --level=raw --match

Remove all data associated with this revolution.
cleanup.pl --path=revol_ssa --dataset=0024              --level=raw

=item B<CONSSMA>

TBD.  

=item B<ARCDD>

TBD.

=back

(As stated above, this step is not applicable to Input or ADP.)

The checks to be performed in this case are (in addition to the OPUS-only 
case above) that all other OPUS processes are currently suspended.  This
requirement is due to the fact that parallel processing may read an index
of data which is about to be deleted.  

=head1 RAW cleanup

In the case of a full cleanup of a dataset, all of the above are done 
with the addition of the raw dataset itself and without any triggering
for a reprocessing.  In the case of science windows, it is the either 
the input or science window pipeline path which is specified, and in both
cases, the effect is the same of the science window being wiped from 
both OPUS workspaces and the repository.

In this complete cleanup, the operator will be queried to confirm the 
rather drastic deletion requested.  

For raw cleanup of an entire revolution of data, i.e. the path
revol_scw, the individual files are not detached from the indices
first.  Instead, all the data is removed first and then the  indices are
B<dal_clean>ed.  The pipelines should be suspended during this
operation.


=head1 AUTHORS

Jake Wendt <jake.wendt@obs.unige.ch>

Tess Jaffe <theresa.jaffe@obs.unige.ch>

=cut

