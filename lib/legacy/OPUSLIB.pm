package OPUSLIB;

=head1 NAME

I<OPUSLIB.pm> - some generic OPUS related perl functions

=head1 SYNOPSIS

use I<OPUSLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut


use strict;
use warnings;

use File::Basename;
use Carp;
use TimeLIB;

use ISDCLIB;
use UnixLIB;
use SSALIB;

use base qw(Exporter);
use vars qw( $VERSION @EXPORT_OK %EXPORT_TAGS );

%EXPORT_TAGS = (
    'osf_stati'   => [ qw( %osf_stati ) ],
    );

@EXPORT_OK = ( @{ $EXPORT_TAGS{'osf_stati'} } );

$VERSION = '0.1';

our %osf_stati = (
	ADP_ST_X			=> "xww",
	ADP_ST_C			=> "cww",
	ADP_COMPLETE	=> "ccc",
	ADP_CLEAN		=> "ccco",
	
	SCW_ST_X			=> "xwwww",
	SCW_ST_C			=> "cwwww",
	SCW_DP_H			=> "chwww",
	SCW_DP_C			=> "ccwww",
	SCW_DP_W_COR_H	=> "cwhww",
	SCW_DP_C_COR_H	=> "cchww",
	SCW_COMPLETE	=> "ccccc",
	SCW_CLEAN		=> "ccccco",
	
	REV_ST_X			=> "xww",
	REV_ST_C			=> "cww",
	REV_GEN_H		=> "chw",
	REV_GEN_C		=> "ccw",
	REV_COMPLETE	=> "ccc",
	REV_CLEAN		=> "ccco",             
	
	SA_OBS_ST_P		=> "p--", #       obs is starting
	SA_OBS_ST_X		=> "x--", #       obs has errored
	SA_OBS_ST_C		=> "c--", #       obs has started, but must wait for science windows      csast.pl
	SA_ST_C			=> "cww",
	SA_COMPLETE		=> "ccc",
	SA_CLEAN			=> "ccco",
	
	INP_ST_C			=> "cww",
	INP_COMPLETE	=> "ccc",
	INP_CLEAN		=> "ccco",
	
	QLA_ST_C			=> "cww",
	QLA_COMPLETE	=> "ccc",
	QLA_CLEAN		=> "ccco",
	
	SSA_ST_C			=> "cww",
	SSA_ST_X			=> "xww",
	SSA_COMPLETE	=> "ccc",
	SSA_CLEAN		=> "ccco",
	
	SMA_ST_C			=> "cww",	#	used in crvmon.pl
	SMA_COMPLETE	=> "ccc",	#	used in crvmon.pl
	
	);
	

##############################################################################

=item B<CheckOSF> ( $path, $dataset )

Function to simply check that no processes are running on the OSF now.

=cut

sub CheckOSF {
	my ($path,$dataset) = @_;

	#  print "$prefix1 Running \' osf_test -p $path.path -f $dataset \'\n"; 
	my @result = `osf_test -p $path.path -f $dataset`;
	#  Should only return one line;  otherwise, confusion of OSFs.

	#  051129 - Jake - SPR 4378 - osf_test does not give a return value ($?)
	#     Its 0 unless syntax is completely wrong (if $dataset or $path is "")
	die "$prefix1 ERROR 009:  \`osf_test -p $path.path -f $dataset\` failed." if ($?);

	if ( @result ) {
		die "$prefix1 ERROR 010:  dataset $dataset matches mutliple OSFs!" if ($#result > 0);
		chomp $result[0];
		#  print "$prefix1 OSF:  $result[0]\n";
		#  Entry looks like 
		#  XXXXXXXX-ssssss______<snip>_______.DATASET______<snip>______-typ-dcf-___
		#	4395ce39-c--_____________________.soib_02201330003_002_IBIS_______________________________________-obs-IBI-____
		#  hex_time status    etc.
		#  So match between first - and .
		#	051209 - Jake - status can include "-" as well as added here.
		$result[0] =~ /^[0-9a-z]+-([a-z-]+)_/;
		die "$prefix1 ERROR 011:  dataset $dataset status is $1, still processing." if ($1 =~ /p\w+/);
	} else {
		print        ( "$prefix1 WARNING:  cannot get status of OSF $dataset in path $path\n" );
		print STDOUT ( "$prefix1 WARNING:  cannot get status of OSF $dataset in path $path\n" );
	}

	return;
} #  end sub CheckOSF


#############################################################################

=item B<CheckProcs> ( $warn )

Function to simply check that no processes are running in OPUS now.

=cut

sub CheckProcs {
	my ( $warn ) = @_;

	my @contents;
	my $stop;
	my $proc;
	my $status;

	@contents = sort ( glob ( "$ENV{OPUS_WORK}/opus/*" ) );
	foreach ( @contents ) {
		#  looks like:
		# 00002129-ddipin___-idle___________.3bf2495f-arcdd____-isdcul10____________-____
		# where "idle" is the current status slot, and the very end is the command
		#  (though the command doesn't matter; even if it's been commanded to halt,
		#   we can't touch anything until it *is* halted or suspended.)
		next unless (/[0-9a-z]+-(\w+)-(\w+)_*\..*-(\w{4})$/);
		#  (Note that proc and status will both still have trailing __'s)
		$proc = $1;
		$status = $2;
		next if ($proc =~ /clean/);
		#  Input processes irrelevant to cleanprp;  they don't read any indices:
		next if ($proc =~ /(n|c)inp/);



		#	FIX ??? 051205
		#	should add all the processes here that are OK to run when cleaning.
		#	OR just use the --FORCE option




		$stop++ unless ($status =~ /susp|absent/);
		print "$prefix1 Process $proc still running!\n" unless ($status =~ /susp|absent/);
	}
	
	if ( $stop ) {
		if ( $warn ) {
			print        ( "$prefix1 WARNING 018:  should not clean while $stop processes running.\n" );
			print STDOUT ( "$prefix1 WARNING 018:  should not clean while $stop processes running.\n" );
			print        ( "$prefix1 Cleanup.pl was given --FORCE so doing so anyway.\n" );
			print STDOUT ( "$prefix1 Cleanup.pl was given --FORCE so doing so anyway.\n" );
		} else {
			die "$prefix1 ERROR 018:  cannot clean while $stop processes running";
		}
	}

	return;
} #  end sub CheckProcs


###############################################################################

=item B<RemoveDatasetsAfterDate> ( %att )

This function takes a path, list of datasets and a date and removes all datasets that are after the given date.  It then returns this new list of datasets.

=cut

sub RemoveDatasetsAfterDate {

	my %att = @_;
	my $path = $att{path};
	my @datasets = @{ $att{datasets}};
	my $do_not_confirm = $att{do_not_confirm};
	my $date = $att{date};
	my $answer;

	#  Pack the right side with zeros until it's 14 characters, i.e.
	#   if somebody says 20021121, then make it 20021121000000.  That
	#   way, people don't have to specify the seconds but *can* if they want.
	while ($date !~ /^\d{14}$/) {
		$date = $date."0";
	}

	print STDOUT "$prefix1 Selecting datasets before date==$date\n";

	#  Take the array of OSFs, 
	my @newlist;
	foreach my $onedataset ( @datasets ) {
		chomp $onedataset;
		$onedataset =~ s/\s+//g;
		next unless $onedataset;
		my $onedate = &OSFTimeStamp ( "dataset" => "$onedataset", "path" => "$path" );
		push @newlist, $onedataset if ($onedate < $date);
	}

	unless ( $do_not_confirm ) {
		#  This is only from the command line:  confirm:
		print STDOUT "$prefix1 Datasets to Clean for Path $path:\n\t".join("\n\t",@newlist)."\n";
		print STDOUT "$prefix1 CONFIRM:  do you really want to clean all of these?\n";
		print STDOUT "$prefix1 If so, please type \`yes\`:  ";
		while(<STDIN>) { $answer = $_; chomp $answer; last; }
		if ($answer !~ /^yes$/) {
			print STDOUT "$prefix1 You did not answer \`yes\`;  quitting\n";



#	FIX : 
#	do I really need to close this log?
#			if ($pars{log}){  close(LOG); }



			exit;
		}
	}

	#  Replace original list with new one.
	return @newlist;
}


##############################################################################

=item B<ParseOSF> ( $osf )

Give an OSF, i.e. literally a string containing, e.g.
3db18375-ccc_____________________.0087_arc_prep___________________________________________________-arc-000-____

Will return array with all fields in the following order:
(hextime,status,name,type,DCF,command)

=cut

sub ParseOSF { 
	my ($osf) = @_;
	chomp $osf;  #  in case it was called e.g. ParseOSF(`ls *blah*`)
	$osf = &File::Basename::basename ( $osf );
	#  time-status____.dataset_____-type-dcf-command 
	$osf =~ /^(\w{8})-([a-z-]+)_+\.(\w.*[a-z0-9])___+-(\w{3})-(\w{3})-(.*)$/i
		or die "*******     ERROR:  cannot parse OSF $osf";
	return ($1,$2,$3,$4,$5,$6);
}


##############################################################################

=item B<ParsePSTAT> ( $pstat )

Give an OSF, i.e. literally a string containing, e.g.
00000dda-nswst____-idle___________.3dd4b4ac-nrtscw___-nrtscw2_____________-____

Will return array with all fields in the following order:
($pid,$process,$status,$time,$path,$node,$command)

=cut

sub ParsePSTAT { 
	my ($pstat) = @_;
	#  {PID}-{PROCESS}-{PROC_STAT}.{START_TIME}-{PATH}-{NODE}-{PROC_CMD}
	#$pstat =~ /^(\w{8})-(\w{9})-(\w{15}).(\w{8})-(\w{9})-(\w{20})-(\w{4})$/;
	#	SPR 4384 - when PROC_STAT has a "-" in it, this parse fails.

	#	SCREW 1838 - 15 is the old length and 40 is the new one
	$pstat =~ /^(\w{8})-(\w{9})-([\w-]{15,40}).(\w{8})-(\w{9})-(\w{20})-(\w{4})$/;
#	$pstat =~ /^(\w{8})-(\w{9})-([\w-]{15}).(\w{8})-(\w{9})-(\w{20})-(\w{4})$/;

	my ($pid,$process,$status,$time,$path,$node,$command) = ($1,$2,$3,$4,$5,$6,$7);
	#  Strip off filling _'s:
	foreach ($pid,$process,$status,$time,$path,$node,$command) {
		s/^(\w+?)_*$/$1/;
	}
	return ($pid,$process,$status,$time,$path,$node,$command);
}

##############################################################################

=item B<OSFTimeStamp> ( %att )

This function examines an OSF_DATASET, grabs it's hex time tag, and converts that tag into UTC using the OPUS tool time_stamp and then a bit of reformatting.

=cut

sub OSFTimeStamp {
	my %att = @_;

	$att{path} = $ENV{PATH_FILE_NAME} unless ($att{path});
	$att{dataset} = $ENV{OSF_DATASET} unless ($att{dataset});
	#  PATH_FILE_NAME has e.g. "nrtrev.path".
	$att{path} =~ s/\.path//;

	my @result = `osf_test -p $att{path} -f $att{dataset}`;
	die "*******     Cannot find $att{path} $att{dataset}" unless (@result);
	chomp $result[$#result];
	die "*******     Confused about $att{path} $att{dataset}. $#result - $result[0]"
		if ( $#result > 0 );

	#  Pass through parser
	my ($hex) = &ParseOSF ( $result[$#result] );

	#  Convert:
	return &TimeLIB::HexTime2Local($hex);
}  # end OSFTimeStamp


##############################################################################

=item B<OSFstatus> ( %att )

check the status of the OSF for a list of files;  give
files=<file or pattern match of files to be globbed>
column=<specific column name, e.g. "SF">
path=<path file name, e.g. nrtscw.path>

returns exit status and array of OSF status letters of that column

automatically skips filename matching $ENV{OSF_DATASET}

=cut

sub OSFstatus {
	
	&Carp::croak ( "OSFstatus: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	my @output;
	my $osf;
	my @results;
	my $retval;
	my $one;
	my ($hextime,$osfstatus,$dataset,$type,$dcfnum,$command);
	
	return 1 unless ( ($att{files}) && ($att{path}));
	$att{match} = "" unless ( defined $att{match} );

	#  NOTE:  this glob, if given just one OSF with no wild card to match,
	#   will just return the one string given and a blank space, sometimes:
	my @files = sort(glob("$att{files}"));

	#	0508-- - Jake - SPR 4291
	if ( ( $att{match} =~ /sma/ ) || ( $att{match} =~ /ssa/ ) || ($att{match} =~ /scw/ ) ) {
		my @newfiles;
		foreach (@files) { 
			if ( ( $att{match} =~ /sma/ ) && ( ! -z $_ ) ) {
				my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $_ );
				push @newfiles, $osfname;
			}
			elsif ( ( $att{match} =~ /ssa/ ) && ( -z $_ ) ) {
				my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $_ );
				push @newfiles, $osfname;
			}
			# Sometimes occurs that scw is used as type for starting consssa automatically, yet ssa is used
			# to check the OSFs. SO, we add a block to handle this case. Really a hack in disguise.
	        elsif ( ( $att{match} =~ /scw/ ) && ( -z $_ ) ) {
	        	print "- Nasty scw/ssa type hack in effect.\n" if ($ENV{DEBUGIN});
                my ( $osfname, $dcf, $inst, $INST, $revno, $scwid ) = &SSALIB::Trigger2OSF ( $_ );
                push @newfiles, $osfname;
            }
			
		}
		@files = @newfiles;
	}

	if (@files) {
		print ">>>>>>>     Found ".($#files + 1)." files\n";
	}
	else {
		print ">>>>>>>     WARNING:  found no files!\n";
		return;
	}
	
	print "\n******************************************************************\n";
	print "       ".&TimeLIB::MyTime()."       CHECKING STATUS OF $att{files} matching \"$att{match}\":\n";
	
	foreach $one (@files){
		next unless ($one =~ /\w+/);  # sometimes glob returns blanks.
		$one = &File::Basename::fileparse($one,'\..*');
		next if ( ($ENV{OSF_DATASET}) && ($one =~ /$ENV{OSF_DATASET}/)) ;
		
		$osf = `$myls $ENV{OPUS_WORK}/$att{path}/obs/*$one*$att{match}*`;
		if ($?) {
			print "*******     WARNING:  cannot \'$myls $ENV{OPUS_WORK}/$att{path}/obs/*$one*$att{match}*\':  "
				."not even started yet.  Returning status \'_\'.\n";
			push @results, "_";
			next;
		}
		
		chomp $osf;
		$osf = &File::Basename::basename($osf);
		
		($hextime,$osfstatus,$dataset,$type,$dcfnum,$command) = &ParseOSF ($osf);
		
		if (defined $att{column}) {
			#  offset is the number you want minus 1 (3rd is 2, starting from 0)
			$osfstatus = substr $osfstatus, ($att{column} - 1), 1;
			print "*******     Status of $one is $osfstatus.\n";
			push @results, $osfstatus;
		}
		
		else {
			print "*******     Status of $one is $osfstatus.\n";
			push @results, $osfstatus if $osfstatus;
		}
	}
	print "************* end CHECKING STATUS ********************************\n";
	
	return 0,@results;
}


1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
