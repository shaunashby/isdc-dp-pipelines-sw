#!perl

=head1 NAME

adpst.pl - ISDC Auxiliary Data Preparation Pipeline, start task

=head1 SYNOPSIS

I<adpst.pl> - Run from within B<OPUS>.  This is the first step of a
three stage pipeline which does Auxiliary Data Preparation.  The
second step is B<adp.pl> and the last step is B<adpfin.pl>.

=head1 DESCRIPTION

I<adpst.pl> - Run from within B<OPUS>.  This is the first step of the
three step ADP pipeline.  The purpose of this step is to trigger on
the existance of a file and create an B<OPUS> observation entry which
will be used to trigger the other steps of the pipeline.  All
triggering is done from the B<ADP_INPUT> directory and the triggering
is done by B<OPUS>.  By the time the script is running the file
already exists in the input directory.  The main job of the script is
to create and open the log file for the processing of this ADP file
and to create an Observation Status File which enters the file into
the B<OPUS> processing stream.

=head1 SUBROUTINES

=over

=cut


use strict;
use ISDCPipeline;
use ISDCLIB;
use OPUSLIB;
use UnixLIB;
use File::Basename;

my $dirname;			#	this var is set and then the dir is made
my $rev;					#	most types return this var (needed globablly)
my $logfilename;
my $logfiledir;		#	this var is set and then the dir is made
my $type;

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","AUXDIR","LOG_FILES","ADP_INPUT","ALERTS","ARC_TRIG","PARFILES");

my $orgdir = "$ENV{AUXDIR}/org/";
my $adpdir = "$ENV{AUXDIR}/adp/";

if (!(-e $orgdir)) {
	`$mymkdir -p $orgdir`;
	die "*******     ERROR:  cannot mkdir $orgdir" if ($?);
}

if (!(-e $adpdir)) {
	`$mymkdir -p $adpdir`;
	die "*******     ERROR:  cannot mkdir $adpdir" if ($?);
}

my ($adpfileSave, $path, $adpextension) = &File::Basename::fileparse($ENV{EVENT_NAME},'\..*');
$adpextension =~ /(.*)_(.*)/;
$adpextension = $1;

my $adpfile = $adpfileSave.$adpextension;  # this holds the actual file 
my $dataset = $adpfileSave.$adpextension;


print "\n=================================================================\n";
print "                   RECEIVED: $dataset\n";

# which one did we get?

SWITCH: {
	
	# pad, iop, ocs (maybe)		#	060118 - Jake - Why were these separate?  They're almost exactly the same.
	if ($dataset =~ /(pad|iop|ocs)_([0-9]{2})_?.*/) {
		$type       = $1;
		my $ao      = "AO$2";
		$dirname    = "$adpdir/$ao/";
		$logfiledir = "$adpdir/$ao/";
		if ( ( $type =~ /ocs/ ) && ( ! -e $dirname ) ) {
			`$mymkdir -p $dirname`;
			die "*******     ERROR:  cannot mkdir $dirname" if ($?);
			$dirname    .= "$type/";
			$logfiledir .= "$type/";
		}
		last SWITCH;
	}
	
	#pod, opp		#	060118 - Jake - Why were these separate?  They're exactly the same.
	if ($dataset =~ /(pod|opp)_([0-9]{4})_.*/) {
		$type       = $1;
		$rev        = $2;
		$dirname    = "$adpdir/$rev.000/";
		$logfiledir = "$adpdir/$rev.000/logs/";
		last SWITCH;
	}

	#paf, asf, ahf		#	060118 - Jake - Why were these separate?  They're quite similar.
	if ($dataset =~ /([0-9]{4})_([0-9]{2,4})\.(PAF|ASF|AHF)/) {
		$rev        = $1;
		my $vers    = $2;
		$type       = lc ( $3 );
		$dirname    = "$adpdir/$rev.000/";
		$logfiledir = "$adpdir/$rev.000/logs/";
		$logfiledir .= "$type/" if ( $dataset =~ /ASF/ );
		if ( $type == "ahf" ) {	#	060210 - Jake - SPR 4392
			die   "*******     ERROR : $adpdir/$rev.000/attitude_historic.fit* already exists!" 
				if ( glob ( "$adpdir/$rev.000/attitude_historic.fit*" ) );
		}
		last SWITCH;
	}

#	# pad
#	if ($dataset =~ /pad_([0-9]{2})_.*/) {
#		$ao = "AO$1";
#		$dirname = "$adpdir/$ao/";
#		$logfiledir = "$adpdir/$ao/";
#		$type = "pad";
#		last SWITCH;
#	}
#	
#	# iop
#	if ($dataset =~ /iop_([0-9]{2}).*/) {
#		$ao = "AO$1";
#		$dirname = "$adpdir/$ao/";
#		$logfiledir = "$adpdir/$ao/";
#		$type = "iop";
#		last SWITCH;
#	}
#	#pod
#	if ($dataset =~ /pod_([0-9]{4})_.*/) {
#		$rev = $1;
#		$dirname = "$adpdir/$rev.000/";
#		$logfiledir = "$adpdir/$rev.000/logs/";
#		$type = "pod";
#		last SWITCH;
#	}
#	#opp
#	if ($dataset =~ /opp_([0-9]{4})_.*/) {
#		$rev = $1;
#		$dirname = "$adpdir/$rev.000/";
#		$logfiledir = "$adpdir/$rev.000/logs/";
#		$type = "opp";
#		last SWITCH;
#	}
#	#ocs
#	if ($dataset =~ /ocs_([0-9]{2}).*/) {
#		$ao = "AO$1";
#		my $shortDirName = "$adpdir/$ao";
#		# Now the AO directory might not exist
#		if (!(-e $shortDirName)) {
#			# doesn't exist, make it
#			`$mymkdir -p $shortDirName`;
#			die "*******     ERROR:  cannot mkdir $shortDirName\n" if ($?);
#		}
#		
#		$dirname = "$shortDirName/ocs/";
#		$logfiledir = "$adpdir/$ao/ocs/";
#		# sadly, $adpfile is too long here.  Make it shorter.
#		#    $adpfile = $adpfileSave."_fi";  # not necessary in OPUS 3.2
#		$type = "ocs";
#		last SWITCH;
#	}


#	#paf
#	if ($dataset =~ /([0-9]{4})_([0-9]{2})\.PAF/) {
#		$rev        = $1;
#		$dirname    = "$adpdir/$rev.000/";
#		$logfiledir = "$adpdir/$rev.000/logs/";
#		$type       = "paf";
#		last SWITCH;
#	}
#	#asf/ahf
#	if ($dataset =~ /([0-9]{4})_([0-9]{4})\.(ASF|AHF)/) {
#		$rev        = $1;
#		my $vers    = $2;	#	060118 - Jake - SPR 4392
#		$type       = lc ( $3 );
#		$dirname    = "$adpdir/$rev.000/";
#		$logfiledir = "$adpdir/$rev.000/logs/";
#		if ($dataset =~ /ASF/) {
#			$logfiledir .= "asf/" ;
##			$type = "asf";
#		}
###		else {
###			$type = "ahf";
##			if ( $vers !~ /0001/ ) {
##				print "*******     ";
##				print "*******     ";
##				print "*******     ";
##				print "*******     WARNING!  This AHF is not version 0001!  I am about to make a mess of things.";
##				print "*******     ";
##				print "*******     With regards to SPR 4392, this may get ugly.  If arc_prep has already run,";
##				print "*******     the directories are write-protected.  attitude_historic.fits.gz may already exist.";
##				print "*******     ";
##				print "*******     ";
##				print "*******     ";
##				die   "*******     attitude_historic.fits.gz does exist!" if ( "$adpdir/$rev.000/attitude_historic.fits.gz" );
##			}
###		}
#		last SWITCH;
#	}

	#orb
	if ($dataset =~ /orbita.*/) {
		$dirname = "$adpdir/ref";
		if (!(-e $dirname)) {
			`$mymkdir -p  $dirname`;
			die "*******     ERROR:  cannot mkdir $dirname" if ($?);
		}
		
		#$dirname = "$adpdir/ref/orbit/";
		my $date = `$mydate -u "+%Y%m%d"`;		#	050412 - Jake - SCREW 1704
		chomp($date);
		if ($ENV{ADP_UNIT_TEST_ORB} =~ /TRUE/) {
			#  Fudge for unit test, where we need two orbit files processed in one day:
			$date = $date+1;  # may be an "unphysical" date, but nobody's checking.      
		}
		$adpfile = "orbita_$date";
		
		## Temporary place for log file to start;  must later get last revolution 
		##   covered by this orbit file from convertorbit parameter file and put log
		##   in that rev directory.  
		$logfiledir = "$adpdir/";
		$type = "orb";
		last SWITCH;
	}
	#rev
	if ($dataset =~ /revno.*/) {
		$dirname = "$adpdir/ref";
		if (!(-e $dirname)) {
			`$mymkdir -p $dirname`;
			die "*******    ERROR:  cannot mkdir $dirname" if ($?);
		}
		
		$dirname = "$adpdir/ref/revno/";
		my $date = `$mydate -u "+%Y%m%d"`;	#	050412 - Jake - SCREW 1704
		chomp ($date);
		$adpfile = "revno_$date";
		$logfiledir = "$adpdir/ref/revno/";
		$type = "rev";
		last SWITCH;
	}
	#tsf
	if ($dataset =~ /TSF_([0-9]{4})_.*_([0-9]{4}).*INT/) {
		$rev = $1;
		my $vers = $2;
		$dirname = "$adpdir/$rev.000/";
		$adpfile = "TSF_${rev}_${vers}.INT";
		$logfiledir = "$adpdir/$rev.000/logs/";
		$type = "tsf";
		last SWITCH;
	}
	#olf
	if ($dataset =~ /.*OLF/) {
		# this has to be moved later...
		$dirname = "$adpdir/";
		#    $logfiledir = "$adpdir/";
		#  Put these instead in workspace until revolution determined.  Never
		#   for empty ones.  
		$logfiledir = $ENV{LOG_FILES};
		$type = "olf";
		last SWITCH;
	} 
	
	if ($dataset =~ /(\d{4})_arc_prep.*/) {
		# archive triggering
		$rev = $1;
		$adpfile = "${rev}_arc_prep";
		$dirname = "$adpdir/$1.000";
		$logfiledir = "$dirname/logs/";
		$type = "arc";
		last SWITCH;    
	}
	
	#  THF files:  "THF_020607_0003.DAT"
	#   get YYMMDD not rev, similar to OLFs, so log temporarily in LOG_FILES
	#   central area, moved later.
	if ($dataset =~ /(THF_\d{6})_(\d{4}).DAT/) {
		#  This has to be moved later
		$dirname = "$adpdir";
		$logfiledir = $ENV{LOG_FILES};
		$type = "thf";
		last SWITCH;    
	}
	
	
	print "=======         file $dataset not recognized\n";
	# exit with status 3 so suffix won't be added;  see resource file
	exit 3;
}
print "=======         file is typ $type\n";

#  The central OPUS log file is just dataset.log, a link to <filename>_log.txt
#   or something in the repository.  So here is the real one, below the link.
$logfilename = "$logfiledir/${adpfile}_log.txt";
$logfilename =~ s/\.fits//;

# change all dots to underscores in dataset names (OPUS 3.2)
$adpfile =~ s/\./_/;		#	060119 - Jake - Actually this only changes the first one.  Is this OK?  I think that there is only one.

print "=======         log is $logfilename\n";

# does the directory exist?
if (!-e $dirname) {
	`$mymkdir -p $dirname`;
	die "*******     ERROR:  cannot mkdir $dirname" if ($?);
}
# does the log file directory exist?
# (using mkdir -p to create diretories recursively, if necessary
# since for the data directory, they are made as needed but the log
# has to be there from the start.
if (!-e $logfiledir) {
	`$mymkdir -p $logfiledir`;
	die "*******     ERROR:  cannot mkdir $logfiledir" if ($?);
}

my $opuslink = "$adpfile.log";
print "=======         OPUS link is $opuslink\n";

my $alert = 0;
my $continue = 1;
my $level = 1;  # most are level 1
my $status;				#	this var is used just for the osf start status
my $message;
my $id;
if ( &CheckRev($rev) ) {
	$status = "$osf_stati{ADP_ST_X}";
	$alert++;
	$message = "ADP file $adpfile received after rev $rev triggered for archiving";
	$id = "508"; 
	$level = 2;
}
else {
	$status = "$osf_stati{ADP_ST_C}";
}

#  Note that OPUS_OBSERVATIONS_DIR isn't set here, since this is startup.
my @prev_osfs = `$myls $ENV{OPUS_WORK}/adp/obs/*$adpfile* 2> /dev/null`;
#  First, check that we didn't already get this or already triggered for 
#    archiving:
if ( ($alert) || (-e "$logfilename") || (@prev_osfs) ) {
	
	if ( (-e "$logfilename") && (!-w "$logfilename")) {
		print "*******     ERROR:  cannot write to $logfilename\n";
		$continue--;
		$alert++;
		$message = "Duplicate file $adpfile received in ADP pipeline";
		$id = 507;
	}
	if ( (-e "$logfilename") && (-w "$logfilename")) {
		print "*******     WARNING:  $logfilename already exists, but it's writable so continuing.\n";    
	}
	if (@prev_osfs) {
		print "*******     ERROR:  OSF $prev_osfs[$#prev_osfs] already exists!\n";
		$message = "Duplicate file $adpfile received in ADP pipeline";
		$id = 507;
		$alert++;
		$continue--;
	}
	
	if ($alert) {
		# must clean out old alerts first, because need to run am_cp from same
		#  place whenever we get this case:
		my ($retval, @result) = &ISDCPipeline::RunProgram("$mymkdir -p $ENV{WORKDIR}/adpst") unless (-d "$ENV{WORKDIR}/adpst");
		die "*******     ERROR:  cannot mkdir $ENV{WORKDIR}/adpst:  @result" if ($retval);
		chdir "$ENV{WORKDIR}/adpst" or die "*******     ERROR:  cannot chdir $ENV{WORKDIR}/adpst";
		&ISDCPipeline::RunProgram("$myrm -f *alert*",1) if (`$myls *alert* 2> /dev/null`);
		$ENV{COMMONLOGFILE} = "$ENV{LOG_FILES}/adpst_alerts.log";
		
		&ISDCPipeline::WriteAlert(
			"message" => "$message",
			"level"   => $level,
			"subdir"  => "$ENV{WORKDIR}/adpst",
			"id"      => "$id",
			);
		
		delete $ENV{COMMONLOGFILE};
		($retval,@result) = &ISDCPipeline::RunProgram("am_cp OutDir=$ENV{ALERTS} OutDir2= Subsystem=ADP DataStream=realTime");
		die "*******     ERROR:  cannot copy alert: @result" if ($retval);
		&ISDCPipeline::RunProgram("$myrm -f *alert*",1);
		#  Exit when can't make an OSF, but if status is set to x, that means
		#   the failure was the rev already archived and 
		exit 1 unless ($continue);
	}
	
} # If something already exists


#$retval = &ISDCPipeline::PipelineStart(
&ISDCPipeline::PipelineStart(
	"pipeline"    => "NRT ADP Start",
	"state"       => "$status",
	"dataset"     => "$adpfile",
	"type"        => "$type",
	"reallogfile" => "$logfilename",
	"logfile"     => "$ENV{LOG_FILES}/$opuslink",
	);

exit;

###########################################################################

=item B<CheckRev> ( $revnum )

=cut

sub CheckRev {
	#check if rev has already been triggered for archive ingest;  should not
	#  have any new files tiggered if so!
	#
	# returns 1 if already triggered;  this used to set OSF to "x" on creation.
	my ($revnum) = @_;
	return 0 unless $revnum;
	print "checking status of rev repository for rev number $revnum\n";
	
	if (-w "$ENV{AUXDIR}/adp/$revnum.000/") {
		print "repository $ENV{AUXDIR}/adp/$revnum.000/ writeable\n";
		return 0;
	}
	else {
		print "repository $ENV{AUXDIR}/adp/$revnum.000/ NOT writeable!  Must have been triggered for archiving already.\n";
		return 1;
	}
}





__END__


=back

=head1 ACTIONS

Note that the patterns below are Perl regular expressions.
Documentation on these can be found in Programming Perl, Second
Edition, Larry Wall et al, Page 57.

=over 5

=item B<pad_([0-9]{2})_.*>

This is a pad file which has a 2 digit AO number in the file name.
This sets the log file directory to the proper AO directory and the 
type to "pad".

=item B<iop_([0-9]{2}).*>

This is an iop file which has a 2 digit AO number in the file name.
This action sets the log file directory to the proper AO directory and the type to "iop".

=item B<pod_([0-9]){4})>

This is a pod file which has a 4 digit revolution number in the file
name.  This action sets the log file directory to the proper
revolution directory and the type to "pod".

=item B<opp_([0-9]){4})>

This is an opp file which has a 4 digit revolution number in the file
name.  This action sets the log file directory to the proper
revolution directory and the type to "opp".

=item B<ocs_([0-9]{2}).*>

This is a ocs file which has a 2 digit AO number in the file name.
The original name of the file, ending in ".fits", is just barely too 
long for an OPUS observation, so the OSF name is set to "_fi" instead.  
This action sets the log file directory to an "ocs" subdirectory of the 
proper AO directory.  It sets the type to "ocs".   

=item B<([0-9]{4})_([0-9]{2})\.PAF>

This is a PAF file which has a 4 digit revolution number in the file
name.  This action sets the log file directory to the "logs" subdirectory of 
the proper revolution directory.  It sets the type to "paf".

=item B<([0-9]{4})_([0-9]{4})\.(ASF|AHF)>

This is either a ASF or an AHF file which has a 4 digit revolution
number in the file name.  This action sets the log file directory to
the "logs" subdirectory of the the proper revolution directory. If it is an
ASF file, the logfile directory is in yet another subdirectory named "asf".
The type is set to either "asf" or "ahf".  

=item B<orbita>

This is the orbita file which was renamed by I<adpmon.pl> to
orbita.orbita.  The file is renamed orbita_YYYYMMDD and the log file
is set to the top level "adp" directory for now.  It will be moved later;  
see adp.pl.  The type is set to "orb".  

=item B<revno>

This is the revno file which was renamed by I<adpmon.pl> to
revno.revno.  Now it is renamed to revno_YYYYMMDD and the log file is
set to the the adp/ref/revno/ subdirectory.  The type is set to "rev".

=item B<TSF_([0-9]{4})_.*_([0-9]{4}).*INT>

This is a TSF file which has a 4 digit revolution number in the file
name.  This action sets the log file directory to the "logs" subdir of the 
proper revolution directory.  The type is "tsf".  The long name of the
original TSF file is shortened to be simply TSF_RRRR_VVVV.INT.  

=item B<.*OLF>

This is a OLF file.  It is not possible to derive the proper directory
from the file name since the revolution number is not contained in the
file name.  For now this log file is put in the adp directory and it
will be moved later.  The type is set to "olf".

=item B<odd>

If none of the above types matches the input, the file is moved to the "odd" 
subdirectory of the input directory, and an alert is sent.  

=head1 RESOURCE FILE

The resource file I<adpst.resource> contains all the file triggers for
the B<ADP> pipeline.

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<OUTPATH>

This is the top of the repository, set to the B<rii> entry in the path file.

=item B<WORKDIR>

This is the pipeline workspace, set to the B<adp_work> entry of the path file.  
=item B<AUXDIR> 

This is the location of the auxiliary data repository and is usally set to
the B<rii_aux? entry in the path file.

=item B<LOG_FILES>

This is the central log file directory, set to the B<log_files> entry in the 
path file.

=item B<ALERTS>

This is where to write alerts, set to the B<alerts> entry in the path file.

=item B<ADP_INPUT> 

This is the input directory where IFTS deposits Auxiliary Data, set to the
B<adp_input> entry in the path file.  

=back


=head1 RESTRICTIONS

Any changes in the input file names will require changes in this
script since it matches on the file names.  Note that corresponding
changes will also have to be made in I<adp.pl> and I<adpmon.pl>.






=head1 REFERENCES

For further information on B<adp.pl> or B<adpfin.pl> please run
perldoc on those files, ie, C<perldoc adp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

=head1 AUTHORS

Bruce O'Neel <bruce.oneel@obs.unige.ch>

Tess Jaffe <Theresa.Jaffe@obs.unige.ch>

Jake Wendt <jake.wendt@obs.unige.ch>

=cut

