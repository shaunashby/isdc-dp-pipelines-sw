#!perl

=head1 NAME

I<csast.pl> - conssa ST step script

=head1 SYNOPSIS

I<csast.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

Handles the creation of the appropriate OSFs

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use OPUSLIB;
use UnixLIB;

print "\n========================================================================\n";
print "*******     Trigger $ENV{EVENT_NAME} received\n";

&ISDCPipeline::EnvStretch("LOG_FILES","OUTPATH","OBSDIR","PARFILES");

my ($dataset, $path, $suffix) = &File::Basename::fileparse($ENV{EVENT_NAME}, '\..*');

my $group;
my @files;
my @scwids;
my $retval;
my @result;
my @inst;
my $inst;
my $scwid;
my $int;
my $status = "$osf_stati{SA_OBS_ST_P}";
my %indices;
my $dcf;

print "*******     Triggering SA processing for $dataset;  looking for what OSFs to create.\n";

$ENV{OSF_DATASET} = $dataset;  # helps with FindObsScws below

chdir ( "$ENV{OBSDIR}/$dataset.000/" )
	or die "******* ERROR:  Cannot chdir to $ENV{OBSDIR}/$dataset.000/";

@files = sort(glob("og_*.fits"));

foreach $inst (@files) { 
	$inst =~ /(ibis|spi|jmx1|jmx2|omc)/;
	$inst = $1;
	$indices{"$inst"} = "swg_idx_$inst.fits";
	die "*******      ERROR:  No scw index ($indices{$inst}) found for instrument $inst" 
		unless (-e "$indices{$inst}") ;
	$inst =~ tr/a-z/A-Z/; 
	push @inst,$inst;
}

# For each instrument, create a set of OSFs:
INST:
foreach $inst (@inst) {
	$dcf = $inst;
	$dcf =~ s/JM/J/; # The DCF is only three, and we need the JEMX number.
	#	$status = "p--"; # reset this;  it was set below to cww
	$status = "$osf_stati{SA_OBS_ST_P}";
	
	($retval,@result) = &ISDCPipeline::RunProgram("$mymkdir $ENV{OBSDIR}/$dataset.000/logs");
	die ">>>>>>>     ERROR:  cannot mkdir $mymkdir "
		."$ENV{OBSDIR}/$dataset.000/logs:\n@result" if ($retval);
	# First, startup OSF for observation, with status p-- 
	#
	#  Note:  Instrument *must* be in OSF;  even if you specify a DCF,
	#   you cannot create identical datasets.  For the moment, I'll leave
	#   the DCF as well, since it's easier to use that in the display
	#   than it is to divide up the dataset in the OMG to sort...
	($retval,@result) = &ISDCPipeline::PipelineStart( 
		"dataset"     => "${dataset}_${inst}", 
		"state"       => "$status",  
		"type"        => "obs", 
		"dcf"         => "$dcf", 
		"logfile"     => "$ENV{LOG_FILES}/${dataset}_${inst}.log", 
		"reallogfile" => "$ENV{OBSDIR}/$dataset.000/logs/${dataset}_${inst}_log.txt", 
		);			       
	
	die "*******     ERROR:  couldn't start pipeline for ${dataset}_${inst}!" if ($retval);
	
	@scwids = sort(glob("$ENV{OBSDIR}/$dataset.000/scw/*"));
	if (!scalar(@scwids)) {
		&LogError(
			"dataset" => "${dataset}_${inst}",
			"inst"    => "$inst",
			"error"   => "no scws found in $ENV{OBSDIR}/$dataset.000/scw/\n",
			"stop"    => 1,
			);
	} # if none found
	
	foreach (@scwids) {
		#	s/.*(\d{12})$/$1/
		#										SPR 3902
		s/.*(\d{12})\.\d{3}$/$1/
			or &LogError(
				"dataset" => "${dataset}_${inst}",
				"inst"    => "$inst", 
				"error"   => "$_ is not recognized as a science window ID!",
				"stop"    => 1
				);
	}
	
	#  Except for SPI, create scw obsids (SPI only obs step?)
	if ($inst !~ /SPI/) {
		foreach $scwid (@scwids) {
			#      $swnum = sprintf("%03d",$swnum);
			
			($retval,@result) = &ISDCPipeline::PipelineStart(
				"dataset"     => "${dataset}_${inst}_${scwid}",
				"state"       => "$osf_stati{SA_ST_C}",
				"type"        => "scw",
				"dcf"         => "$dcf",
				"logfile"     => "$ENV{LOG_FILES}/${dataset}_${inst}_${scwid}.log",
				"reallogfile" => "$ENV{OBSDIR}/$dataset.000/logs/${dataset}_${inst}_${scwid}_log.txt",
				);		
			# again, try to put the error on the blackboard 
			&LogError(
				"dataset" => "${dataset}_${inst}",
				"inst"    => "$inst",
				"error"   => "cannot start pipeline for ScW $scwid;\n@result"
				) if ($retval);
			
			#      $swnum++;
			
		} # foreach science window
		
		# here, startup is done, but have to wait for scws 
		#	$status = "c--";
		$status = "$osf_stati{SA_OBS_ST_C}";
		
	} # if not SPI
	else {
		# for SPI, no science windows to wait for.
		$status = "$osf_stati{SA_ST_C}";
	}

	&ISDCPipeline::PipelineStep( 
		"step"         => "ST - Startup Done.",
		"program_name" => "osf_update -p conssa.path -f ${dataset}_${inst} -t obs -n $dcf -s $status",
		"logfile"      => "$ENV{LOG_FILES}/${dataset}_${inst}.log", 
		);			       
	
	#	060711 - Jake - SCREW 1854
	if ($inst =~ /IBIS/) {
		&ISDCLIB::DoOrDie ( "$mycp -p $ENV{ISDC_OPUS}/conssa/rebinned_corr_ima.fits.gz $ENV{OBSDIR}/$dataset.000/" );
		&ISDCLIB::DoOrDie ( "$mychmod -w $ENV{OBSDIR}/$dataset.000/rebinned_corr_ima.fits.gz" );
	}
} # foreach inst

exit 0;

######################################################################

=item LogError

This LogError is much different than the others, so I am leaving it as is.

=cut

sub LogError {
	
	my %att = @_;
	
	&ISDCPipeline::RunProgram("osf_update -p conssa.path -f $att{dataset} -s $osf_stati{SA_OBS_ST_X}");
	&ISDCPipeline::PipelineStep(
		"step"         => "ST ERROR",
		"program_name" => "ERROR",
		"logfile"      => "$ENV{LOG_FILES}/$att{dataset}.log",
		"error"        => "$att{error}",
		"stoponerror"  => $att{stop},
		);
	
} # end LogError

=back

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

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

#	last line
