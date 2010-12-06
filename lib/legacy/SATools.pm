package SATools;

=head1 NAME

I<SATools.pm> - library of CONSSA processing functions 

=head1 SYNOPSIS

use I<SATools.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use File::Basename;
use ISDCPipeline;
use ISDCLIB;
use OPUSLIB;

sub SATools::ScwCheck;
sub SATools::ObsCheck;
sub SATools::ScwSetWait;
sub SATools::RefCatTest;	#	I don't think this is used anywhere

$| = 1;

############################################################################

=item B<ScwCheck> ( %att )

Check status of science window OSFs in SA pipeline;  called from csafin process. 

Default checks ScWs for "c" and sets Obs to "w", but for IBIS, different;

SATools::ScwCheck(
	"ogid" => "soj1_0000000001_000_JMX1",
	"scwid" => "000100010010",
	"proc" => "SA JMX Scw",
	"dcf" => "JX1",
	"scw_complete" => "o",
	"obs_wait" => "v",
	"ogid_current_state" => "s",
	);

=cut

sub ScwCheck {
	
	&Carp::croak ( "SATools::ScwCheck: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	my $bad = 0;
	my $good = 0;
	my $retval;
	my @result;
	my $osf;
	my $stat;
	$att{scw_complete} = "c" unless (defined($att{scw_complete}));
	$att{obs_wait} = "w" unless (defined($att{obs_wait}));
	#  For loop two IBIS, the OG current stat will be s (waiting for scws)
	$att{ogid_current_state} = "-" unless(defined($att{ogid_current_state}));
	
	#  Just in case (though this shouldn't happen) check that the Obs grup OSF
	#   is started properly:
	($retval,@result) = &ISDCPipeline::RunProgram("osf_test -p conssa.path -f $att{ogid} -n $att{dcf} -pr ST");
	chomp $result[0];
	if  ($result[0] !~ /c/) {
		print "*******     WARNING:  OSF $att{ogid} status $result[0] of ST not expected!  "
			."Should be c.  Quitting.\n";
		return;
	}
	
	#  Get status of all ScW OSFs of same instrument (DCF) and OG:
	print "*******     CHECKING all scws associated with OGID $att{ogid};  "
		."current scwid is $att{scwid}.\n";
	
	$att{dcf} = $ENV{OSF_DCF_NUM} unless(defined($att{dcf}));
	
	my %stats = &ISDCPipeline::BBUpdate(
		"dcf"    => "$att{dcf}",
		"match"  => "$att{ogid}",
		"type"   => "scw",
		"return" => 2,
		#  Used only here so far, to tell it to use osf_test instead of ls, which
		#   has been causing transient problems.  How slow will it be?
		"safe"   => "yes",
		);
	
	
	#  Count number not done in main step for this loop:
	foreach $osf (sort keys %stats) {
		$stat = $stats{$osf};
		$stat =~ /^\w(\w)\w/;
		$bad++ unless ($1 eq $att{scw_complete});
		$good++ if  ($1 eq $att{scw_complete});
	}
	
	die "*******     ERROR:  got nothing back from BBUpdate!  Something's wrong here...." 
		unless ($bad + $good);
	
	# if not, log it and quit:
	if ($bad) {
		print "******     There are $bad science windows of ogid $att{ogid} "
			."not completed (and $good complete, out of total "
			.($bad + $good)." returned by BBUpdate);  not ready to run obs step\n";
		return 0;
	}
	else {
		# if so, set obs OSF to "w"
		
		print "*******     All $good science windows for ogid $att{ogid} and inst "
			."$att{dcf} completed;  setting obs OSF to waiting.\n";
		
		#  Sometimes, it seems to reset OG despite an error in one!  How?!
		print "#######     DEBUG:  returned list is:\n";
		foreach $osf (sort keys %stats) {
			print "#######     DEBUG:  $osf $stats{$osf}\n";
		}
		
		#  Just in case (though this shouldn't happen) check that the OSF is 
		#   currently set to the expected value.
		($retval,@result) = &ISDCPipeline::RunProgram("osf_test -p conssa.path -f $att{ogid} -n $att{dcf} -pr SA");
		chomp $result[0];
		
		die "*******     ERROR:  OSF $att{ogid} status $result[0] of SA not expected!  "
			."Should be $att{ogid_current_state}\n" 
			unless ($result[0] =~ /$att{ogid_current_state}/);
		
		#  First, change all science windows with SA done but FI not to done:
		#   (Saves checking every time, not to mention error when the next
		#    one finds that the OG has already been reset!)
		print "*******     Resetting all science windows now to current status $att{scw_complete}:\n";
		
		&ISDCPipeline::BBUpdate(
			"match"   => "$att{ogid}",
			"type"    => "scw",
			"dcf"     => "$att{dcf}",
			"column"  => "FI",
			"curstat" => "w",
			"newstat" => "$att{scw_complete}",
			);
		
		#  Now update the one Obs Grp OSF:    
		($retval,@result) = &ISDCPipeline::RunProgram("osf_update -p conssa.path -f $att{ogid} -n $att{dcf} -c SA -s $att{obs_wait}");
		
		die "*******      ERROR:  cannot update status of OSF $att{ogid}_obs to SA=w:\n@result" 
			if ($retval);
		
	} # end if all do ne
	
	return;
	
} # end of ScwCheck


############################################################################

=item B<ObsCheck> ( %att )

Used in csafin to check the OSFs of other instruments for this OG to see if you can write protect the whole subdir.

Returns $working ( number of related OSFs still processing )

=cut

sub ObsCheck {
	my %att = @_;
	my $working = 0;
	print "*******     Checking other instruments of OGID $att{ogid}\n";
	
	my %stats = &ISDCPipeline::BBUpdate(
		"match"  => "$att{ogid}",
		"type"   => "obs",
		"return" => 2,
		);    
	foreach (sort keys %stats) {
		$working++ unless ( ($stats{$_} =~ /^$osf_stati{SA_COMPLETE}/) || (/^$ENV{OSF_DATASET}$/) );
	}
	print "*******     Returning $working OSFs still processing for OGID $att{ogid}\n";
	
	return $working;
	
}  # end ObsCheck


############################################################################

=item B<ScwSetWait> ( %att )

Used only after IBIS first Obs loop run;  sets science windows in OG from "o" state to "v" so their next loop triggers.

&SATOOLS::ScwSetWait(
	"ogid" => "$ogid",
	"dcf" => "$dcf",
	);

=cut

sub ScwSetWait {
	
	&Carp::croak ( "StdIBIS::ScwSetWait: Need even number of args" ) if ( @_ % 2 );
	
	my %att = @_;
	
	$att{dcf} = $ENV{OSF_DCF_NUM} unless(defined($att{dcf}));
	$att{ogid} = $ENV{OSF_DATASET} unless(defined($att{ogid}));
	
	&ISDCPipeline::BBUpdate(
		"match"     => "$att{ogid}",
		"type"      => "scw",
		"dcf"       => "$att{dcf}",
		"fullstat"  => "cvv",
		"matchstat" => "cgg", # do I want this?
		);
	
	return;
	
} # end of ScwSetWait


############################################################################

=item B<RefCatTest> ( %att )

Checks to see if $ISDC_REF_CAT actually exists.

Returns $ISDC_REF_CAT

=cut

sub RefCatTest {
	
	my %att = @_;
	#  ISDC_REF_CAT variable must be defined
	my $ref_cat;
	
	if (defined($ENV{ISDC_REF_CAT})) {
		$ref_cat = $ENV{ISDC_REF_CAT};
		#  remember, ISDC_REF_CAT is a DOL, so to test, must get rid of [extn]
		my ($catfile,$path,$extn) = &File::Basename::fileparse($ENV{ISDC_REF_CAT},'\..*');
		if (!-e "${path}/${catfile}.fits") {
			&Error ( "$att{proc} - Reference Catalog ISDC_REF_CAT does not exist!" );
		} # if ISDC_REF_CAT doesn't exist
	} # if ISDC_REF_CAT defined
	else {
		&Error ( "$att{proc} - Reference Catalog ISDC_REF_CAT not defined!" );
	} # if not defined
	
	return $ref_cat;
	
} # end RefCatTest

############################################################################

1;

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

