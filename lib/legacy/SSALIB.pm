package SSALIB;

=head1 NAME

I<SSALIB.pm> - consssa specific functions

=head1 SYNOPSIS

use I<SSALIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use ISDCPipeline;
use ISDCLIB;

sub SSALIB::ParseOSF;
sub SSALIB::ParseTrigger;
sub SSALIB::Trigger2OSF;		#	convert trigger format name to OSF format name
sub SSALIB::OSF2Trigger;		#	convert OSF format name to Trigger format name	- NOT YET
sub SSALIB::inst2instdir;

$| = 1;

#####################################################################################

=item B<OSF2Trigger> ( $OSF )

=cut

sub OSF2Trigger {
	my ( $OSF )  = @_;		#	sssp_002400100010 or smsp_0021_cyg_74_partial
	print "Converting OSF $OSF to trigger.\n" if ( $ENV{DEBUGIN} );

	my ( $in, $scwid ) = ( $OSF =~ /^s\w(\w{2})_(.+)$/ );	#	050413 - Jake - SPR 4083
	print ( "in:$in\n" )   if ( $ENV{DEBUGIN} );
	print ( "scwid:$scwid\n" ) if ( $ENV{DEBUGIN} );
	my $inst = &ISDCLIB::in2inst ( $in );

	&Error ( "Unrecognized instrument ->$inst<-" )
		unless ( $inst =~ /isgri|picsit|spi|omc|jmx1|jmx2/ );

	my $trigger = "${scwid}_${inst}";
	print ( "trigger:$trigger\n" ) if ( $ENV{DEBUGIN} );

	return $trigger;
}


#####################################################################################

=item B<Trigger2OSF> ( $triggerfile )

=cut

sub Trigger2OSF {
	my ( $triggerfile ) = @_;
	print "Converting trigger $triggerfile to OSF.\n" if ( $ENV{DEBUGIN} );
	#	$MYPATH/123456789012_isgri.trigger
	my ( $trigger, $path, $ext ) = &File::Basename::fileparse($triggerfile,'\..*');
	
	my ( $scwid, $revno, $inst ) = ( $trigger =~ /^((\d{4}).+)_([^_]{3,6})$/ );
#	my ( $revno ) = ( $trigger =~ /^(\d{4})/ );
	&Error ( "No revno found in trigger ->$trigger<-" ) unless ( $revno );
	print ( "revno:$revno\n" ) if ( $ENV{DEBUGIN} );

#	my ( $scwid ) = ( $trigger =~ /^(\d{4}.+)_[^_]{3,6}$/ );
	&Error ( "No scwid found in trigger ->$trigger<-" ) unless ( $scwid );
	print ( "scwid:$scwid\n" ) if ( $ENV{DEBUGIN} );

#	my ( $inst )  = ( $trigger =~ /^.+_([\w\d]{3,6})$/ );
	&Error ( "No instrument found in trigger ->$trigger<-" ) unless ( $inst );
	( my $INST = $inst ) =~ tr/a-z/A-Z/;
	print ( "INST:$INST\n" ) if ( $ENV{DEBUGIN} );

	&Error ( "Unrecognized instrument ->$inst<-" )
		unless ( $inst =~ /isgri|picsit|spi|omc|jmx1|jmx2/ );
	print ( "inst:$inst\n" ) if ( $ENV{DEBUGIN} );

	# The DCF is only three, and we need the JEMX number.  So the DCFs will be
	#   IBI, ISG, PIC, SPI, OMC, JX1, and JX2
	my $dcf = $INST;
	$dcf =~ s/JM/J/;
	$dcf =~ s/ISGRI/ISG/; 
	$dcf =~ s/PICSIT/PIC/; 
	print ( "dcf:$dcf\n" ) if ( $ENV{DEBUGIN} );
        
	my $in = &ISDCLIB::inst2in ( $inst );
	my $osfname = "ss${in}_${scwid}";
	$osfname =~ s/^ss/sm/ unless ( -z "$triggerfile" );
#	$osfname =~ s/^(s\w{3}_)9999_?/$1/;
	print ( "osfname:$osfname\n" ) if ( $ENV{DEBUGIN} );

	return ( $osfname, $dcf, $inst, $INST, $revno, $scwid );
}


#####################################################################################

=item B<ParseOSF> ( )

=cut

sub ParseOSF {
	my ( $in, $scwid, $revno ) = ( $ENV{OSF_DATASET} =~ /^s\w(\w{2})_((\d{4}).+)$/ );
	&Error ( "No instrument abbrev found in dataset ->$ENV{OSF_DATASET}<-" ) unless ( $in );
	&Error ( "No scwid found in dataset ->$ENV{OSF_DATASET}<-" ) unless ( $scwid );

	&Message ("OSF_DATASET:$ENV{OSF_DATASET}") if ( $ENV{DEBUGIN} );
	&Message ("scwid:$scwid") if ( $ENV{DEBUGIN} );

	&Error ( "No revno found in dataset ->$ENV{OSF_DATASET}<-" ) unless ( $revno );
	&Message ("revno:$revno") if ( $ENV{DEBUGIN} );

	#	I just want to remove the "0000" from smii_0000_Sgr_A_star or smii_0000Sgr_A_star
	#	and then have smii_Sgr_A_star and NOT smii__Sgr_A_star
	#	I think that the ? means 0 or 1.
	#	071203 - Jake - SCREW 2058 - Leave the 0000 there
	#( my $OG_DATAID = $ENV{OSF_DATASET} ) =~ s/^(s\w{3}_)0000_?/$1/;		#	Don't know if the _? is gonna work just right, but seems to at the mo
	my $OG_DATAID = $ENV{OSF_DATASET};

	&Message ("OG_DATAID:$OG_DATAID") if ( $ENV{DEBUGIN} );

	my $inst = &ISDCLIB::in2inst ( $in );
	&Message ("inst:$inst") if ( $ENV{DEBUGIN} );
	( my $INST = $inst ) =~ tr/a-z/A-Z/;
	&Message ("INST:$INST") if ( $ENV{DEBUGIN} );

	my $og = "og_$inst.fits";
	$og = "og_ibis.fits" if ( $inst =~ /isgri|picsit/ );
	&Message ("og:$og") if ( $ENV{DEBUGIN} );

	my $instdir = &inst2instdir ( $inst, $revno );
	&Message ("instdir:$instdir") if ( $ENV{DEBUGIN} );

	my $OBSDIR  = "$ENV{REP_BASE_PROD}/$instdir/$OG_DATAID";
	&Message ("OBSDIR:$OBSDIR") if ( $ENV{DEBUGIN} );

	return ( $scwid, $revno, $og, $inst, $INST, $instdir, $OG_DATAID, $OBSDIR );
}


#####################################################################################


=item B<ParseTrigger> ( $trigger, $inst, $idx2og )

read the trigger file
return filename of index to be given to og_create (idx2og)
return SPI pointings string to be passed to spi_science_analysis (spiPoint)

=cut

sub ParseTrigger {

	#	#_SPI_POINTINGS 1 2 3 4 6 7 8
	#	#_SKIP_isgri 002600180010
	#	#_SKIP_jmx 002600210010
	#	#_RRRR_VERSION_isgri 001
	#	#_RRRR_VERSION_jmx 002
	#	# This is a comment line that needs to be ignored
	#	002600170010
	#	002600180010
	#	002600190010
	#	002600200010
	#	002600210010
	#	002600220010
	#	002600230010
	#	002600240010 

	my ( $trigger, $inst, $idx2og ) = @_;
	&Message ("trigger:$trigger") if ( $ENV{DEBUGIN} );
	&Message ("inst:$inst")       if ( $ENV{DEBUGIN} );
	&Message ("idx2og:$idx2og")   if ( $ENV{DEBUGIN} );

	my $swg = "swg_$inst";
	$swg    =~ s/isgri/ibis/;
	$swg    =~ s/picsit/ibis/;
	my $instdir = &inst2instdir ( $inst );
	&Message ("instdir:$instdir") if ( $ENV{DEBUGIN} );
	my $in =  &ISDCLIB::inst2in ( $inst );
	&Message ("in:$in") if ( $ENV{DEBUGIN} );

	my $scwid;
	my $revno;
	my $skip;
	my $vvv;
	my $spiPoint;

	open  IDX2OG, ">> $idx2og"  or &Error ( "Can't open $idx2og" );
	open TRIGGER, "<  $trigger" or &Error ( "Can't open $trigger" );
	while ( <TRIGGER> ) {
		chomp;
		#	&Message ( "Read:$_:\n" );
		#next if ( /^# / );		#	not really necessary
		if ( /^\s*#_SPI_POINTINGS/ ) {
			( $spiPoint ) = ( /^\s*#_SPI_POINTINGS\s+([ \d]*)\s*$/ );
			&Message ( "Found SPI Pointings $spiPoint" );
			next;
		}
		if ( /^\s*#_SKIP_$inst/ ) {
			( $skip ) = ( /^\s*#_SKIP_$inst\s+([ \d]*)\s*$/ );
			&Message ( "Skip list $skip" );
			next;
		}
		if ( /^\s*#_RRRR_VERSION_$inst/ ) {
			( $vvv  ) = ( /^\s*#_RRRR_VERSION_$inst\s+(\d{3})\s*$/ );
			&Message ( "Using RRRR VERSION $vvv" );
			next;
		}
		unless ( /^\s*\d{12}\s*/ ) {
			&Message ( "+$_+ does not match anything" ) if ( $ENV{DEBUGIN} );
			next;
		}
		( $scwid ) = ( /^\s*(\d{12})\s*/ );
		( $revno ) = ( $scwid =~ /^(\d{4})/ );

		if ( $skip =~ /$scwid/ ) {
			&Message ( "Skipping $scwid by request" );
		} else {
			if ( $inst =~ /spi/ ) {
				print IDX2OG &ISDCLIB::FindDirVers("./scw/$revno/$scwid")."/swg.fits[1]\n";
			} else {
				( $vvv ) = ( &ISDCLIB::FindDirVers("$ENV{REP_BASE_PROD}/$instdir/$revno") =~ /\.(\d{3})$/ ) unless ( "$vvv" );
				print IDX2OG &ISDCLIB::FindDirVers("./$instdir/$revno.$vvv/ss${in}_$scwid/scw/$scwid")."/$swg.fits[1]\n";
			}
		}
	}
	close TRIGGER;
	close IDX2OG;

	return ( $spiPoint );

}


########################################################################

=item B<inst2instdir> ( $inst, $revno )

returns obs_$inst at minimum.

If $ENV{OSA_VERSION} is set, it is appended to obs_$inst, returning obs_$inst$ENV{OSA_VERSION}$ENV{OG_WRITE}.

If $revno is given, that too is appended with a preceding "/", returning obs_$inst$ENV{OSA_VERSION}$ENV{OG_WRITE}/$revno.

=cut

sub inst2instdir {
	my ( $inst, $revno ) = @_;
	my $instdir = "obs_$inst";
	$instdir    =~ s/obs_jmx\d/obs_jmx/;
	$instdir   .= "$ENV{OSA_VERSION}" if ( exists $ENV{OSA_VERSION} );
	$instdir   .= "$ENV{OG_WRITE}"    if ( exists $ENV{OG_WRITE} );
	$instdir   .= "/$revno.000" if ( defined $revno );
	return $instdir
}

1;

=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
