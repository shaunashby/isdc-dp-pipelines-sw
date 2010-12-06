package CorLIB;

use strict;
use File::Basename;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

=head1 NAME

I<CorLIB.pm> - library containing COR specific routines used by both the consssa and the nrtscw/conscw pipelines

=head1 SYNOPSIS

use I<CorLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

sub CorLIB::IBEP;			#	IBIS Events Pick
sub CorLIB::IIEP;			#	IBIS ISGRI Events Pick
sub CorLIB::IPEP;			#	IBIS PICSIT Events Pick
sub CorLIB::JXEP;			#	JEMX Events Pick
sub CorLIB::Fin;			#	Generic cor/fin step
sub CorLIB::IBISFIN;
sub CorLIB::JMXFIN;
sub CorLIB::AddOldIBISGTIs;
sub CorLIB::CopyGTIExtension;	#	dal_list, idx2dol, dal_copy
sub CorLIB::GTI_Merge;	#	gti_merge (060222-Created this to minimize and simplify code.  This is called from here and nswcor.pl.)

$| = 1;

######################################################################

=item B<CopyGTIExtension> ( $grpdol, $gti_struct, $gti_name, $targetfile )

Searches for actual dol of $grpdol[$gti_struct], then searches this dol for the dol of $gti_name.

=cut

sub CopyGTIExtension {			

	my $grpdol     = $_[0];	#	
	my $gti_struct = $_[1];	#	IBIS-GNRL-GTI, JMX1-GNRL-GTI, JMX2-GNRL-GTI, SPI.-GNRL-GTI, OMC.-GNRL-GTI, INTL-GNRL-GTI
	my $gti_name   = $_[2];	#	MERGED_ISGRI, MERGED_PICSIT, MERGED
	my $targetfile = $_[3];	#	picsit_events.fits, isgri_events.fits, compton_events.fits, jmx[12]_events.fits
									#	spi_oper.fits, spi_emer.fits, spi_calib.fits, spi_diag.fits

	&Message ("Running CopyGTIExtension ( $grpdol, $gti_struct, $gti_name, $targetfile )" );

	my @extensions;
	my $donotcountlist = "";
	if ( $targetfile =~ /picsit_events/ ) {
		@extensions = (  "PICS-??LE-RAW" );			
	} elsif ( $targetfile =~ /isgri_events/ ) {
		@extensions = (  "ISGR-EVTS-RAW", "ISGR-EVTS-ALL" );
	} elsif ( $targetfile =~ /jmx1_events/ ) {
		@extensions = (  "JMX1-*-RAW", "JMX1-FULL-ALL" );
		$donotcountlist = "JMX1-RATE-RAW";
	} elsif ( $targetfile =~ /jmx2_events/ ) {
		@extensions = (  "JMX2-*-RAW", "JMX2-FULL-ALL" );
		$donotcountlist = "JMX2-RATE-RAW";
	} elsif ( $targetfile =~ /spi_oper/ ) {
		@extensions = (  "SPI.-O???-RAW" );			
	} elsif ( $targetfile =~ /spi_emer/ ) {
		@extensions = (  "SPI.-E???-RAW" );			
	} elsif ( $targetfile =~ /spi_calib/ ) {
		@extensions = ( "SPI.-CCRV-RAW" );
	} elsif ( $targetfile =~ /spi_diag/ ) {
		@extensions = (  "SPI.-D???-RAW" );			
	} else {
		&Error ("-$targetfile- did not match any of the expected files!?");	
	}

	my $rows = 0;
	foreach ( @extensions ) {
		$rows = &ISDCLIB::RowsIn ( $grpdol, $_, $donotcountlist );
		last if ( $rows > 0 );		
	}
	
	if ( $rows <= 0 ) {
		print "No rows found in @extensions.\nNot copying GTI to $targetfile.\n";
		return;
	}

	my ($retval,@results);
	my $usenextline = "";
	my $gtifile = "";
	my $members = "";

	my $gtidol = &ISDCPipeline::FindDOL ( $grpdol, "$gti_struct-IDX" );

	($retval, @results) = &ISDCPipeline::PipelineStep(
		"step"          => "CorLIB - locating newly created gti_merge",
		"program_name"  => "idx2dol",
		"par_index"     => "$gtidol",
		"par_select"    => "GTI_NAME == '$gti_name'",
		"par_sort"      => "",
		"par_sortType"  => "1",
		"par_sortOrder" => "1",
		"par_numLog"    => "0",
		"par_outFormat" => "1",
		"par_txtFile"   => "",
		);
	&Error ("idx2dol failed. gtidol=$gtidol \nStatus = $retval") 
		if ( $retval );	

	print "Searching idx2dol output for structure location.\n";
	foreach (@results) {
		chomp;
		if ( $usenextline =~ /YES/ ) {
			( $gtidol ) = ( /Log_0\s+\:\s+(.*)\s*$/ );
			$usenextline = "";
			last;
		}
		if ( /^.*([\d+])\s+members.*$/ ) {
			$members     = $1;
			$usenextline = "YES";
		}
	} 
	$usenextline = "";
	&Error ("No $gti_name found!\n@results") unless ( $members > 0 );		
	&Error ("Multiple $gti_name s found!\n@results") if ( $members > 1 );	

	( $gtifile ) = ( $gtidol =~ /^(.*)\[.*$/ );
	&Error ("File does not exist: $gtidol") 
		unless ( ( -r $gtifile ) or ( -r "$gtifile.gz" ) );	
	&Error ("Merged events file $targetfile does not exist.") 
		unless ( ( -r $targetfile ) or ( -r "$targetfile.gz" ) );	

	&ISDCLIB::QuickDalCopy   ( "$gtidol", "$targetfile", "" );

	return;
}

####################################################################################################

=item B<JXEP> ( %att )

JEMX Events Pick is just a wrapper around evts_pick.

=cut

sub JXEP {
	my %att = @_;

	#  No need to figure out the scwid, I just need to do it for each one.
	my $scwdir = glob ( "scw/0*" );
	&ISDCPipeline::PipelineStep(
		"step"           => "JMX evts_pick for $scwdir",
		"program_name"   => "evts_pick",
		"par_swgDOL"     => "$scwdir/swg_jmx$att{jemxnum}.fits",
		"par_events"     => "$scwdir/jmx$att{jemxnum}_events.fits",
		"par_instrument" => "JMX$att{jemxnum}",
		"par_GTIname"    => "",
		"par_select"     => "",
		"par_evttype"    => "99",
		"par_attach"     => "no",
		"par_timeformat" => "0",
		"par_chatter"    => "3",
		);

	return;
}

####################################################################################################

=item B<IBEP> ( %att )

IBIS Events Pick just calls IPEP and/or IIEP depending on $att{INST}.

=cut

sub IBEP {
	my %att = @_;

	$att{evname} = "isgri_events"  if ( ( $att{disableIsgri}=~/n/i ) && ( $att{disablePICsIT}=~/y/i ) );
	$att{evname} = "picsit_events" if ( ( $att{disableIsgri}=~/y/i ) && ( $att{disablePICsIT}=~/n/i ) );
	$att{evname} = "ibis_events"   if ( ( $att{disableIsgri}=~/n/i ) && ( $att{disablePICsIT}=~/n/i ) );

	my $scwdir = glob ( "scw/0*" );

	&CorLIB::IIEP (
		"scwdir" => "$scwdir",
		"evname" => "$att{evname}",
		) if ( $att{INST} =~ /ISGRI/ );

	&CorLIB::IPEP (
		"scwdir" => "$scwdir",
		"evname" => "$att{evname}",
		) if ( $att{INST} =~ /PICSIT/ );

	return;
}

####################################################################################################

=item B<IIEP> ( %att )

IBIS ISGRI Events Pick is just a wrapper around evts_pick.

=cut

sub IIEP {
	my %att = @_;

	&ISDCPipeline::PipelineStep(
		"step"           => "IBIS IIEP evts_pick for $att{scwdir} and $att{evname}",
		"program_name"   => "evts_pick",
		"par_swgDOL"     => "$att{scwdir}/swg_ibis.fits",
		"par_events"     => "$att{scwdir}/$att{evname}.fits",
		"par_instrument" => "IBIS",
		"par_GTIname"    => "",
		"par_select"     => "",
		"par_evttype"    => "0",
		"par_attach"     => "no",
		"par_timeformat" => "0",
		"par_chatter"    => "3",
		);

	return;
}


####################################################################################################

=item B<IPEP> ( %att )

IBIS PICSIT Events Pick is just a wrapper around evts_pick.

=cut

sub IPEP {
	my %att = @_;

	foreach my $evttype ( "1", "2" ) {
		&ISDCPipeline::PipelineStep(
			"step"           => "IBIS IPEP evts_pick for $att{scwdir} and $att{evname}",
			"program_name"   => "evts_pick",
			"par_swgDOL"     => "$att{scwdir}/swg_ibis.fits",
			"par_events"     => "$att{scwdir}/$att{evname}.fits",
			"par_instrument" => "IBIS",
			"par_GTIname"    => "",
			"par_select"     => "",
			"par_evttype"    => "$evttype",
			"par_attach"     => "no",
			"par_timeformat" => "0",
			"par_chatter"    => "3",
			);
	}

	return;
}


####################################################################################################

=item B<Fin> ( %att )

Currently called only from consssa/cssfin.pl.

Copies, cleans, write-protects, gzip's and index management.

=cut

sub Fin {
	my %att = @_;

	my $revno     = $att{revno};
	my $scwid     = $att{scwid};
	my $proc      = $att{proc};
	my $OBSDIR    = $att{OBSDIR};
	my $OG_DATAID = $att{OG_DATAID};
	my $INST      = $att{INST};
	my $newscwdir = "$ENV{REP_BASE_PROD}/scratch/$revno/$scwid.000/";

	&ISDCPipeline::RunProgram( "$mymkdir -p $newscwdir" ) unless ( -d "$newscwdir" );

	#	This relative dir thing seems to work, but we'll see
	chdir ( "$newscwdir" ) or die "Could not chdir $newscwdir";
	my $scwdirbase = "/isdc/arc/rev_2/scw/$revno/$scwid";

	my $oldscwdir      = &ISDCLIB::FindDirVers( "$scwdirbase" );
	&Error ( "Version of $scwdirbase not found!?!?!" ) unless ( $oldscwdir );
	
	my $revdir      = &ISDCLIB::FindDirVers( "$ENV{REP_BASE_PROD}/scw/$revno/rev" );
	&ISDCPipeline::RunProgram( "$myln -s $revdir $ENV{REP_BASE_PROD}/scratch/$revno/rev.000" )
		unless ( -l "$ENV{REP_BASE_PROD}/scratch/$revno/rev.000" );

	unless ( -e "$newscwdir/swg.fits" ) {
		foreach my $file ( glob( "$oldscwdir/*" ) ) {
			if ( ( $file =~ /swg.fits/ )
				|| ( $file =~ /_scw.txt/ )
				) {
				&ISDCPipeline::RunProgram ( "$mycp -p $file $newscwdir/" );
			} else {
				&ISDCPipeline::RunProgram ( "$myln -s $file $newscwdir/" ) unless ( $ENV{CREATE_REV_3} );
				&ISDCPipeline::RunProgram ( "$mycp -p $file $newscwdir/" ) if     ( $ENV{CREATE_REV_3} );
			}
		}
	}

	&ISDCPipeline::RunProgram ( "$mychmod +w swg.fits" );

	chdir ( "$newscwdir/" ) or die "Could not chdir $newscwdir";

	my @result;
	my %lim_gti;
	foreach my $inst ("ibis","jmx1","jmx2") {
		my $struct = $inst."-GOOD-LIM";
		$struct =~ s/sc/intl/;
		$struct = uc($struct);
		$struct =~ s/(OMC|SPI)/$1\./;
		
		@result = &ISDCPipeline::GetICFile (
			"structure" => "$struct", 
			"filematch" => "swg.fits[1]",
			"error"     => 0,
			);
	
		$lim_gti{$inst} = $result[$#result] if (@result);
		&Error ( "Cannot find the IC structure $struct" ) unless (@result);
	}

	my $bti;
	@result = &ISDCPipeline::GetICFile (
		"structure" => "GNRL-INTL-BTI",
		"sort"      => "VSTART",
		"error"     => 0,
		"filematch" => "swg.fits[1]",
		);

	$bti = $result[$#result] if (@result);
	&Error ( "Cannot find the IC structure GNRL-INTL-BTI" ) unless (@result);

	my $obsscwdir = glob ( "$OBSDIR/scw/0*" );

	if ($INST =~ /ISGRI/) {
		&IBISFIN ( 
			"bti"       => $bti,
			"oldscwdir" => $oldscwdir,
			"obsscwdir" => $obsscwdir,
			"newscwdir" => $newscwdir,
			);
	}
	elsif ($INST =~ /JMX/) {
		&JMXFIN ( 
			"INST"      => $INST,
			"bti"       => $bti,
			"oldscwdir" => $oldscwdir,
			"obsscwdir" => $obsscwdir,
			"newscwdir" => $newscwdir,
			);
	}
	elsif ($INST =~ /PICSIT|OMC|SPI/) {
		&Message ( "We are currently not rerunning the correction step for $INST" );
	}
	else {
		&Error ( "Made it to the end of cssfin.pl::CorLIB::Fin and did not match +$INST+" );
	}

	chdir ( "$newscwdir/" ) or die "Could not chdir $newscwdir";

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - Extract groups",
		"program_name"  => "dal_grp_extract",
		"par_oDOL"      => "swg.fits[1]",
		"par_iDOL"      => "",
		"par_verbosity" => "3",         
		);

	&ISDCPipeline::RunProgram ( "$mycp -p $OBSDIR/logs/$OG_DATAID*.txt $newscwdir/" );

	my @allfiles = ();
	foreach ( glob ( "$newscwdir/*" ) ) {
		push @allfiles, $_ unless ( -l $_ );
	}
	&ISDCPipeline::PipelineStep ( 
		"step"         => "$proc - Write-protect everything",
		"program_name" => "$mychmod -w @allfiles" 
		);

	return;
}

###########################################################################

=item B<IBISFIN> ( %att )

IBIS specific fin step stuff.

=cut

sub IBISFIN { 
	my %att = @_;
	&Message ("Running IBISFIN" );
	my $bti       = $att{bti};
	my $oldscwdir = $att{oldscwdir};
	my $newscwdir = $att{newscwdir};
	my $obsscwdir = $att{obsscwdir};
	my @Children = ();
	my @fitslist = ( "ibis_gti.fits", "ibis_deadtime.fits" );

	if ( $ENV{CREATE_REV_3} ) {
		push @fitslist, "isgri_events.fits";
	} else {
		push @fitslist, "isgri_cor_events.fits";
	}

	&ISDCLIB::QuickClean     ( @fitslist );
	&ISDCLIB::QuickDalClean  ( "swg.fits" );
	&ISDCLIB::QuickDalDetach ( "swg.fits", "IBIS-GNRL-GTI*", "yes" );

	foreach my $file ( @fitslist ) {
		if ( $file =~ /isgri_events/ ) {
			#	we copy this file from the archive because the ISGR-EVTS-PRW and 
			#	ISGR-EVTS-SRW extensions should remain in the same order
			#	ibis_deadtime is quite similar, but because the first structure is new
			#	we start with the new file instead of the old one.
			&ISDCPipeline::RunProgram ( "$mycp -p $oldscwdir/$file.gz $newscwdir/" ) if ( -e "$oldscwdir/$file.gz" );
			&UnixLIB::Gunzip ( "$file.gz" );
			next;
		}
		&ISDCPipeline::RunProgram ( "$mycp -p $obsscwdir/$file $newscwdir/" ) if ( -e "$obsscwdir/$file" );
	}

	&ISDCPipeline::RunProgram ( "$mycp -p $obsscwdir/isgri_dead.fits $newscwdir/ibis_deadtime.fits" ) 
		if ( -e "$obsscwdir/isgri_dead.fits" );

	&CorLIB::AddOldIBISGTIs ( $oldscwdir );

	push @Children, "ibis_gti.fits[1]"        if ( &ISDCLIB::RowsIn ( "ibis_gti.fits",     "IBIS-GNRL-GTI" ) > 0 );

	#
	#	be careful to preserve the actual order of extensions in this file
	#	1 - "ISGR-EVTS-PRW"
	#	2 - "ISGR-EVTS-SRW"
	#	3 - "ISGR-EVTS-ALL"
	#	4 - "IBIS-GNRL-GTI"
	#	remove the original ISGR-EVTS-ALL and attach the new one
	#	remove the original IBIS-GNRL-GTI and attach the new one
	if ( ( -e "isgri_events.fits" ) && ( $ENV{CREATE_REV_3} ) ) {
		push @Children, "isgri_events.fits[ISGR-EVTS-PRW]"  if ( &ISDCLIB::RowsIn ( "isgri_events.fits[ISGR-EVTS-PRW]", "" ) > 0 );
		push @Children, "isgri_events.fits[ISGR-EVTS-SRW]"  if ( &ISDCLIB::RowsIn ( "isgri_events.fits[ISGR-EVTS-SRW]", "" ) > 0 );

		&ISDCPipeline::RunProgram ( "$mychmod +w isgri_events.fits" );
		&ISDCLIB::QuickDalDelete ( "$newscwdir/isgri_events.fits", ( "IBIS-GNRL-GTI", "ISGR-EVTS-ALL" ) );
		&ISDCLIB::QuickDalCopy   ( "$obsscwdir/swg_ibis.fits", "isgri_events.fits", "ISGR-EVTS-ALL" );
		push @Children, "isgri_events.fits[ISGR-EVTS-ALL]";
	}

	push @Children, "isgri_cor_events.fits[ISGR-EVTS-COR]"  if ( &ISDCLIB::RowsIn ( "isgri_cor_events.fits[ISGR-EVTS-COR]" ,"" ) > 0 );

	if ( -e "ibis_deadtime.fits" ) {
		&Message ( "ibis_deadtime.fits exists; copying other DEAD-SCP structures" );
		&ISDCLIB::QuickDalCopy   ( "$oldscwdir/swg.fits", "ibis_deadtime.fits", ( "PICS-DEAD-SCP", "COMP-DEAD-SCP" ) );
		push @Children, "ibis_deadtime.fits[PICS-DEAD-SCP]" if ( &ISDCLIB::RowsIn ( "ibis_deadtime.fits[PICS-DEAD-SCP]", "" ) > 0 );
		push @Children, "ibis_deadtime.fits[COMP-DEAD-SCP]" if ( &ISDCLIB::RowsIn ( "ibis_deadtime.fits[COMP-DEAD-SCP]", "" ) > 0 );
		push @Children, "ibis_deadtime.fits[ISGR-DEAD-SCP]" if ( &ISDCLIB::RowsIn ( "ibis_deadtime.fits[ISGR-DEAD-SCP]" ) > 0 );

		#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#
		#	because part of ibis_deadtime is new and part is old
		#	this WILL be a problem if we decide to rerun Picsit and Compton.
		#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#	#
	}

	&ISDCLIB::QuickDalAttach ( "swg.fits[1]", @Children );

	#	No need for gti_create as the structure already exists.
	#	MERGED_ISGRI is created by ibis_scw1_analysis
	&CorLIB::GTI_Merge ( "IBIS", "ISGRI_ISOC", $bti );

	&CorLIB::CopyGTIExtension ( "swg.fits[1]", "IBIS-GNRL-GTI", "MERGED_ISGRI", "isgri_events.fits" )
		if ( $ENV{CREATE_REV_3} );

	&UnixLIB::Gzip ( @fitslist );

	return;

}


###########################################################################

=item B<AddOldIBISGTIs> ( $oldscwdir )

Because there are fewer GTI's in new ibis_gti, we need to copy the missing ones from old ibis_gti.

GTI Names in new ibis_gti
VETO, OSM1, PICsIT, ISGRI_MCE[0-7], ATTITUDE, ISGRI_DATA_GAPS, MERGED_ISGRI, ISGRI_ISOC

GTI Names in original ibis_gti ( in addition to those above )
MERGED_PICSIT, PICSIT_ISOC, P_SGLE_DATA_GAPS, P_MULE_DATA_GAPS, C_SGLE_DATA_GAPS, C_MULE_DATA_GAPS

=cut

sub AddOldIBISGTIs {
	my ( $oldscwdir ) = @_;
	&Message ("Running AddOldIBISGTIs" );

	foreach my $gti_name ( "MERGED_PICSIT", "PICSIT_ISOC", "P_SGLE_DATA_GAPS", "P_MULE_DATA_GAPS", "C_SGLE_DATA_GAPS", "C_MULE_DATA_GAPS"  ) {
	
		my ($retval, @results) = &ISDCPipeline::PipelineStep(
			"step"          => "locating GTI_NAME $gti_name",
			"program_name"  => "idx2dol",
			"par_index"     => "$oldscwdir/ibis_gti.fits[1]",
			"par_select"    => "GTI_NAME == '$gti_name'",
			"par_sort"      => "",
			"par_sortType"  => "1",
			"par_sortOrder" => "1",
			"par_numLog"    => "0",
			"par_outFormat" => "1",
			"par_txtFile"   => "",
			);
		&Error ("idx2dol failed. Status = $retval") if ( $retval );

		my $gtidol;
		my $usenextline;
		print "Searching idx2dol output for structure location.\n";
		foreach (@results) {
			chomp;
			if ( $usenextline =~ /YES/ ) {
				( $gtidol ) = ( /Log_0\s+\:\s+(.*)\s*$/ );
				last;
			}
			$usenextline = "YES" if ( /members/ );
		}
		&Error ( "No match found in idx2dol output." ) unless ( $gtidol );

		#	copy even if it has 0 rows, or the idx_add to follow will fail!
		&ISDCLIB::QuickDalCopy   ( "$gtidol", "ibis_gti.fits", "" );	#	060113

		chomp ( my $count = `dal_list ibis_gti.fits[1] | grep TABLE  | wc -l` );
		$count += 2;
		&ISDCPipeline::PipelineStep(
			"step"          => "idx_add just added IBIS-GNRL-GTI",
			"program_name"  => "idx_add",
			"par_index"     => "ibis_gti.fits[1]",
			"par_template"  => "",
			"par_element"   => "ibis_gti.fits[$count]",
			"par_sort"      => "",
			"par_sortType"  => "1",
			"par_sortOrder" => "1",
			"par_security"  => "0",
			"par_update"    => "1",
			"par_stamp"     => "1"
			);
	}

	return;
}



###########################################################################

=item B<JMXFIN> ( %att )

JMX specific fin step stuff.

=cut

sub JMXFIN {
	my %att = @_;
	my $INST      = $att{INST};
	my $bti       = $att{bti};
	my $oldscwdir = $att{oldscwdir};
	my $newscwdir = $att{newscwdir};
	my $obsscwdir = $att{obsscwdir};
	
	my @Children = ();
	my ( $jnum ) = ( $INST =~ /JMX(\d)/ );
	my $other = 3 - $jnum;

	my @fitslist = ( "jmx${jnum}_gti.fits", "jmx${jnum}_deadtime.fits" );
	if ( $ENV{CREATE_REV_3} ) {
		push @fitslist, "jmx${jnum}_events.fits";
	} else {
		push @fitslist, "jmx${jnum}_full_cor.fits";
	}

	&ISDCLIB::QuickClean     ( @fitslist );
	&ISDCLIB::QuickDalClean  ( "swg.fits" );			#	shouldn't need cleaned unless a rerun

	&ISDCLIB::QuickDalDetach ( "swg.fits", "JMX$jnum-GNRL-GTI*", "yes" );

	foreach my $file ( @fitslist ) {
		if ( $file =~ /jmx._events/ ) {
			#	we copy this file from the archive because the ISGR-EVTS-PRW and 
			#	ISGR-EVTS-SRW extensions should remain in the same order
			&ISDCPipeline::RunProgram ( "$mycp -p $oldscwdir/$file.gz $newscwdir/" ) if ( -e "$oldscwdir/$file.gz" );
			&UnixLIB::Gunzip ( "$file.gz" );
			next;
		}
		&ISDCPipeline::RunProgram ( "$mycp -p $obsscwdir/$file $newscwdir/" ) if ( -e "$obsscwdir/$file" );
	}

	&ISDCPipeline::RunProgram ( "$mycp -p $obsscwdir/jmx${jnum}_dead_time.fits $newscwdir/jmx${jnum}_deadtime.fits" )
										  if ( -e "$obsscwdir/jmx${jnum}_dead_time.fits" );

	####################################################################################################
	#
	#	be careful to preserve the actual order of extensions in this file
	#	1 - "JMX?-FULL-PRW"
	#	2 - "JMX?-FULL-SRW"
	#	3 - "JMX?-FULL-ALL"
	#	4 - "JMX?-GNRL-GTI"
	#	remove the original JMX?-EVTS-ALL and attach the new one
	#	remove the original JMX?-GNRL-GTI and attach the new one
	if ( ( -e "jmx${jnum}_events.fits" ) && ( $ENV{CREATE_REV_3} ) ) {
		push @Children, "jmx${jnum}_events.fits[JMX$jnum-FULL-PRW]" if ( &ISDCLIB::RowsIn ( "jmx${jnum}_events.fits[JMX$jnum-FULL-PRW]", "" ) > 0 );
		push @Children, "jmx${jnum}_events.fits[JMX$jnum-FULL-SRW]" if ( &ISDCLIB::RowsIn ( "jmx${jnum}_events.fits[JMX$jnum-FULL-SRW]", "" ) > 0 );

		&ISDCPipeline::RunProgram ( "$mychmod +w jmx${jnum}_events.fits" );

		&ISDCLIB::QuickDalDelete ( "$newscwdir/jmx${jnum}_events.fits", ( "JMX$jnum-GNRL-GTI", "JMX$jnum-FULL-ALL" ) );
		&ISDCLIB::QuickDalCopy ( "$obsscwdir/swg_jmx$jnum.fits", "jmx${jnum}_events.fits", "JMX$jnum-FULL-ALL" );
		push @Children,"jmx${jnum}_events.fits[JMX$jnum-FULL-ALL]";
	}

	push @Children, "jmx${jnum}_full_cor.fits[JMX$jnum-FULL-COR]" if ( &ISDCLIB::RowsIn ( "jmx${jnum}_full_cor.fits[JMX$jnum-FULL-COR]", "" ) > 0 );
	push @Children, "jmx${jnum}_gti.fits[1]"                      if ( &ISDCLIB::RowsIn ( "jmx${jnum}_gti.fits[JMX$jnum-GNRL-GTI]", "" )      > 0 );
	push @Children, "jmx${jnum}_deadtime.fits[JMX$jnum-DEAD-SCP]" if ( &ISDCLIB::RowsIn ( "jmx${jnum}_deadtime.fits[JMX$jnum-DEAD-SCP]", "" ) > 0 );

	&ISDCLIB::QuickDalAttach ( "swg.fits[1]", @Children );

	#	No need for gti_create as the structure already exists.
	#	MERGED is created by jemx_scw_analysis
	&CorLIB::GTI_Merge ( "JMX$jnum", "ISOC", $bti );

	&CorLIB::CopyGTIExtension ( "swg.fits[1]", "JMX$jnum-GNRL-GTI", "MERGED", "jmx${jnum}_events.fits" )
		if ( ( $ENV{CREATE_REV_3} ) && ( -e "jmx${jnum}_events.fits" ) );

	&UnixLIB::Gzip ( @fitslist );

	return;
}


####################################################################################################

=item B<GTI_Merge> ( $INST, $name, $bti )

Created this to minimize the code. (SCREW 1819) It is called several times from 2 places with most of the same parameters.

=cut

sub GTI_Merge {
	my ( $INST, $name, $bti ) = @_;		
	#	JMX1, MERGED, some_dol[1] or IBIS, ISGRI_ISOC, some_dol[1]

	my %pars = (
		"step"                   => "$INST $name gti_merge",
		"program_name"           => "gti_merge",
		"par_InSWGroup"          => "",
		"par_OutSWGroup"         => "swg.fits[1]",
		"par_MergedName"         => "$name",
		"par_OutInstrument"      => "$INST",
		"par_SC_Names"           => "ATTITUDE",
		"par_SPI_Names"          => "",
		"par_IBIS_Names"         => "",
		"par_JEMX1_Names"        => "",				
		"par_JEMX2_Names"        => "",								
		"par_OMC_Names"          => "",
		"par_IREM_Names"         => "",
		"par_GTI_Index"          => "",
		"par_BTI_Dol"            => "$bti",
		"par_BTI_Names"          => "",
		);

	if ( ( $INST =~ /IBIS/ ) && ( $name =~ /PICSIT/ ) ) {
		$pars{"par_IBIS_Names"}     = "OSM1 P_SGLE_DATA_GAPS P_MULE_DATA_GAPS";
	} elsif ( $INST =~ /IBIS/ ) {
		$pars{"par_IBIS_Names"}     = "OSM1 ISGRI_DATA_GAPS";
	} elsif ( $INST =~ /JMX(\d)/ ) {
		$pars{"par_JEMX${1}_Names"} = "OSM1 DATA_GAPS ATTITUDE";
	} elsif ( $INST =~ /SPI/ ) {
		$pars{"par_SPI_Names"}      = "OSM1 DATA_GAPS";
	} elsif ( $INST =~ /OMC/ ) {
		$pars{"par_OMC_Names"}      = "OSM1";
	} elsif ( $INST =~ /SC/ ) {
		$pars{"par_SC_Names"}       = "ATTITUDE OSM1";
	}

	&ISDCPipeline::PipelineStep ( %pars );

	return;
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

