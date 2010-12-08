package QLAMOS;

=head1 NAME

I<QLAMOS.pm> - nrtqla mosaic pipeline library

=head1 SYNOPSIS

use I<QLAMOS.pm>;

=head1 DESCRIPTION

This library produced in response to SCREW 1983.

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use ISDCPipeline;
use OPUSLIB qw(:osf_stati);
use UnixLIB;
use ISDCLIB;

$| = 1;

######################################################################

sub Mosaic {
	my ( $rev ) = @_;

	chomp ($rev);
	print "Processing revolution $rev\n";

	my %obs;		#	hash containing OBS_ID keys containing an array of POINTING_IDs

	my ( $expREF, $podvsREF, $pdefv ) = &ParsePDef ( $rev );
	my %exp = %{$expREF};

	my ( $obsREF ) = &ParsePOD ( $rev, $podvsREF );
	my %obs_e = %{$obsREF}; #	hash containing OBS_ID keys containing a hash of EXP_ID keys

	#	Populate %obs observations with POINTING_IDs.  BEWARE!  There is a potential bug as SWIDs are NOT determined yet
	foreach my $observation ( sort keys ( %obs_e ) ) {
		foreach my $exposure ( sort keys ( %{$obs_e{$observation}} ) ) {
			push @{$obs{$observation}}, @{$exp{$exposure}} if ( $exp{$exposure} );
		}
	}

	&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{MOSAICS}" ) unless ( -d "$ENV{MOSAICS}" );
	foreach my $observation ( sort keys ( %obs ) ) {
#		&QLAMOS::StartObservation ( $rev, $observation, $pdefv );

		my $obs_idx = "$ENV{MOSAICS}/${rev}_${observation}_${pdefv}.txt"; print "$obs_idx \n";
		unless ( -e "$obs_idx" ) {
			my $next_pointing = sprintf ( "%08d", (sort @{$obs{$observation}})[$#{$obs{$observation}}]+1 );
			my $next_rev      = sprintf ( "%04d", $rev+1 );
			my @next_scws = glob ( "$ENV{REP_BASE_PROD}/scw/$rev/$next_pointing\*/swg.fits" );
			my @next_rev  = glob ( "$ENV{REP_BASE_PROD}/scw/$next_rev/0\*/swg.fits" ) unless ( @next_scws );
			if ( @next_scws || @next_rev ) {
				&QLAMOS::WriteSCWList ( $obs_idx, $rev, $obs{$observation} ) ;
			} else {
				print "Must wait for pointing: $next_pointing or rev: $next_rev before writing scw list.\n";
			}
		}

		if ( -e "$obs_idx" ) {
			foreach my $inst ( qw/ibis jmx1 jmx2/ ) {
				my $in = &ISDCLIB::inst2in ( $inst );
				$ENV{OSF_DATASET} = "qm${in}_${rev}_${observation}_${pdefv}";
				&QLAMOS::StartOSF ( $inst, $obs_idx );
			}	#	end foreach instrument
		}	#	end if
	}	#	end each observation
}

######################################################################

sub WriteSCWList {
	my ( $obs_idx, $rev, $observations ) = @_;

	open SCW_LIST, "> $obs_idx";
	foreach my $pointing ( sort @{$observations} ) {
		my @scws = glob ( "$ENV{REP_BASE_PROD}/scw/$rev/$pointing\*0.???/swg.fits" );
		foreach my $scw ( @scws ) {
			$scw =~ s/$ENV{REP_BASE_PROD}\/+scw\/+$rev\/+//;
			$scw =~ s/\.\d{3}\/+swg.fits//;
			print "$scw\n";
			print SCW_LIST "$scw\n";
		}
	}	#	end foreach pointing
	close SCW_LIST;
	unlink $obs_idx if ( -z $obs_idx );
}	#	end if

######################################################################

sub StartOSF {
	my ( $inst, $obs_idx ) = @_;

	if ( `$myls $ENV{OPUS_WORK}/nrtqla/obs/*.$ENV{OSF_DATASET}* 2> /dev/null` ) {
		print "OSF for $ENV{OSF_DATASET} exists\n";
	} else {
		print "OSF for $ENV{OSF_DATASET} does not exist.\n";

		my $LOGDIR = "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs";
		&ISDCLIB::DoOrDie ( "$mymkdir -p $LOGDIR" ) unless ( -d "$LOGDIR" );
		&Error ( "Did not mkdir $LOGDIR" ) unless ( -d "$LOGDIR" );

		#   IBI, SPI, OMC, JX1, and JX2
		my $dcf = $inst;
		$dcf =~ tr/a-z/A-Z/; 
		$dcf =~ s/JM/J/; 

		if ( &ScwsComplete( $inst, $obs_idx) == 0 ) {
			# Startup OSF for observation, with status cww 
			my $retval = &ISDCPipeline::PipelineStart (
				"dataset"     => "$ENV{OSF_DATASET}", 
				"state"       => "$osf_stati{QLA_ST_C}",  
				"type"        => "obs", 
				"dcf"         => "$dcf", 
				"logfile"     => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log", 
				"reallogfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}_qla.txt", 
				);
			&Error ( "cannot start pipeline for $ENV{OSF_DATASET}" ) if ( $retval );
		} else {
			print "All science windows in $obs_idx are not complete.\n";
		}
	}	#	end unless
}

######################################################################

sub ScwsComplete {
	my ( $inst, $obs_idx ) = @_;

	my $osf;
	my $in = &ISDCLIB::inst2in ( $inst );
	open SCW_LIST, "< $obs_idx";
	while ( <SCW_LIST> ) {
		chomp;
		my $osfname = "$ENV{OPUS_WORK}/nrtqla/obs/*qs${in}_${_}*scw*";
		$osf = `$myls $osfname`;
		if ( $? ) {
			print "*******     WARNING:  cannot \'$myls $osfname\':  not found!\n";
			return 1;
		}
		chomp $osf;
		$osf = &File::Basename::basename($osf);
		my ($hextime,$osfstatus,$dataset,$type,$dcfnum,$command) = &OPUSLIB::ParseOSF ($osf);
		if ( $osfstatus !~ /$osf_stati{QLA_COMPLETE}/ ) {
			print "*******     WARNING:  $_ not complete\n";
			print "*******     $osf\n";
			return 2;
		}
	}
	close SCW_LIST;
	return 0;
}

######################################################################

sub ParsePDef {
	my ( $rev ) = @_;

	my %podvs;	#	hash simply containing potentially multiple pod versions
	my %exp;		#	hash containing EXP_ID keys containing an array of POINTING_IDs
	my $good_pdef;	#	flag to count good lines in pdef file

	my @pdefs = sort(glob(&ISDCLIB::FindDirVers ( "$ENV{REP_BASE_PROD}/aux/adp/$rev" )."/pointing_definition_predicted_*.fits*"));
	print "Found pdef files \n", join ( "\n", @pdefs) , "\n";
	my ( $pdefv ) = ( $pdefs[$#pdefs] =~ /pointing_definition_predicted_(.+)\.fits/ );

	my @pdef_list = &ISDCLIB::DoOrDie ( "fdump fldsep=, outfile=STDOUT columns='POINTING_ID,POINTING_TYPE,EXPID,PODV' rows=- prhead=no pagewidth=256 page=no wrap=yes showrow=no showcol=no showunit=no infile=$pdefs[$#pdefs]" );

	print "POINTING_ID,POINTING_TYPE,EXPID,PODV\n";
	foreach my $line ( @pdef_list ) {
		next if ( $line =~ /^\s*$/ );
		my ( $pointing_id, $pointing_type, $expid, $podver ) = split /\s*,\s*/, $line;
		next unless ( $pointing_type == 0 );
		next unless ( $pointing_id && $expid && $podver );
		$good_pdef++;
		$podvs{$podver}++;
		push @{$exp{$expid}}, $pointing_id;
	}
	&Error ( "No good lines found in $pdefs[$#pdefs]!" ) unless ( $good_pdef );
#	$podvs{'0022'}++;		#	for debugging

	return ( \%exp, \%podvs, $pdefv );
}

######################################################################

sub ParsePOD {
	my ( $rev, $podREF ) = @_;
	my %podvs = %{$podREF};

	my %obs_e; #	hash containing OBS_ID keys containing a hash of EXP_ID keys

	my $good_pod;	#	flag to count good lines in pod file
	&Error ( "Multiple PODV values (", join ( ",", sort keys ( %podvs ) ), ") found in PDEF file!!" ) if ( scalar keys %podvs > 1 );
	my @pods  = sort(glob(&ISDCLIB::FindDirVers("$ENV{REP_BASE_PROD}/aux/adp/$rev")."/pod_${rev}_".(keys %podvs)[0].".fits*"));
	print "Found pod files \n", join ( "\n", @pods) , "\n";

	my @pod_list = &ISDCLIB::DoOrDie ( "fdump fldsep=, outfile=STDOUT columns='EXP_ID,OBS_ID' rows=- prhead=no pagewidth=256 page=no wrap=yes showrow=no showcol=no showunit=no infile=$pods[$#pods]" );	#	AUXL-EXPO-REF	#	Why EXP_ID and not EXPID?

	print "EXP_ID,OBS_ID\n";
	foreach my $line ( @pod_list ) {
		next if ( $line =~ /^\s*$/ );
		$good_pod++;
		print $line;
		my ( $exp_id, $obs_id ) = split /\s*,\s*/, $line;
		$obs_e{$obs_id}{$exp_id}++;
	}
	&Error ( "No good lines found in $pods[$#pods]!" ) unless ( $good_pod );

	return ( \%obs_e );
}

1;
