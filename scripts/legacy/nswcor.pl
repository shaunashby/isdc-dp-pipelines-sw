#!/usr/bin/perl

=head1 NAME

I<nswcor.pl> - NRT/CONS Science Window COR step script

=head1 SYNOPSIS

I<nswcor.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

use strict;
use warnings;

use File::Basename;
use ISDCPipeline;
use ISDCLIB;
use UnixLIB;
use CorLIB;

&ISDCPipeline::EnvStretch("OUTPATH","WORKDIR","SCWDIR","LOG_FILES","PARFILES","CFG_DIR","ALERTS","REV_WORK","REV_INPUT");

my $proc = &ISDCLIB::Initialize();
my $revno = &ISDCPipeline::RevNo( $ENV{OSF_DATASET} );
my $prevrev = sprintf "%04d", ( $revno - 1 );
my $grpdol = "swg.fits[GROUPING]";

chdir &ISDCPipeline::FindScw( "$ENV{OSF_DATASET}" );
&Message ( `pwd` );

########################################################################
#########             find required limit files
########################################################################

# find latest version of alert limits
my $missing;
my $struct;
my %lim_gti;
my @result;
my $gti_list;

foreach my $inst ("ibis","spi","omc","jmx1","jmx2", "sc") {
	$struct = $inst."-GOOD-LIM";
	$struct =~ s/sc/intl/;
	$struct = uc($struct);
	$struct =~ s/(OMC|SPI)/$1\./;
	
	@result = &ISDCPipeline::GetICFile(
		"structure" => "$struct", 
		"filematch" => "$grpdol",
		"error"     => 0,
		);

	if (@result) {
		$lim_gti{$inst} = $result[$#result];
		$gti_list .= "-----   ".$lim_gti{$inst}."\n";
	} else {
		print "*****     Missing GOOD limit for $inst\n";
		$missing .= "$struct";
	}
}

#  Get the BTIs:
my $bti;
@result = &ISDCPipeline::GetICFile(
	"structure" => "GNRL-INTL-BTI",
	"sort"      => "VSTART",
	"error"     => 0,
	"filematch" => "$grpdol",
	);

if (@result) {
	$bti = $result[$#result];
	$gti_list .= "-----   ".$bti."\n";
} else {
	print "*****     Missing BTI file.\n";
	$missing .= " GNRL-INTL-BTI";
}

&Error ( "Cannot find the following IC structures:\n$missing\n" ) if ($missing);

&Message ( "GTI LIMITS:\n$gti_list-----   " );


########################################################################
#########             generic first steps
########################################################################


&ISDCPipeline::PipelineStep(
	"step"             => "$proc - gti_attitude",
	"program_name"     => "gti_attitude",
	"par_InSWGroup"    => "",
	"par_OutSWGroup"   => "$grpdol",
	"par_AttStability" => "0.1",
	"par_Instrument"   => "SC",
	"par_AttStability_Z" => "0.4",
	);

&ISDCPipeline::PipelineStep(
	"step"                  => "$proc - gti_data_gaps",
	"program_name"          => "gti_data_gaps",
	"par_InSWGroup"         => "",
	"par_OutSWGroup"        => "$grpdol",
	"par_OverwriteGTI"      => "n",
	"par_Instrument"        => "",
	"par_SPI_Mode"          => "41 42 51 61",
	"par_ISGRI_Mode"        => "41 42 43",
	"par_PICSIT_SGLE_Mode"  => "41 42 43",
	"par_PICSIT_MULE_Mode"  => "41 42 43",
	"par_COMPTON_SGLE_Mode" => "41 42 43",
	"par_COMPTON_MULE_Mode" => "41 42 43",
	"par_JMX1_Mode"         => "41 42 43 44 45",
	"par_JMX2_Mode"         => "41 42 43 44 45",
	"par_FindAllIbisGaps"   => "yes",
	"par_RemoveWrongGTIs"   => "yes",
	);

########################################################################
#########             call each instrument subroutine
########################################################################

&scGTI();
&ibisAnalysis();
&jemxAnalysis("1");
&jemxAnalysis("2");
&omcGTI();
&spiAnalysis();
&DataMerge();

########################################################################
#########             generic last steps
########################################################################

#  Check for any indices I forgot to clean up in IBIS.
my @junk = glob("working*");
foreach (@junk) { unlink "$_"; }

&Message ( "done" );

exit 0;

########################################################################
#########             done with main
########################################################################

=item B<ibisAnalysis> ( )

=cut

sub ibisAnalysis {

	&Message ( "IBIS Analysis starting" );

	&ISDCPipeline::PipelineStep(
		"step"                  => "$proc - IBIS correction",
		"program_name"          => "ibis_correction",
		"par_swgDOL"            => "$grpdol",
		"par_disableIsgri"      => "NO",
		"par_disablePICsIT"     => "NO",
		"par_disableCompton"    => "NO",
		"par_osimData"          => "NO",
		"par_GENERAL_clobber"   => "YES",
		"par_GENERAL_levelList" => "PRP,COR,GTI,DEAD,BIN_I,CAT_I,BKG_I,IMA,IMA2,BIN_S,CAT_S,SPE,LCR,COMP",
		"par_IC_Group"          => "$ENV{REP_BASE_PROD}/idx/ic/ic_master_file.fits[1]",
		"par_IC_Alias"          => "$ENV{IC_ALIAS}",
		"par_ICOR_idxSwitch"    => "",
		"par_ICOR_faltStatus"   => "",
		"par_ICOR_GODOL"        => "",
		"par_ICOR_riseDOL"      => "",
		"par_PCOR_enerDOL"      => "",
		"par_outputExists"      => "YES",
		"par_ICOR_probShot"     => "0.01",
		"par_ICOR_protonDOL"    => "",
		"par_ICOR_supGDOL"      => "",
		"par_ICOR_supODOL"      => "",
		);

	&ISDCPipeline::PipelineStep(
		"step"                 => "$proc - IBIS gti",
		"program_name"         => "ibis_gti",
		"par_swgDOL"           => "$grpdol",
		"par_GTI_Index"        => "",
		"par_IC_Group"         => "$ENV{REP_BASE_PROD}/idx/ic/ic_master_file.fits[1]",
		"par_IC_Alias"         => "$ENV{IC_ALIAS}",
		"par_disableIsgri"     => "NO",
		"par_disablePICsIT"    => "NO",
		"par_disableCompton"   => "YES",
		"par_GTI_LimitTable"   => "",
		"par_GTI_gtiUserP"     => "",
		"par_GTI_gtiUserI"     => "",
		"par_GTI_TimeFormat"   => "OBT",
		"par_GTI_Accuracy"     => "any",
		"par_GTI_SCP"          => "",
		"par_GTI_SCI"          => "",
		"par_GTI_PICsIT"       => "VETO ATTITUDE P_SGLE_DATA_GAPS P_MULE_DATA_GAPS",
		"par_GTI_ISGRI"        => "VETO ATTITUDE ISGRI_DATA_GAPS",
		"par_GTI_BTI_Dol"      => "",
		"par_GTI_BTI_Names"    => "",
		"par_outputExists"     => "YES",			
		"par_GTI_attTolerance_X" => "0.05",
		"par_GTI_attTolerance_Z" => "0.2",
		);

	&CorLIB::CopyGTIExtension ( "$grpdol", "IBIS-GNRL-GTI", "MERGED_PICSIT", "picsit_events.fits" );

	&CorLIB::CopyGTIExtension ( "$grpdol", "IBIS-GNRL-GTI", "MERGED_ISGRI",  "isgri_events.fits" );

	&CorLIB::GTI_Merge ( "IBIS", "PICSIT_ISOC", $bti );
	&CorLIB::GTI_Merge ( "IBIS", "ISGRI_ISOC", $bti );

	&ISDCPipeline::PipelineStep(
		"step"                  => "$proc - IBIS dead",
		"program_name"          => "ibis_dead",
		"par_swgDOL"            => "$grpdol",
		"par_picsoutDead"       => "",		#	"ibis_deadtime.fits[PICS-DEAD-SCP,1,BINTABLE]",
		"par_isgroutDead"       => "",		#	"ibis_deadtime.fits[ISGR-DEAD-SCP,1,BINTABLE]",
		"par_compoutDead"       => "",		#	"ibis_deadtime.fits[COMP-DEAD-SCP,1,BINTABLE]",
		"par_outputExists"      => "YES",
		"par_disableIsgri"      => "NO",
		"par_disablePICsIT"     => "NO",
		"par_disableCompton"    => "NO",
		"par_osimData"          => "NO",
		"par_chatter"           => "2",
		"par_GENERAL_clobber"   => "YES",
		"par_GENERAL_levelList" => "PRP,COR,GTI,DEAD,BIN_I,CAT_I,BKG_I,IMA,IMA2,BIN_S,CAT_S,SPE,LCR,COMP",
		"par_IC_Group"          => "$ENV{REP_BASE_PROD}/idx/ic/ic_master_file.fits[1]",
		"par_IC_Alias"          => "$ENV{IC_ALIAS}",
		"par_veto_mod"          => "",
		);
}


########################################################################

=item B<jemxAnalysis> ( $num )

=cut

sub jemxAnalysis {												
	my $num   = $_[0];
	my $other = 3 - $num ;									
	my $instr = "JMX"."$num";

	&Message ( "$instr Analysis starting" );

	&ISDCPipeline::PipelineStep(
		"step"                   => "$proc - $instr correction",
		"program_name"           => "j_correction",
		"par_swgDOL"             => "$grpdol",
		"par_jemxNum"            => "$num",
		"par_IC_Group"           => "",
		"par_IC_Alias"           => "$ENV{IC_ALIAS}",
		"par_instMod"            => "",
		"par_chatter"            => "2",
		"par_clobber"            => "y",
		"par_osimData"           => "n",
		"par_gainHist"           => "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/idx/jemx".$num."_aca_frss_index.fits[GROUPING,1]",
		"par_gainModel"          => "2",
		"par_j_cor_both_fullRaw" => "",
		"par_j_cor_both_fullCor" => "",
		"par_j_cor_both_restRaw" => "",
		"par_j_cor_both_restCor" => "",
		"par_j_cor_gain_fullPrp" => "",
		"par_j_cor_gain_restPrp" => "",
		"par_j_cor_gain_sptiRaw" => "",
		"par_j_cor_gain_sptiPrp" => "",
		"par_j_cor_gain_sptiCor" => "",
		"par_j_cor_gain_specRaw" => "",
		"par_j_cor_gain_specPrp" => "",
		"par_j_cor_gain_specCor" => "",
		"par_outputExists"       => "YES",					
		"par_j_cor_gain_gainFac" => "",
		"par_subDir"             => "",
		"par_randPos" => "n"
		);

	&GTI_Create ( $instr, $lim_gti{lc($instr)}, $grpdol );

	&ISDCPipeline::PipelineStep(
		"step"             => "$proc - gti_attitude",
		"program_name"     => "gti_attitude",
		"par_InSWGroup"    => "",
		"par_OutSWGroup"   => "$grpdol",
		"par_AttStability" => "0.05",
		"par_Instrument"   => "$instr",
		"par_AttStability_Z" => "0.2",
	);

	foreach my $name ( "ISOC", "MERGED" ) {
		&CorLIB::GTI_Merge ( $instr, $name, $bti );
	}

	&CorLIB::CopyGTIExtension ( "$grpdol", "$instr"."-GNRL-GTI", "MERGED", lc($instr)."_events.fits" );

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - $instr dead",
		"program_name" => "j_dead",
		"par_swgDOL"   => "$grpdol",
		"par_jemxNum"  => "$num",
		"par_IC_Group" => "",
		"par_IC_Alias" => "$ENV{IC_ALIAS}",
		"par_instMod"  => "",
		"par_chatter"  => "2",
		"par_clobber"  => "y",
		"par_osimData" => "n",
		"par_j_dead_time_calc_csswHrw" => "",
		"par_j_dead_time_calc_csswCnv" => "",
		"par_outputExists" => "YES",					
		"par_subDir"   => "",
		);
}

########################################################################

=item B<spiAnalysis> ( )

=cut

sub spiAnalysis {

	my $coeffDOL;

	&Message ( "SPI starting" );
	
	if ( $ENV{PATH_FILE_NAME} =~ /nrt/ ) {
		my @iii_prep_dones = `$myls $ENV{REV_INPUT}/*iii_prep.trigger_done 2> /dev/null`;
		unless ( $#iii_prep_dones <= 0 ) {
			my $lastrev = $iii_prep_dones[$#iii_prep_dones];
			my ($root,$path,$suffix) = &File::Basename::fileparse($lastrev,'\..*');
			( $lastrev ) = ( $root =~ /^(\d{4})/ );
			if ( $lastrev ) {
				$coeffDOL = "$ENV{REP_BASE_PROD}/scw/$lastrev/rev.000/aca/spi_gain_coeff.fits";
			} else {
				&Message ( "Could not parse a revno from -$root-; Using GetICFile SPI.-COEF-CAL" );
				my @result = &ISDCPipeline::GetICFile(
					"structure" => "SPI.-COEF-CAL", 
					"filematch" => "$grpdol",
					);

				if ( @result ) {
					$coeffDOL = $result[$#result];
				} else {
					&Message ( "Could not get an IC File for SPI.-COEF-CAL based on $grpdol" );
				}
			}
		} else {
			&Message ( "There are no $ENV{REV_INPUT}/*iii_prep.trigger_done files" );
			my @result = &ISDCPipeline::GetICFile(
				"structure" => "SPI.-COEF-CAL", 
				"filematch" => "$grpdol",
				);

			if ( @result ) {
				$coeffDOL = $result[$#result];
			} else {
				&Message ( "Could not get an IC File for SPI.-COEF-CAL based on $grpdol" );
			}
		}
	} else {		#	cons
		$coeffDOL = "$ENV{REP_BASE_PROD}/scw/$revno/rev.000/aca/spi_gain_coeff.fits";
	}

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - SPI spi_gain_corr",
		"program_name"  => "spi_gain_corr",
		"par_prpOG"     => "$grpdol",
		"par_coeffDOL"  => "$coeffDOL",
		"par_outfile"   => "",
		"par_randomise" => "yes",
		"par_clobber"   => "yes",
		"par_overwriteALL" => "no"
		);

	&GTI_Create ( "SPI", $lim_gti{spi}, $grpdol );

	foreach my $name ( "ISOC", "MERGED" ) {
		&CorLIB::GTI_Merge ( "SPI", $name, $bti );
	}

	foreach ( "spi_oper.fits", "spi_calib.fits", "spi_emer.fits", "spi_diag.fits" ) { 
		&CorLIB::CopyGTIExtension ( "$grpdol", "SPI.-GNRL-GTI", "MERGED", "$_" );
	}
}

########################################################################

=item B<DataMerge> ( )

=cut

sub DataMerge {											
	my $instr = "";

	#--------------------------------------------------
	#	SPR 3853 - NEVER, EVER change this name
	#	The name swg_tmp is hard coded in DAL3GEN
	#
			my $tmpDOL = "swg_tmp.fits[1]";
	#
	#--------------------------------------------------

	#
	#Error_3 2004-06-11T09:27:29 evts_pick 3.1.1: Cannot select SPI events of type SPI.-OSGL-ALL! status=-2504
	#Error_2 2004-06-11T09:27:29 evts_pick 3.1.1: Error code -2504 from processing data!
	#
	foreach $instr ( "SPI", "JMX1", "JMX2", "IBIS" ) {		
		&ISDCPipeline::PipelineStep(
			"step"           => "$proc - $instr evts_pick",
			"program_name"   => "evts_pick",
			"par_swgDOL"     => "$grpdol",
			"par_events"     => "",
			"par_instrument" => "$instr",
			"par_GTIname"    => "",
			"par_select"     => "",
			"par_evttype"    => "99",
			"par_attach"     => "no",
			"par_timeformat" => "0",
			"par_chatter"    => "2",
			);
	}

	#	040604 - MB's notes
	#
	# need first to detach children then the group.
	# recursive appears not to work in this specific case
	#
	# swg_create has a BUG and does not remove temporary groups from *_tmp.fits
	# -> rm -f *_tmp.fits to patch.
	#
	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - Detaching * from $tmpDOL and deleting.",
		"program_name"  => "dal_detach",
		"par_object"    => "$tmpDOL",
		"par_child"     => "",
		"par_pattern"   => "*",
		"par_delete"    => "yes",
		"par_recursive" => "yes",
		"par_showonly"  => "no",
		"par_reverse"   => "no",
		);

	&ISDCPipeline::PipelineStep(
		"step"          => "$proc - Detaching $tmpDOL from $grpdol and deleting.",
		"program_name"  => "dal_detach",
		"par_object"    => "$grpdol",
		"par_child"     => "$tmpDOL",
		"par_pattern"   => "",
		"par_delete"    => "yes",
		"par_recursive" => "yes",
		"par_showonly"  => "no",
		"par_reverse"   => "no",
		);

	&ISDCPipeline::PipelineStep(
		"step"         => "$proc - rm the tmp files",
		"program_name" => "$myrm -f *_tmp.fits",
		);

}

########################################################################

=item B<omcGTI> ( )

=cut

sub omcGTI {		#########            OMC Subroutine
	
	&Message ( "OMC starting" );
	
	&GTI_Create ( "OMC", $lim_gti{omc}, $grpdol );
	
	&CorLIB::GTI_Merge ( "OMC", "ISOC", $bti );
}

########################################################################

=item B<scGTI> ( )

=cut

sub scGTI {		#########            Spacecraft Subroutine

	&Message ( "SC starting" );
	
	&GTI_Create ( "SC", $lim_gti{sc}, $grpdol );
	
	&CorLIB::GTI_Merge ( "SC", "SC", $bti );
}

########################################################################

=item B<GTI_Create> ( $INST, $lim_gti, $grpdol )

created this to minimize the code.
It is called about 4 times with most of the same pars.

=cut

sub GTI_Create {
	my ( $INST, $lim_gti, $grpdol ) = @_;

	&ISDCPipeline::PipelineStep(
		"step"           => "$proc - $INST gti_create",
		"program_name"   => "gti_create",
		"par_InSWGroup"  => "",
		"par_OutSWGroup" => "$grpdol",
		"par_Data"       => "",
		"par_ModeTable"  => "",
		"par_LimitTable" => "$lim_gti",
		"par_GTI_Index"  => "",
		"par_Force"      => "no"
		);
}
	
=back

=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
