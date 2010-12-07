package Datasets;

use strict;
use warnings;

use ISDCPipeline;
use ISDCLIB;
use UnixLIB;

=head1 NAME

I<Datasets.pm> - library used the the rev pipelines primarily for its hash tables

=head1 SYNOPSIS

use I<Datasets.pm>;

=head1 DESCRIPTION

=item

=head1 HASHES

=over

=cut

$| = 1;

=item B<%Datasets::Types>

Correpsondence of mnemonics and raw dataset root names: 
IF YOU ADD A TYPE, don't forget to update opus_wrapper.

=cut

%Datasets::Types = (
	# Process:
	# IREM:
	"ire" => "irem_raw",                      # ire
	
	# IBIS:
	"irn" => "isgri_raw_noise",               # irn
	"irv" => "ibis_raw_veto",                 # irv
	"irc" => "isgri_raw_cal",                 # irc
	"itv" => "ibis_raw_tver",                 # gen
	"idp" => "ibis_raw_dump",                 # idp
	"ilt" => "ibis_ilt",                      # idp
	#  Note: prc's file-by-file DP in gen, ACA in arc in gen
	"prc" => "picsit_raw_cal",                # gen

	# JEMX1:
	"jm1" => "jemx1_raw_frss",                # jmf
	"j1d" => "jemx1_raw_dump",                # gen
	"j1f" => "jemx1_raw_dfeedump",            # gen
	"j1t" => "jemx1_raw_tver",                # gen
	"j1e" => "jemx1_raw_ecal",                # jm3

	# JEMX2
	"jm2" => "jemx2_raw_frss",                # jmv 
	"j2d" => "jemx2_raw_dump",                # gen
	"j2f" => "jemx2_raw_dfeedump",            # gen
	"j2t" => "jemx2_raw_tver",                # gen
	"j2e" => "jemx2_raw_ecal",                # jme

	# OMC:
	"obc" => "omc_raw_bias",                  # gen
	"odc" => "omc_raw_dark",                  # gen
	"ofc" => "omc_raw_flatfield",             # gen
	"osc" => "omc_raw_sky",                   # gen
	"omd" => "omc_raw_dump",                  # gen
	"omt" => "omc_raw_tver",                  # gen
	
	#  SPI
	"stv" => "spi_raw_tver",                  # gen
	"sdp" => "spi_raw_dump",                  # gen
	"sas" => "spi_raw_asdump",                # gen
	"sdf" => "spi_raw_dfdump",                # gen
	"spd" => "spi_raw_pddump",                # gen
	"sac" => "spi_acs_cal",                   # gen
	"spa" => "spi_psd_adcgain",               # spi
	"spp" => "spi_psd_performance",           # spi
	"spe" => "spi_psd_efficiency",            # spi
	"sps" => "spi_psd_si",                    # spi
	
	#  Spacecraft:
	"sct" => "sc_raw_tref",                   # gen
	
	);

######################################################################

=item B<%Datasets::IndicesRev>

Yeah, this looks tricky, but it's really very simple...  ;)

This is used to determine what indices there are.  It will access these as in:
$Datasets::Indices{$type}{"prp/isgri_prp_nois"}

Also used in cleanopus to re-run things if necessary

Everything must be indexed under idx, but some things are under other groups, etc., so it's not a simple matter of using everything in Products.  Almost,though.  

=cut

%Datasets::IndicesRev = (
	
	#  IREM
	#  All IREM products are under this group except LCTR - ( 040322 - Jake - so why is it commented out?? )
	"ire" => { 
		"prp/irem_prp" => "IREM-CHNK-GRP",
		# "cfg/irem_lctr" => "IREM-LCTR-HIS",
	},


	#  IBIS
	"irn" => {												#  No groups here, so all individually:
		"prp/isgri_prp_noise"=>"ISGR-NOIS-CPR",
		"raw/isgri_raw_noise"=>"ISGR-NOIS-CRW",
		"cfg/isgri_pxlswtch"=>"ISGR-SWIT-STA",
	},
	"irv" => {												#  Again, no group, so individually:
		"raw/ibis_raw_veto" => "IBIS-VETO-CRW",
		"aca/ibis_aca_veto" => "IBIS-VETO-CAL",
		"aca/ibis_aca_cu" => "IBIS-UNIT-CAL",
	},
	"irc" => {												#  CTPR group has raw children (ISGR-CDTE-{CRW,PRW}):
		"prp/isgri_prp_cal"=>"ISGR-CTPR-GRP",
	},
	"itv" => { "raw/ibis_raw_tver" => "IBIS-TVER-CRW", },
	"idp" => {
		"raw/ibis_raw_dump"=>"IBIS-DUMP-CRW",
		"cfg/isgri_context"=>"ISGR-CTXT-GRP",
		"cfg/picsit_context"=>"PICS-CTXT-GRP",
		"cfg/hepi_context"=>"PICS-HEPI-GRP",
		"cfg/veto_context"=>"IBIS-VCTX-GRP",
		"cfg/iasw_context"=>"ISGR-LUT.-GRP",
		"cfg/picsit_fault_list"=>"PICS-FALT-STA",
	},
	"ilt" => { 
		"cfg/isgri_context_new" => "ISGR-CTXT-GRP", 
		"cfg/isgri_context_dead" => "ISGR-DEAD-CFG",		#	040323 - Jake - SCREW 1344
	},
	"prc" => { "raw/picsit_raw_cal"=>"PICS-CSI.-GRP" },


	#  JEMX1
	"jm1" => { 
		"raw/jemx1_raw_frss" => "JMX1-FRSS-CRW",
		"aca/jemx1_aca_frss" => "JMX1-GAIN-CAL",
	},
	"j1d" => { "raw/jemx1_raw_dump" => "JMX1-DUMP-CRW", },
	"j1f" => { "raw/jemx1_raw_dfeedump" => "JMX1-DFEE-CRW", },
	"j1t" => { "raw/jemx1_raw_tver" => "JMX1-TVER-CRW", },
	"j1e" => { "prp/jemx1_prp_ecal"=>"JMX1-ECAL-GRP" }, #  Raw attached to PRP grp:

	
	#  JEMX2
	"jm2" => { 
		"raw/jemx2_raw_frss" => "JMX2-FRSS-CRW",
		"aca/jemx2_aca_frss" => "JMX2-GAIN-CAL",
	},
	"j2d" => { "raw/jemx2_raw_dump" => "JMX2-DUMP-CRW", },
	"j2f" => { "raw/jemx2_raw_dfeedump" => "JMX2-DFEE-CRW", },
	"j2t" => { "raw/jemx2_raw_tver" => "JMX2-TVER-CRW", },
	"j2e" => { "prp/jemx2_prp_ecal"=>"JMX2-ECAL-GRP" },

	
	#  OMC
	"obc" => { "raw/omc_raw_bias" => "OMC.-BIAS-GRP", },
	"odc" => { "raw/omc_raw_dark" => "OMC.-DARK-GRP", },
	"ofc" => { "raw/omc_raw_flatfield" => "OMC.-LEDF-GRP", },
	"osc" => { "raw/omc_raw_sky" => "OMC.-SKYF-GRP", },
	"omd" => { "raw/omc_raw_dump" => "OMC.-DUMP-CRW", },
	"omt" => { "raw/omc_raw_tver" => "OMC.-TVER-CRW", },
	

	#  SPI
	"stv" => { "raw/spi_raw_tver" => "SPI.-TVER-CRW", },
	"sdp" => { "raw/spi_raw_dump" => "SPI.-DUMP-CRW", },
	"sas" => { "raw/spi_raw_asdump"=>"SPI.-ASMD-HRW", },
	"sdf" => { "raw/spi_raw_dfdump"=>"SPI.-DFMD-HRW", },
	"spd" => { "raw/spi_raw_pddump"=>"SPI.-PDMD-HRW", },
	"sac" => { "raw/spi_acs_cal" => "SPI.-ACS.-CRW", },

	
	#  Spacecraft:
	"sct" => { "raw/sc_raw_tref"=>"INTL-TREF-CRW",} ,


	#  ARC
	"arc" => {
		"osm/exposure_report" => "REPT-EXPO-CST",
		"osm/spi_psd_adcgain"=>"SPI.-ADC.-PSD",
		"osm/spi_psd_performance"=>"SPI.-PERF-PSD",
		"osm/spi_psd_efficiency"=>"SPI.-EFFI-PSD",
		"osm/spi_psd_si"=>"SPI.-SIGN-PSD",
		"cfg/picsit_fault_list" => "PICS-FALT-STA",
		"osm/hk_averages" => "GNRL-AVRG-GRP",
	},
	
	);

######################################################################

=item %Datasets::IndicesGlobal

...whereas these are all under idx/rev/:
SCREW 702:  whenever you add something here, add it to IndicesRev as well!

=cut

%Datasets::IndicesGlobal = (
	
	"irc" => {					#	040323 - Jake - required addition for SCREW 791
		"prp/isgri_prp_cal"=>"ISGR-CTPR-GRP",
	},

	"ilt" => {					#	this type is new for IndicesGlobal for SCREW 1344
		"cfg/isgri_context_dead" => "ISGR-DEAD-CFG",		#	040323 - Jake - SCREW 1344
	},
	
	"idp" => {
		"cfg/isgri_context"=>"ISGR-CTXT-GRP",
		"cfg/picsit_context"=>"PICS-CTXT-GRP",
		"cfg/hepi_context"=>"PICS-HEPI-GRP",
		"cfg/veto_context"=>"IBIS-VCTX-GRP",
		"cfg/iasw_context"=>"ISGR-LUT.-GRP",
		"cfg/picsit_fault_list.fits"=>"PICS-FALT-STA",
	},

	"irn" => {
		"prp/isgri_prp_noise"=>"ISGR-NOIS-CPR",
		"raw/isgri_raw_noise"=>"ISGR-NOIS-CRW",
		"cfg/isgri_pxlswtch"=>"ISGR-SWIT-STA",
	},
	
	#	iii (from iii_prep which is just SPI calibration) 	#	040525 - Jake - SCREW 1347
	"iii" => { "aca/spi_gain_coeff"     => "SPI.-COEF-CAL", },
	
	"arc" => {
		#  NOTE:  if you add something here,
		#  may have to change global indexing: 
		#  time stamp or no?
		#  It assumes not, so if yes, change...
		"osm/hk_averages" => "GNRL-AVRG-GRP",
		"cfg/picsit_fault_list"=>"PICS-FALT-STA",
	},
	
	"ire" => { "prp/irem_prp" => "IREM-CHNK-GRP" },
	
	"jm1" => { "aca/jemx1_aca_frss" => "JMX1-GAIN-CAL" },
	
	"jm2" => { "aca/jemx2_aca_frss" => "JMX2-GAIN-CAL" },
	
	);



######################################################################

=item %Datasets::Products

This is a list of every type of data under rev.000 (excluding things under logs, i.e. logs and alerts.)  It's the root name except in the case of INT and TPF files, whose root names are not under pipeline control.

Note that some (sub)hash elements point to an array instead of a scalar, where there are several products under the same subdir.  See the RevContentsCheck subroutine below for how to access those.

040326 - Jake - if the name is not here, it won't be copied and the data will be deleted

=cut

%Datasets::Products = (
	
	#  Also under arc, so change both:
	"spa" => {
		"osm" => "spi_psd_adcgain",    
	},
	
	"spp" => {
		"osm" => "spi_psd_performance",
	},
	
	"spe" => {
		"osm" => "spi_psd_efficiency",
	},
	
	"sps" => {
		"osm" => "spi_psd_si",
	},
	
	#	iii (from iii_prep which is just SPI calibration) 	#	040525 - Jake
	"iii" => {
		"aca" => [ 
			"spi_cal_se_spectra",
			"spi_cal_me_spectra",
			"spi_cal_se_results",
			"spi_cal_me_results",
			"spi_gain_coeff",
			]
	},
	
	#  TO BE FIXED:  PICsIT ACA result will go here too.
	"arc" => {
		#  WARNING:  array instead of string here!
		"osm" => [
			"exposure_report",
			"spi_psd_adcgain",
			"spi_psd_performance",
			"spi_psd_efficiency",
			"spi_psd_si",
			"isgri_cdte_cor",			#	040322 - Jake - SCREW 791 
			],
		"cfg" => "picsit_fault_list",
	},
	
	"ire" => { 
		"raw" => "irem_raw", 
		"prp" => "irem_prp",
	},
	
	"irn" => { 
		"raw" => "isgri_raw_noise", 
		"prp" => "isgri_prp_noise",
		"cfg" => "isgri_pxlswtch",
	},
	
	"irv" => { 
		"raw" => "ibis_raw_veto", 
		#  WARNING:  array instead of string here!
		"aca" => [ 
			"ibis_aca_veto",
			"ibis_aca_cu",
			]
	},
	
	"irc" => { 
		"raw" => "isgri_raw_cal", 
		"prp" => "isgri_prp_cal",
	},
	
	"itv" => { "raw" => "ibis_raw_tver", },
	
	"idp" => { 
		"raw" => "ibis_raw_dump", 
		#  WARNING:  array instead of string here!
		"cfg" => [ 
			"isgri_context",
			"picsit_context",
			"hepi_context",
			"veto_context",
			"iasw_context",
			"picsit_fault_list" 
			],
	},
	
	
	"ilt" => { 
		#  WARNING:  array instead of string here!
		"cfg" => [
			"isgri_context_new", 
			"TPF",
			"INT",
			"isgri_context_dead",				#	040323 - Jake - SCREW 1344
			],
	},
	
	"prc" => { 
		"raw" => "picsit_raw_cal", 
		"aca" => "picsit_aca_cal", 
	}, 
	
	"jm1" => { 
		"raw" => "jemx1_raw_frss", 
		"aca" => "jemx1_aca_frss",
	},
	
	
	"j1d" => { "raw" => "jemx1_raw_dump", },
	"j1f" => { "raw" => "jemx1_raw_dfeedump", },
	"j1t" => { "raw" => "jemx1_raw_tver", },
	"j1e" => { 
		"raw" => "jemx1_raw_ecal", 
		"prp" => "jemx1_prp_ecal", 
	},
	
	"jm2" => { 
		"raw" => "jemx2_raw_frss", 
		"aca" => "jemx2_aca_frss",
	},
	
	"j2d" => { "raw" => "jemx2_raw_dump", },
	"j2f" => { "raw" => "jemx2_raw_dfeedump", },
	"j2t" => { "raw" => "jemx2_raw_tver", },
	"j2e" => { 
		"raw" => "jemx2_raw_ecal", 
		"prp" => "jemx2_prp_ecal", 
	},
	
	"obc" => { "raw" => "omc_raw_bias", },
	"odc" => { "raw" => "omc_raw_dark", },
	"ofc" => { "raw" => "omc_raw_flatfield", },
	"osc" => { "raw" => "omc_raw_sky", },
	"omd" => { "raw" => "omc_raw_dump", },
	"omt" => { "raw" => "omc_raw_tver", },
	
	"stv" => { "raw" => "spi_raw_tver", },
	"sdp" => { "raw" => "spi_raw_dump", },
	
	"sac" => { "raw" => "spi_acs_cal", },
	
	);


######################################################################

=back

=head1 SUBROUTINES

=over

=item B<RevDataset> ($osf)

parse trigger file OSF name to get full datasetfile name, using above Types has table;  

also sets other useful stuff like revno, previous, etc.  

=cut

sub RevDataset {
	my ($osf) = @_;
	#my $date = &TimeLIB::MyTime();
	#my $unknown;
	#  Format:    RRRR_YYYYMMDDHHMMSS_VV_MMM
	($osf =~ /^(\d{4})_(\d{14})_(\d{2})_(\w{3})$/) 
		or ($osf =~ /(\d{4})_arc_prep/)
		or ($osf =~ /(\d{4})_iii_prep/)											
		or die "*******     ERROR:  don't recognize OSF $osf";
	my $revno = $1;
	my $prevrev = $revno - 1;
	$prevrev = sprintf "%04d", $prevrev;
	my $nextrev = $revno + 1;
	$nextrev = sprintf "%04d", $nextrev;
	#  print "Previous revolution is $prevrev;  next rev is $nextrev\n";
	# if it's an arc_prep trigger, return immediately
	if ($osf =~ /arc_prep/) {
		return($osf,"arc",$revno,$prevrev,$nextrev,1);
	}
	# if it's an iii_prep trigger, return immediately						
	if ($osf =~ /iii_prep/) {														
		return($osf,"iii",$revno,$prevrev,$nextrev,1);						
	}																						
	my $time = $2;
	my $vers = $3;
	my $type = $4;
	
	if ($Datasets::Types{$type}) {
		#    print "*******  Type $type corresponds to $Datasets::Types{$type}\n";
	}
	else {
		die "*******     ERROR:  unrecognized dataset $type!";
	}
	my $file = "$Datasets::Types{$type}_${time}_${vers}.fits";

	return ($file,$type,$revno,$prevrev,$nextrev,1,$vers) ;
	
}  # RevDataset


######################################################################

=item B<RevContentsCheck> ($revno)

given a revolution number, checks that all contents are recognized, used as preparation for archiving.

=cut

sub RevContentsCheck {
	my ($revno) = @_;
	my @matches;
	my $one;
	my ($root,$path,$suffix);
	my $type;
	my $element_ref;
	
	print "*******     Checking contents of revolution $revno;\n*******     "
			."You'll only see the file name printed if it's recognized.\n";
	chdir "$ENV{SCWDIR}/${revno}/rev.000/" 
		or die "*******     ERROR:  cannot chdir to $ENV{SCWDIR}/${revno}/rev.000/";
	my @contents = sort(glob("*"));
	push @contents, sort(glob("*/*"));
	print "*******     Found ".scalar(@contents)." files to check\n";
	
	FILES:  
	foreach $one (@contents) {
		
		print "*******     File:  $one\n";
		($root,$path,$suffix) = &File::Basename::fileparse($one,'\..*');
		$path =~ s/\///;
		#    print "*******      Root is $root, suffix is $suffix, path is $path\n";
		# top level contents:  raw,prp,ica,aca,osm,logs and nothing else
		next if (($root =~ /^(raw|prp|cfg|aca|osm|logs)$/) && !($suffix));



		next if ( $root =~ /^README$/ );	#	060613 - Jake - added cause this new file may exist during some reprocessing



		
		# OSM dir contents:  all data files in osm should be in the cfg files 
		#  except for the group name and the exposure status file:
		next if (($root =~ /hk_averages/) && ($suffix =~ /fits/));
		next if (($root =~ /exposure_report/) && ($suffix =~ /fits/));

		@matches = `$mygrep $root $ENV{CFITSIO_INCLUDE_FILES}/GNRL_AVRG_GRP.cfg`;
		foreach (@matches) { # it may appear more than once, e.g. in comments
			# (only one file grep'ed, no ":" like in nswfin.pl)
			if (/^file\s/) {
				# if the line starts just "file ", then it's in there
				print "*****     Found in configuration file:  $_";
				next FILES;
			}
		}
		
		# LOGS dir contents include alerts:
		next if ( ($path =~ /logs/) && ($root =~ /L\d{1}_/)&& ($suffix =~ /alert/));  
		#  and .txt logs:
		next if ( ($path =~ /logs/) && ($suffix =~ /txt/));
		
		#  Because for these, I don't control the file names, can only
		#   do a cursory check of the suffix and location:
		next if ( ($path =~ /cfg/) && ($suffix =~ /TPF|INT/));
		
		#  Everything else should be listed under Products:
		#  (But unfortunately, we have to search through the hash.  Perhaps
		#     I should turn it around?  So very inefficient this way, but
		#     listed by type will be useful elsewhere.)
		foreach $type ( keys( %Datasets::Products ) ) {
			
			next unless defined $Datasets::Products{$type}{$path};
			
			#  Need a reference to figure out whether there are multiple
			#   products under a given subdirectory (SCALAR vs ARRAY element):
			$element_ref = \$Datasets::Products{$type}{$path};
			
			if (ref( $element_ref ) eq "SCALAR") {
				#  This for single entries:
				next FILES if ($root =~ /$Datasets::Products{$type}{$path}/);
			}
			else {
				#  This for array entries: (which are REF not ARRAY?)
				foreach ( @{ $Datasets::Products{$type}{$path} } ) {
					next FILES if ($root =~ /$_/);
				}
			} # end if array
			
		} # end foreach type
		
		#  Lastly, check all defined raw files:
		foreach $type ( keys( %Datasets::Types ) ) {
			next unless defined$Datasets::Types{$type};
			next FILES unless ($root =~ /$Datasets::Types{$type}/);
		} # end foreach type
		
		# if you got to here, you're a junk file: 
		#print "*******     ERROR:  Found junk file $one\n";
		&Error( "ERROR before archiving: Found junk file $one" );
		
	} # end of foreach $one (@contents) {
	
	return;
	
} # end RevContentsCheck

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
    
