#!perl

=head1 NAME

I<nqlobs.pl> - nrtqla pipeline mosaic step script

=head1 SYNOPSIS

I<nqlobs.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

This script does a cleanup of any potential residue from previous runs, creates the OG and runs the associated _science_analysis.  This is then followed by a call to QLALIB::QCheck.

=cut

use strict;
use warnings;

use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use IBISLIB;
use JMXLIB;
use QLALIB;

print "\n========================================================================\n";

&ISDCPipeline::EnvStretch ( "LOG_FILES", "OUTPATH", "WORKDIR", "OBSDIR", "PARFILES", "IC_ALIAS" );

my ( $obsid, $revno, $inst, $INST, $og, $pdefv ) = &QLALIB::ParseOSF ( $ENV{OSF_DATASET} );
my $in = &ISDCLIB::inst2in ( $inst );
&Message ( "obsid - $obsid" );
&Message ( "revno - $revno" );
&Message ( "pdefv - $pdefv" );

my $proc = &ProcStep()." $INST";

&Message ( "$proc - STARTING" );

$ENV{PARFILES} = "$ENV{OPUS_WORK}/nrtqla/scratch/$ENV{OSF_DATASET}/pfiles";

&ISDCLIB::DoOrDie ( "mkdir -p $ENV{PARFILES}" ) unless ( -d "$ENV{PARFILES}" );

print "*******     ObsID is $ENV{OSF_DATASET};  Instrument is $INST;  group is $og.\n";

if ( -d "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000" ) {
	#  Clean up previous run:
	`$mychmod -R 755 $ENV{OBSDIR}/$ENV{OSF_DATASET}.000`;
	
	#  Move log back to central dir (assuming it exists and the previous
	#   run had gotten that far.
	&ISDCPipeline::MoveLog (
		"$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}_qla.txt",
		"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_qla.txt",
		"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"
		) if ( -e "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}_qla.txt" );
	
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - clean up previous",
		"program_name" => "$myrm -rf $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/",
		"subdir"       => "$ENV{WORKDIR}",
		);
}

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs" ) 
	unless ( -d "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs" );
open OBSIDX, "< $ENV{OPUS_WORK}/nrtqla/mosaics/${revno}_${obsid}_${pdefv}.txt"
	or &Error ( "Couldn't open $ENV{OPUS_WORK}/nrtqla/mosaics/${revno}_${obsid}_${pdefv}.txt" );
open IDX2OG, "> $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}.idx2og" 
	or &Error ( "Couldn't open $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}.idx2og" );
while ( <OBSIDX> ) {
	chomp;
	my $swg = "obs/qs${in}_${_}.000/scw/${_}.000/swg_${inst}.fits";
	print IDX2OG "$swg\n";
}
close IDX2OG;
close OBSIDX;

&ISDCPipeline::PipelineStep (
	"step"           => "$proc - create OG of one science window",
	"program_name"   => "og_create",
	"par_idxSwg"     => "obs/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}.idx2og",
	"par_instrument" => "$INST",
	"par_ogid"       => "$ENV{OSF_DATASET}",
	"par_baseDir"    => "./",
	"par_obs_id"     => "",
	"par_purpose"    => "Mosaic QLA",
	"par_versioning" => "1",
	"par_obsDir"     => "obs",
	"par_scwVer"     => "001",
	"par_swgName"    => "swg",
	"par_keep"       => "",
	"par_verbosity"  => "3",
	"subdir"         => "$ENV{REP_BASE_PROD}",
	);

&ISDCPipeline::MoveLog (
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_qla.txt",
	"$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}_qla.txt",
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"
	);

chdir ( "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000" )
	or die "Cannot chdir to $ENV{OBSDIR}/$ENV{OSF_DATASET}.000";
print "*******     Current directory is $ENV{OBSDIR}/$ENV{OSF_DATASET}.000\n";

#  Just call the appropriate script:
my $osa9qlaNum=3; # We will add the JEMX number to get appropriate input
                  # value of 4 for JEMX1 and 5 for JEMX2 needed in post-JSA step below:
if    ( $INST =~ /IBI/ ) {
	&IBISLIB::ISA (
		"INST"    => "$INST",
		"proctype" => "mosaic",
		);	
	`cat2ds9 catDOL="isgri_mosa_res.fits[2]" fileName="isgri_20-40keV.reg" symbol=circle color=white`;
	`cat2ds9 catDOL="isgri_mosa_res.fits[3]" fileName="isgri_40-80keV.reg" symbol=circle color=white`;
}
elsif ( $INST =~ /JMX(\d)/ ) {
    &JMXLIB::JSA (
		"jemxnum"  => "$1",
		"proctype" => "mosaic",
		);
    $osa9qlaNum+=$1;
    
    print "=== post-Mosaic processing for JEMX$1 ===\n";

    &ISDCPipeline::PipelineStep(
	"step"                => "Source location step for JEMX$1 - Soft",
	"program_name"        => "j_ima_src_locator",
	"par_inDOL"           => "J_MOSAIC.fits[2]",
	"par_sigDOL"          => "J_MOSAIC.fits[4]",
	"par_outFile"         => "sloc_res_soft",
	);
    
    &ISDCPipeline::PipelineStep(
	"step"                => "Source identification step for JEMX$1 - Soft",
	"program_name"        => "q_identify_srcs",
	"par_srcl_cat_dol"    => $ENV{'ISDC_REF_CAT'},
	"par_srcl_res_dol"    => "sloc_res_soft.fits",
	"par_instrument"      => $osa9qlaNum,
	);
    
    `cat2ds9 catDOL=sloc_res_soft.fits fileName=jmx_soft.reg symbol=circle color=white`;

    &ISDCPipeline::PipelineStep(
	"step"                => "Source location step for JEMX$1 - Hard",
	"program_name"        => "j_ima_src_locator",
	"par_inDOL"           => "J_MOSAIC.fits[6]",
	"par_sigDOL"          => "J_MOSAIC.fits[8]",
	"par_outFile"         => "sloc_res_hard",
	);
    
    &ISDCPipeline::PipelineStep(
	"step"                => "Source identification step for JEMX$1 - Hard",
	"program_name"        => "q_identify_srcs",
	"par_srcl_cat_dol"    => $ENV{'ISDC_REF_CAT'},
	"par_srcl_res_dol"    => "sloc_res_hard.fits",
	"par_instrument"      => $osa9qlaNum,
	);
    
    `cat2ds9 catDOL=sloc_res_hard.fits fileName=jmx_hard.reg symbol=circle color=green`;   
}

`$myrm -rf $ENV{PARFILES}` if ( -e "$ENV{PARFILES}" );

exit 0;


=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut
