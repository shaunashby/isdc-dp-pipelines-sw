#!perl

=head1 NAME

I<nqlscw.pl> - nrtqla pipeline SCW step script

=head1 SYNOPSIS

I<nqlscw.pl> - Run from within B<OPUS>.  

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

my ( $scwid, $revno, $inst, $INST, $og ) = &QLALIB::ParseOSF ( $ENV{OSF_DATASET} );
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

#	this should always be 000, but for the re-qla processing, we need this
my ( $dirvers ) = ( &ISDCLIB::FindDirVers ( "$ENV{REP_BASE_PROD}/scw/$revno/$scwid" ) =~ /\.(\d{3})$/ );

my $swg = ( -e "$ENV{REP_BASE_PROD}/scw/$revno/$scwid.$dirvers/swg.fits" ) ? "swg" : "swg_prp";

&ISDCPipeline::PipelineStep (
	"step"           => "$proc - create OG of one science window",
	"program_name"   => "og_create",
	"par_idxSwg"     => "scw/$revno/$scwid.$dirvers/$swg.fits[GROUPING]",
	"par_instrument" => "$INST",
	"par_ogid"       => "$ENV{OSF_DATASET}",
	"par_baseDir"    => "./",
	"par_obs_id"     => "",
	"par_purpose"    => "ScW QLA",
	"par_versioning" => "1",
	"par_obsDir"     => "obs",
	"par_scwVer"     => "001",
	"par_swgName"    => "swg",
	"par_keep"       => "",
	"par_verbosity"  => "3",
	"subdir"         => "$ENV{REP_BASE_PROD}",
	);

# Move log to directory where we want it now:
&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs" ) unless ( -d "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs" );
&ISDCPipeline::MoveLog (
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}_qla.txt",
	"$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs/$ENV{OSF_DATASET}_qla.txt",
	"$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log"
	);

chdir ( "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000" )
	or die "Cannot chdir to $ENV{OBSDIR}/$ENV{OSF_DATASET}.000";
print "*******     Current directory is $ENV{OBSDIR}/$ENV{OSF_DATASET}.000\n";


#  Just call the appropriate script:
if    ( $INST =~ /IBI/ ) {
	&IBISLIB::ISA (
		"INST"    => "$INST",
		);	
}
elsif ( $INST =~ /JMX(\d)/ ) {
	&JMXLIB::JSA (
		"jemxnum"  => "$1",
		);
}

&QLALIB::QCheck (
	"ogDOL" => "$og"."[1]",
	"INST"  => "$INST"
	);

`$myrm -rf $ENV{PARFILES}` if ( -e "$ENV{PARFILES}" );

exit 0;

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
