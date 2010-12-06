#!perl

=head1 NAME

I<nqlfin.pl> - nrtqla pipeline FIN step script

=head1 SYNOPSIS

I<nqlfin.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

Write protect all data, add this OSF to the GNRL-OBSG-GRP-IDX and copy alerts.

This also will move the trigger to trigger_done, which I found completely unnecessary.  Especially since this single trigger is associated with 3 actual OSFs which means that it is probably NOT actually done.  I have also modified the cleanup script to delete the trigger file when the first OSF is cleaned.  I think that this functionality should be removed or the whole idea of nrtqla processing should be rethought.  

Also, the triggers for the slews, since we only ever process the pointings continue to remain in the input directory.  This should also be rethought.

=cut

use strict;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;
use lib "$ENV{ISDC_OPUS}/nrtqla";
use QLALIB;

my ($retval,@result);

print "\n========================================================================\n";
print "*******     Trigger $ENV{OSF_DATASET} received\n";

&ISDCPipeline::EnvStretch ( "LOG_FILES","OUTPATH","WORKDIR","OBSDIR","PARFILES","ALERTS" );

#	$pdefv is ONLY returned if this is a mosaic, so I use it as a flag
my ( $obsid, $revno, $inst, $INST, $og, $pdefv ) = &QLALIB::ParseOSF ( $ENV{OSF_DATASET} );
my $proc = &ProcStep()." $INST";

&Message ( "$proc - STARTING" );

print "*******     Obs/Scw is $obsid;  Instrument is $INST;  group is $og.\n";

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - write protect results",
	"program_name" => "$mychmod -R -w *fits scw", 
	"subdir"       => "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000",
	);

&ISDCPipeline::MakeIndex (
	"root"     => "GNRL-OBSG-GRP-IDX",
	"subdir"   => "$ENV{OUTPATH}/idx/obs",
	"add"      => "1",
	"osfname"  => "$ENV{OSF_DATASET}",
	"files"    => "$og",
	"filedir"  => "../../obs/$ENV{OSF_DATASET}.000/",
	"ext"      => "[GROUPING]",
	"template" => "GNRL-OBSG-GRP-IDX.tpl",
	"clean"    => "3",
	) unless ( $pdefv );

&ISDCPipeline::LinkUpdate (
	"root"    => "GNRL-OBSG-GRP-IDX",
	"ext"     => ".fits",
	"subdir"  => "$ENV{REP_BASE_PROD}/idx/obs",
	"logfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log",
	) unless ( $pdefv );

#  Move alerts into logs subdirectory
&ISDCPipeline::PipelineStep (
	"step"         => "$proc - move alerts to logs dir",
	"program_name" => "$mymv *alert* logs",
	"subdir"       => "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/",
	) if ( `$myls $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/*alert* 2> /dev/null` );

unless ( $pdefv ) {
	if ( ! -d "$ENV{ALERTS}" ) {
		( $retval, @result ) = &ISDCPipeline::RunProgram ( "$mymkdir -p $ENV{ALERTS}" );
		die "*******     Cannot \'$mymkdir -p $ENV{ALERTS}\':  @result" if ($retval);
	}

	my $scw_prp_index;
	$scw_prp_index = "$ENV{REP_BASE_PROD}/idx/scw/prp/GNRL-SCWG-GRP-IDX.fits[GROUPING]" 
		if ( -e "$ENV{REP_BASE_PROD}/idx/scw/prp/GNRL-SCWG-GRP-IDX.fits" );
	&ISDCPipeline::PipelineStep (
		"step"           => "$proc - copy alerts to $ENV{ALERTS}",
		"program_name"   => "am_cp",
		"par_OutDir"     => "$ENV{ALERTS}",
		"par_OutDir2"    => "",
		"par_Subsystem"  => "QLA",
		"par_DataStream" => "realTime",
		"par_ScWIndex"   => $scw_prp_index,
		"subdir"         => "$ENV{OBSDIR}/$ENV{OSF_DATASET}.000/logs",
		);
}

&Message ( "$proc - write protect obs dir" );

##  Now recursively write protect, and hereafter log  only to process log
( $retval, @result ) = &ISDCPipeline::RunProgram ( "$mychmod -R -w $ENV{OBSDIR}/$ENV{OSF_DATASET}.000/" );

die "*******  ERROR:  cannot write protect $ENV{OBSDIR}/$ENV{OSF_DATASET}.000:\n@result" if ( $retval );

unless ( $pdefv ) {
	##  Move trigger to done:
	( $retval, @result ) = &ISDCPipeline::RunProgram (
		"$mymv $ENV{INPUT}/$ENV{OSF_DATASET}.trigger_processing "
			."$ENV{INPUT}/$ENV{OSF_DATASET}.trigger_done"
		) if ( -e "$ENV{INPUT}/$ENV{OSF_DATASET}.trigger_processing" ); 
	die "******     ERROR:  Cannot move trigger file $ENV{INPUT}/$ENV{OSF_DATASET}.trigger_processing "
		."to done:\n@result" if ( $retval );

	# if it had an error during processing, was fixed and reset by hand,
	#  then this needs to find the "_bad" trigger file instead.  
	( $retval, @result ) = &ISDCPipeline::RunProgram (
		"$mymv $ENV{INPUT}/$ENV{OSF_DATASET}.trigger_bad "
			."$ENV{INPUT}/$ENV{OSF_DATASET}.trigger_done"
		) if ( -e "$ENV{INPUT}/$ENV{OSF_DATASET}.trigger_bad" ); 
	die "******     ERROR:  Cannot move trigger file $ENV{INPUT}/$ENV{OSF_DATASET}.trigger_bad "
		."to done:\n@result" if ( $retval );
}

exit;


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
