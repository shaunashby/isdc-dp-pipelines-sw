#!perl -w

use strict;
use lib "$ENV{ISDC_OPUS}/pipeline_lib/";
use lib "/home/wendt/";
use ISDCPipeline;
use OPUSLIB;
use UnixLIB;
use ISDCLIB;
use QLAMOS;

######################################################################
#
#	All after this was added by Jake, 070502, with regards to SCREW 1983
#
#	* OBS_ID to EXP_ID matching should be made with the pod
#	* EXP_ID to SCW_ID matching should be made with the pointing_definition_predicted_vvv
#	* The pod version should be taken from the PODV column in the PDEF.
#	* The OBS group name should be qm{ii,j1,j2}_obsid_vvvv where vvvv is taken from the PDEF name

$ENV{PATH_FILE_NAME} = "nrtqla";
$ENV{LOG_FILES}    = "$ENV{OPUS_WORK}/nrtqla/logs";
$ENV{MOSAICS}      = "$ENV{OPUS_WORK}/nrtqla/mosaics";
$ENV{OBSDIR}       = "$ENV{REP_BASE_PROD}/obs";
$ENV{PROCESS_NAME} = "nqlmon";
$ENV{OSF_DATASET} = "NQLMON_UNDECLARED_OSF_DATASET";

my @revolutions = &ISDCLIB::DoOrDie ( "$myls -1d $ENV{REP_BASE_PROD}/scw/???? | tail -2 | awk -F/ '{print \$NF}'" );

foreach my $rev ( @revolutions ) {
	&QLAMOS::Mosaic ( $rev );
}

exit 0;
