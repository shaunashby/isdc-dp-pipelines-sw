#!perl

=head1 NAME

ninput.pl -  Input Pipeline 


=head1 SYNOPSIS

I<ninput.pl> - Run from within B<OPUS>.  This is the second of a three step 
pipeline which processes raw science windows written by Pre-Processing.  

=head1 DESCRIPTION

This process triggers on the blackboard entry for a science window and performs
a few final steps to prepare the data for the science window pipeline.  

=over 5

=item B<spi_merge_schk>

The first action is to run the executable B<spi_merge_schk> to perform
some reformatting of science and housekeeping data for SPI which is not 
done in Pre-Processing.  The only input is the raw science window group.  
Please type "spi_merge_schk --h" for more information.  

=item B<clean_scw>

The next action is to clean the raw science window group.  This examines every
member of the group and deletes those which are empty, with the exception of 
housekeeping data structures.  See the help for B<clean_scw> for more 
information.  Its parameters are the raw science window group and the flag
"showonly=no", i.e. to really delete the empty elements.  

=item B<idx_add>

Next, the science window is added to indices kept in the pipeline 
workspace, one index for the current revolution and one for the next.  
These indices is used later in the Science Window Revolution 
Pipeline.  See the help for B<idx_add> for more information and the Science 
Window ADD for the usage of these indices.   

=item B<locking>

Lastly, the pipeline write protects all the resulting data.  This is a 
simple UNIX B<chmod> command to remove write permission on the raw 
science window group and all files in the "raw" subdirectories.  

=back


=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=over 5

=item B<SCWDIR>

This is set to the B<rii_scw> entry in the path file.  This is the 
scw part of the repository.

=item B<LOG_FILES>

This is set to the B<log_files> entry in the path file.  It is the 
location of all log files seen by OPUS.  The real files are located
in the repository and linked to from here.

=item B<PARFILES>

This is set to the B<parfiles> entry in the path file.  It is the 
location of the pipeline parameter files.  

=item B<WORKDIR>

This is the workspace for the pipeline.  Here, it creates the indices of
the science windows. 

=back

=cut

use strict;
use ISDCPipeline;
use UnixLIB;
use ISDCLIB;

&ISDCPipeline::EnvStretch ( "SCWDIR", "LOG_FILES", "PARFILES", "WORKDIR", "ALERTS" );

my $proc = &ISDCLIB::Initialize();
#my $proc = &ProcStep();
my $path = ( $ENV{PATH_FILE_NAME} =~ /cons/ ) ? "consinput" : "nrtinput";

# Set group name and extension
#	why both grpdol and grpname??????????????	- because its easier in some cases
my $grpdol  = "swg_raw.fits[1]";
my $grpname = "swg_raw.fits";
my $newname = "swg.fits";

#
#  Get raw list here and access it throughout this script
#
my @raw_list = &ISDCLIB::ParseConfigFile ( "GNRL_SCWG_RAW.cfg" );
my $revno    = &ISDCPipeline::RevNo ( $ENV{OSF_DATASET} );

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - STARTING",
	"program_name" => "NONE",
	"type"         => "inp",
	);

my $fitslist = "";
foreach my $raw ( @raw_list ) {
	$fitslist .= "$raw.fits " if ( -r "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/$raw.fits" );
}

if ( ! -w "$grpname" ) {
	&ISDCPipeline::PipelineStep (
		"step"         => "$proc - changing permissions",
		"program_name" => "$mychmod -R +w $fitslist $grpname",
		"type"         => "inp",
		);
}

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - spi_merge_schk",
	"program_name" => "spi_merge_schk",
	"par_scw_dol"  => "$grpdol",
	"par_kill_old" => "yes",
	"type"         => "inp",
	"structures"   => "SPI.-SHK1-TMP SPI.-SCHK-HRW",
	);

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - clean science window group",
	"program_name" => "swg_clean",
	"par_object"   => "$grpdol",
	"par_showonly" => "no",
	"type"         => "inp",
	);


#	050331 - SCREW 1693 - We used to gzip the raw data here, but it seems pointless as we just throw it away

$fitslist = "";
foreach my $raw ( @raw_list ) {
	$fitslist .= "$raw.fits " if ( -r "$ENV{SCWDIR}/$revno/$ENV{OSF_DATASET}.000/$raw.fits" );
}

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - lock RAW data and group",
	"program_name" => "$mychmod -R -w $fitslist $grpname",
	"type"         => "inp",
	);

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{REP_BASE_PROD}/idx/scw/raw" ) unless ( -d "$ENV{REP_BASE_PROD}/idx/scw/raw" );

&ISDCPipeline::MakeIndex (
	"root"     => "GNRL-SCWG-GRP-IDX",
	"subdir"   => "$ENV{REP_BASE_PROD}/idx/scw/raw",
	"osfname"  => "$ENV{OSF_DATASET}",
	"type"     => "inp",
	"ext"      => "[1]",
	"filedir"  => "../../../scw/$revno/$ENV{OSF_DATASET}.000",
	"files"    => "$grpname",
	"add"      => "1",
	"template" => "GNRL-SCWG-GRP-IDX.tpl",
	# can't sort on TSTART as elsewhere;  not set!
	"sort"     => "",
	);

&ISDCPipeline::LinkUpdate (
	"root"    => "GNRL-SCWG-GRP-IDX",
	"ext"     => ".fits",
	"subdir"  => "$ENV{REP_BASE_PROD}/idx/scw/raw",
	"type"    => "inp",
	"logfile" => "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log",
	);

&ISDCLIB::DoOrDie ( "$mymkdir -p $ENV{ALERTS}" ) unless ( -d "$ENV{ALERTS}" );

&ISDCPipeline::PipelineStep (
	"step"           => "$proc - copy alers",
	"program_name"   => "am_cp",
	"par_OutDir2"    => "",
	"par_OutDir"     => "$ENV{ALERTS}",
	"par_Subsystem"  => "PP",
	"par_DataStream" => "realTime",
	"par_ScWIndex"   => "",
	);

&ISDCPipeline::PipelineStep (
	"step"         => "$proc - done",
	"program_name" => "NONE",
	"type"         => "inp",
	);

exit 0;


######################################################################


__END__ 


=head1 REFERENCES

For further information on the other processes in this pipeline, please run
perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see
C<file:///isdc/software/opus/html/opusfaq.html> on the office network
or C<file:///isdc/opus/html/opusfaq.html> on the operations network.
Note that understanding this document requires that you understand
B<OPUS> first.

For further information about the Input Pipeline, please see the Input 
Pipeline ADD.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <Jake.Wendt@obs.unige.ch>

=cut

