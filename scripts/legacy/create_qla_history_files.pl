#!perl

#	qdp column names
#	center dT count_soft error_soft count_hard error_hard detsig radius revolution pointing subpointing pointingtype 

#	ops_nrt@anaB6:~> rsync -avz /home/scientist/IQLA/SOURCES/ /isdc/pvphase/IQLA/SOURCES/

#  ops_nrt@anaB6:~> cp /nrt/ops_1/cat/hec/nrt_refr_cat.fits /isdc/pvphase/IQLA/


use strict;
use warnings;

use File::Basename;
$ENV{COMMONLOGFILE} =~ s/^\+//;

my $source_data = "/isdc/pvphase/IQLA/SOURCES";

die "$source_data unavailable." unless ( -r $source_data ); 

my $refcat = "/isdc/pvphase/IQLA/nrt_refr_cat.fits";
die "$refcat unavailable." unless ( -r $refcat );

die "Chances are that you didn't source \$HEADAS/headas-init.csh" unless ( $ENV{PATH} =~ /headas/ );

my %hsh;
$hsh{"jemx"}{"template"} = "JMX1-SRCL-CAT";	#	JMX1 or JMX2.  Doesn't matter.
$hsh{"jemx"}{"filename"} = "jemx_qla_history.fits";
$hsh{"jemx"}{"max_bin1"} = 70;
$hsh{"jemx"}{"factor_bin1"} = 1;
$hsh{"jemx"}{"max_bin2"} = 25;
$hsh{"jemx"}{"factor_bin2"} = 1;

$hsh{"isgr"}{"template"} = "ISGR-SRCL-CAT";
$hsh{"isgr"}{"filename"} = "isgr_qla_history.fits";
$hsh{"isgr"}{"max_bin1"} = 85;
$hsh{"isgr"}{"factor_bin1"} = 1.5872;
$hsh{"isgr"}{"max_bin2"} = 55;
$hsh{"isgr"}{"factor_bin2"} = 1.4194;

#	-rw-r--r--    1 4510     pv           2692 Aug 13 10:59 0467_Vela_X-1_isgri_lc.qdp
#	-rw-r--r--    1 4510     pv           2297 Aug 13 10:59 0467_Vela_X-1_jemx1_lc.qdp

#	use a , for fldsep for easier parsing later
my @catalog = &DoOrDie ( "fdump fldsep=, outfile=STDOUT columns=\"SOURCE_ID,NAME\" rows=- prhead=no pagewidth=256 page=no wrap=yes showrow=no showcol=no showunit=no infile=$refcat" );

open LOG, ">> qla.log";
print LOG "Opening : ",`date`;

foreach my $instr ( keys ( %hsh ) ) {
	my %source;

	print     "\n######################################################################\n\n";
	print     "Processing $instr\n\n";
	print LOG "\n######################################################################\n\n";
	print LOG "Processing $instr\n\n";
	print LOG "$hsh{$instr}{'template'}\n";
	print LOG "$hsh{$instr}{'filename'}\n";

	print &DoOrDie ( "dal_create obj_name=$hsh{$instr}{'filename'} template=$hsh{$instr}{'template'}.tpl" );
	foreach my $column ( qw/DAY_ID CLASS RA_OBJ DEC_OBJ ERR_RAD SPA_MODL SPA_NPAR SPA_PARS SPE_MODL SPE_NPAR SPE_PARS VAR_MODL VAR_NPAR VAR_PARS SPI_FLUX_1 SPI_FLUX_2 ISGR_FLUX_1 ISGR_FLUX_2 PICS_FLUX_1 PICS_FLUX_2 JEMX_FLUX_1 JEMX_FLUX_2 E_MIN E_MAX FLUX FLUX_ERR RATE_ERR SEL_FLAG FLAG/ ) {
		print "Deleting unused column +$column+\n";
		print &DoOrDie ( "fdelcol infile=$hsh{$instr}{'filename'}+1 colname=$column confirm=n proceed=y" );
	}

	my $source_name = "";
	my $source_id   = "";
	chomp ( my $date = `date` );
	my $source_cmt  = "Computed maximum from qdp files on $date";
	SOURCE: foreach my $source_dir ( glob "$source_data/*" ) {
		#print "$source_dir \n";
		$source_name  = &basename ( $source_dir );
		$source_name =~ s/_/ /g;
		print LOG "\nProcessing source : $source_name \n";

		my @match = grep /,$source_name\s*,/, @catalog;	#	here's where the comma is needed to ensure there is only 1 correct match
		if ( scalar @match > 1 ) {
			print "More than 1 match\n@match";
			print LOG "More than 1 match\n@match";
			die "This needs investigation and perhaps a better grep on the lines above!";
		} if ( scalar @match == 1 ) {
			print LOG "One match found for $source_name.\n@match";
 		} else {
			print LOG "No matches.  Skipping $source_name.\n";
			next SOURCE;
		}

		( $source_id ) = ( $match[0] =~ /^([^,]+),/ );
		print LOG "$source_id\n";

		my @qdp_files = glob "$source_dir/*$instr*qdp";
		unless ( @qdp_files ) {
			print LOG "No qdp files found for $source_name and $instr.  Skipping.\n";
			next SOURCE;
		}
		print LOG "Computing maximums from all found qdp files.\n";
		my $max_bin1 = 0;
		my $max_bin2 = 0;
		foreach my $qdp_file ( @qdp_files ) {
			#	print "Reading $qdp_file\n";
			open QDP, "< $qdp_file";
			while (<QDP>) {
				#	The important lines are of similar format to ...
				#  2390.561301884880 0.0125578776098791    18.2054    1.1155    10.0588    0.7230   29.266   2.40  ! 0459 0042 001 0/ );
				#	column 3 and column 5 are the fluxes
				next unless ( /^\s+\S+\s+\S+\s+(\S+)\s+\S+\s+(\S+)/ );
				$max_bin1 = $1 if ( $1 > $max_bin1 );
				$max_bin2 = $2 if ( $2 > $max_bin2 );
			}
			close QDP;
		}
		print "$source_name : $instr : Max1 $max_bin1 : Max2 $max_bin2 \n";
		$source{"$source_id"}{"name"} = $source_name;
		$source{"$source_id"}{"max1"} = ( $max_bin1 > $hsh{$instr}{'max_bin1'} ) ? $hsh{$instr}{'max_bin1'} : $max_bin1;
		$source{"$source_id"}{"max2"} = ( $max_bin2 > $hsh{$instr}{'max_bin2'} ) ? $hsh{$instr}{'max_bin2'} : $max_bin2;
		$source{"$source_id"}{"cmt"}  = $source_cmt;
	}
	open OUTPUT, "> $instr.maximums";
	my $numrows = scalar keys %source;
	print &DoOrDie ( "faddrow inDol=$hsh{$instr}{'filename'}+1 numrows=$numrows" );
	my $row = 1;
	foreach my $id ( sort { $source{"$a"}{"name"} cmp $source{"$b"}{"name"} } keys ( %source ) ) {

#	I did now also the OSA 7 processing for the Crab in the hard ISGRI band. Concerning JEM-X, Stephane confirmed that the count rates did not change. So all we have to do, is to update the ISGRI historic light curves. Looking at the ISGRI Crab count rate in OSA 7, it appears that the count rate is lower towards latest revolutions. Therefore, I suggest to apply the following conversion:
#	
#	                     OSA 5.1    OSA 7        factor
#	ISGRI 20-40 keV      109 cps    173 cps      1.5872
#	ISGRI 40-80 keV       62 cps     88 cps      1.4194
#	
#	Can you update the history files and let us know when we can switch?
#	At the same time, I'll create and updated version of the limit files.
#	It would be great if we could get this done next week.
#	
#	Cheers
#	Volker 

		$source{"$id"}{"max1"} *= $hsh{$instr}{'factor_bin1'};
		$source{"$id"}{"max2"} *= $hsh{$instr}{'factor_bin2'};

		printf OUTPUT "%20s %20s %15s %15s   %50s\n", $id, $source{"$id"}{"name"}, $source{"$id"}{"max1"}, $source{"$id"}{"max2"}, $source{"$id"}{"cmt"}; 
		print &DoOrDie ( "ftedit infile=$hsh{$instr}{'filename'}+1 column=SOURCE_ID      row=$row value=\"$id\"" );
		print &DoOrDie ( "ftedit infile=$hsh{$instr}{'filename'}+1 column=NAME           row=$row value=\"$source{$id}{'name'}\"" );
		print &DoOrDie ( "ftedit infile=$hsh{$instr}{'filename'}+1 column=COMMENTS       row=$row value=\"$source{$id}{'cmt'}\"" );
		print &DoOrDie ( "ftedit infile=$hsh{$instr}{'filename'}+1 column=RATE element=1 row=$row value=\"$source{$id}{'max1'}\"" );
		print &DoOrDie ( "ftedit infile=$hsh{$instr}{'filename'}+1 column=RATE element=2 row=$row value=\"$source{$id}{'max2'}\"" );
		$row++;
	}
	close OUTPUT;
}

print LOG "Closing : ",`date`;
close LOG;

exit;

######################################################################

sub DoOrDie {
	my ( $command ) = @_;
	my @result = `$command`;
	die ( "$command failed with $?" ) if ( $? );
	return @result;
}

