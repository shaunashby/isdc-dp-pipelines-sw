#!/usr/bin/perl

=head1 NAME

crvmon.pl - CONS Revolution Pipeline Monitor

=head1 SYNOPSIS

I<crvmon.pl> - Run from within B<OPUS>.  

=head1 DESCRIPTION

The Consolidated Rev File pipeline monitor, crvmon, contains then all the 
intelligence to control the flow of processing among Preproc, Input, Rev 
File, Science Window, Standard Science Window Analysis (SSA), Standard 
Mosaic Analysis (SMA) pipelines and their ingestion.

The existance of a RRRR_pp.done file triggers the checking the status of 
said revolution.  When the following files are found, crvmon takes the
noted action.  (Note:  These are essentially searched for in reverse order
so that the same action is not taken multiple times.)

B<RRRR_pp.done> - 

B<RRRR_inp.done> - Trigger REV processing.

B<RRRR_scwdp.started> - Check for completion of SCW DP.

B<RRRR_scwdp.done> - Trigger iii_prep

B<RRRR_iii_prep.trigger> - Wait for existance of rev.done.

B<RRRR_rev.done> - Trigger arc_prep

B<RRRR_arc_prep.trigger> - Wait for existance of arc.done.

B<RRRR_arc.done> - Trigger ingestion of REV and SCW output.

B<RRRR_ingest.done> - Trigger SSA processing.

B<RRRR_ssa.started> - Check for completion of SSA processing.

B<RRRR_ssa.done> - Trigger ingestion of SSA output.

B<RRRR_ssa_ingest.done> - Trigger SMA processing.	IGNORED

B<RRRR_sma.started> - Check for completion of SMA processing.

B<RRRR_sma.done> - Trigger ingestion of SMA output.

B<RRRR_sma_ingest.done> - Cleanup started.	IGNORED

It waits for the RRRR_pp.done file to be created, signaling PP completed
on a revolution and for all science windows to finish in the Input
pipeline.  Then it writes the RRRR_inp.done file and sets all the Rev OSFs
to cww to start them processing.  

It waits until the revolution files are all done and then writes the
RRRR_rev.done file and sets all the science window pipeline OSFs to cwhww
to start them processing.  

It waits until the science windows are all done and then writes the
RRRR_arc_prep.trigger to finish the revolution.  When that's done, it
cleans up.

Please note that there is no explicit conssma pipeline.  Here I treat it as a separate pipeline, but it is simply the consssa pipeline running mosaics.

=cut

use strict;
use warnings;

use File::Basename;
use ISDCPipeline;
use UnixLIB;
use OPUSLIB  qw(:osf_stati);
use TimeLIB;
use SSALIB;
use CRVLIB;

my $retval;
my @result;
my @list;

print "\n========================================================================\n";
&ISDCPipeline::EnvStretch("REV_INPUT","INP_INPUT","SCW_INPUT","SSA_INPUT","SMA_INPUT","ARC_TRIG_DONE","ARC_TRIG_INGESTING");
#&ISDCPipeline::EnvStretch("START_SSA", "START_SMA", "USING_AUTO_TRIGGERING");

if ( $ENV{CONSREV_UNIT_TEST} =~ /TRUE/ ) {
	$ENV{USING_AUTO_TRIGGERING} = 1;
	$ENV{AUTO_START_SSA}  = 1;
	$ENV{AUTO_START_SMA}  = 1;
	$ENV{AUTO_CLEAN_SSA}  = 1;
	$ENV{AUTO_CLEAN_SMA}  = 1;
   $ENV{START_LEVEL}     = "PP";
} 
elsif ( $ENV{CRV_SSA_UNIT_TEST} =~ /TRUE/ ) {
   $ENV{USING_AUTO_TRIGGERING} = 1;
   $ENV{AUTO_START_SSA}  = 1;
   $ENV{AUTO_START_SMA}  = 1;
   $ENV{AUTO_CLEAN_SSA}  = 1;
   $ENV{AUTO_CLEAN_SMA}  = 1;
   $ENV{START_LEVEL}     = "SSA";
}

print "\n\n"
	.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
	.">>>>>>>     ".&TimeLIB::MyTime()."     CHECKING status of data flow:\n"
	.">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";

######
#  Everything should get a pp.done first, so get those to compile list
#   of revs we're dealing with:
######
my @revs = sort(glob("$ENV{REV_INPUT}/*pp.done"));

print ">>>>>>>     Found the following revolutions to consider:\n", join("\n",@revs), "\n>>>>>>>\n";

=item Loop over all pp.done files

Now see what remains to be checked for that rev:

=over

=cut

foreach my $revno (@revs) {
	$revno = File::Basename::basename($revno);
	print "\n------------------------------------------\nEvaluating $revno.\n";
	$revno =~ s/^(\d{4})_pp\.done$/$1/;

#	Currently, .started and .done files are explicitly removed in 
#	pipeline_lib/cleanup.pl AND consrev/crvmon.pl::CheckClean()

#	Cleanup should remove: 
#		Data directory, 
#		possible link to data dir, 
#		obs/*OSF*, 
#		input/*trigger*, 
#		ARCHIVE:*trigger*, 
#		OPUS_WORK/pipeline/logs/*


	######################################################################

=item

If $ENV{REV_INPUT}/${revno}_sma.done (touched in CheckPipeline(SMA)) is there, then we can check if its ingested and begin ingest if its not and cleanup everything for all pipelines if it is.

=cut

	if ( -e "$ENV{REV_INPUT}/${revno}_sma.done" ) {
		print "$ENV{REV_INPUT}/${revno}_sma.done found.  Checking if SMA for $revno is ingested.\n";
		&CRVLIB::CheckIngested ( "conssma", "$revno", "sma_ingest.done" );	
		#	as triggers are created in a temporary location, this step requires manual intervention
		#	If all smas ingested, touch sma_ingest.done and run CheckClean
	}

	######################################################################

=item

If $ENV{REV_INPUT}/${revno}_sma.started (touched in CheckIngested(SSA)) is there, check if it has completed.

=cut

	elsif ( -e "$ENV{REV_INPUT}/${revno}_sma.started" ) {
		print "$ENV{REV_INPUT}/${revno}_sma.started found.  Checking if SMA for $revno is complete.\n";
		#	If all smas are complete, touch sma.done and ingest triggers
		&CRVLIB::CheckPipeline ( "conssma", "$revno", "$ENV{SMA_INPUT}", "$osf_stati{SMA_COMPLETE}", "sma.done" );
	}



=item

If $ENV{START_LEVEL} is set to "SMA", we start SSA.  (This may eventually be adjusted to start SMA)

=cut


	elsif ( $ENV{START_LEVEL} =~ /SMA/ ) {
#		print "touching $ENV{REV_INPUT}/${revno}_ssa.started";
#		`$mytouch "$ENV{REV_INPUT}/${revno}_ssa.started"`;
#		&CRVLIB::CheckIngested ( "consssa", "$revno", "ssa_ingest.done" );		
		&CRVLIB::StartSSA ( $revno );
	}




	######################################################################

=item

If $ENV{REV_INPUT}/${revno}_ssa.done (touched by CheckPipeline(SSA)), begin ingest or cleanup.

=cut

	elsif ( -e "$ENV{REV_INPUT}/${revno}_ssa.done" ) {
		if ( ( $ENV{AUTO_START_SMA} ) && ( $ENV{USING_AUTO_TRIGGERING} ) ) {
			print "$ENV{REV_INPUT}/${revno}_ssa.done found.  Checking if SSA for $revno is ingested.\n";
			#	as triggers are created in a temporary location, this step requires manual intervention
			#	If all ssas ingested, touch ssa_ingest.done and sma.started, and run cons_sma_start
			&CRVLIB::CheckIngested ( "consssa", "$revno", "ssa_ingest.done" );
		} else {
			# Now add final step for creation of input datasets needed for pixelisation:
			use File::Path qw(mkpath);
            use File::Copy qw(copy);
            
            my $imgfilename;
            my $jmxnumber=1;
            my $pixelsdir = 'pixels';
            
            # Where the tar will be written to:
            my $outbasedir = $ENV{REP_BASE_PROD}."/".$pixelsdir;          

            # Loop over instruments:
            for my $inst ('isgri','jmx') {
                my @imagefiles=();
                # Input dirs are obs_isgri, obs_jmx under REP_BASE_PROD.
                # If the directory exists, look for data:
                if ( -d  $ENV{REP_BASE_PROD}."/obs_$inst/${revno}.000" ) {
                	# Return if the *.done already exists for this revolution:
                	next if (-f "$ENV{REV_INPUT}/rev3_pixels_${inst}_${revno}.done");
                    
                    opendir(DIR,$ENV{REP_BASE_PROD}."/obs_$inst/${revno}.000");
                    # Get only dirs like ss* which exist for current instrument:
                    map {
                        chomp;
                        # Extract the SCW id:
                        my ($scwid) = ($_ =~ /^ss.*?_(\d*?)$/);
                        # For JMX dataset, get the number of JMX being used (1,2):
                        if ($inst =~/jmx/) {
                            ($jmxnumber) = ( $_ =~ /^ssj(\d).*?/);         
                            $imgfilename = "jmx${jmxnumber}_sky_ima.fits.gz";
                        } else {
                            $imgfilename = "isgri_sky_ima.fits.gz";
                        }
        
                        # Look for the image file:
                        my $ifile = $ENV{REP_BASE_PROD}."/obs_$inst/${revno}.000/".$_."/scw/".$scwid.".000/$imgfilename";
                        # If the file exists, copy it to the pixels directory for the current revolution:
                        if (-f $ifile) {        
                            # Create the output filename. Mangle using SCW id:      
                            my $outdir = $outbasedir."/$inst/$revno";
                            # Create output dir if it doesn't exist:
                            mkpath($outdir) unless (-d $outdir);
                            # Push the file onto the archive also, ready for writing later:
                            push(@imagefiles,"./ssa_${scwid}_000_$imgfilename");
                            # Copy image file to final location where we will create the archive:
                            copy($ifile,$outdir."/ssa_${scwid}_000_$imgfilename");              
                        } else {
                            print "Warning: Unable to find an image...looking for $ifile.\n";
                        }       
                    } grep { $_ =~ /^ss*/; } readdir(DIR);

                # Create a manifest which also serves as an input list for genpixels then add it to the archive:
                print "Creating MANIFEST.pixels for genpixels processing (rev3. pixelisation)\n";
                open(MANIFEST,"> $outbasedir/$inst/$revno/MANIFEST.pixels") or die "Unable to write manifest $outbasedir/$inst/$revno/MANIFEST.pixels: $!\n";
                print MANIFEST join("\n",@imagefiles)."\n";
                close(MANIFEST);
                # Add the manifest to image list:
                push(@imagefiles,"MANIFEST.pixels");
       
                # Create the tar file for the image files for this instrument only if the obs_XXX dir exists:
                chdir($outbasedir."/$inst/$revno");
                system($mytar,"-czf",$outbasedir."/${inst}-images-$revno.tgz",".");
                
                print "Created tar archive for image files for ${inst}, rev number ${revno}\n";
                print "--> $outbasedir/${inst}-images-$revno.tgz written.\n";
                print "Writing $ENV{REV_INPUT}/rev3_pixels_${inst}_${revno}.done (actually a copy of MANIFEST.pixels).\n";
                copy("$outbasedir/$inst/$revno/MANIFEST.pixels","$ENV{REV_INPUT}/rev3_pixels_${inst}_${revno}.done")
                }
            }			
			print "$ENV{REV_INPUT}/${revno}_ssa.done found, but not auto triggering.  Checking if $revno is clean\n";
			&CRVLIB::CheckClean( $revno );
		}
	}

	######################################################################

=item

If $ENV{REV_INPUT}/${revno}_ssa.started (touched by CheckPipeline (SCW)) exists, we check to see if it has completed.

=cut

	elsif ( ( -e "$ENV{REV_INPUT}/${revno}_ssa.started" ) && ( $ENV{USING_AUTO_TRIGGERING} ) ) {
		print "$ENV{REV_INPUT}/${revno}_ssa.started found.  Checking if SSA for $revno is complete.\n";
		#	If all ssas are complete, touch ssa.done and ingest triggers
		&CRVLIB::CheckPipeline ( "consssa", "$revno", "$ENV{SSA_INPUT}", "$osf_stati{SSA_COMPLETE}", "ssa.done" );
	}





=item

If $ENV{START_LEVEL} is set to SSA, we check to see if SCW has be ingested.

=cut

	elsif ( $ENV{START_LEVEL} =~ /SSA/ ) {
#		print "touching $ENV{REV_INPUT}/${revno}_ssa.started";
#		`$mytouch "$ENV{REV_INPUT}/${revno}_ssa.started"`;
		&CRVLIB::CheckIngested ( "consscw", "$revno", "ingest.done" );		
	}




	######################################################################

=item

If the arc.done (touched in nrtrev/Archiving.pm->RevArchiving which is called from nrtrev/nrvfin.pl if it is a arc_prep) is there, then we're just waiting for archive ingest.  Check whether it's done and we can clean the blackboards.

RevArchiving cleans up, write protects and creates ingest trigger (in temp location)

=cut

	elsif (-e "$ENV{REV_INPUT}/${revno}_arc.done") {
		if ( ( $ENV{AUTO_START_SSA} ) && ( $ENV{USING_AUTO_TRIGGERING} ) ) {
			print "$ENV{REV_INPUT}/${revno}_arc.done found.  Checking if $revno is ingested\n";
			#	as triggers are created in a temporary location, this step requires manual intervention
			#	If all scws ingested, touch ingest.done and ssa.started, and run cons_ssa_start
			&CRVLIB::CheckIngested ( "consscw", "$revno", "ingest.done" );		
		} else {
			print "$ENV{REV_INPUT}/${revno}_arc.done found, but not auto triggering.  Checking if $revno is clean\n";
			&CRVLIB::CheckClean( $revno );
		}
	}

	######################################################################

=item

If arc_prep (touched by CheckPipeline(consscw) after all science windows have completed processing) has already been triggered and is in process, then we do nothing.

=cut

	elsif (`$myls $ENV{REV_INPUT}/${revno}_arc_prep.trigger* 2> /dev/null`) {
		print ">>>>>>>     Found $ENV{REV_INPUT}/${revno}_arc_prep.trigger;  skipping.\n";
		next;		#	050126 - Jake - I don't really think that this "next" is necessary.
		#	UNLESS we put some code after the if and before the endfor (but then they may all need one)
	}

	######################################################################

=item

If there's a rev.done (touched at the very end of nrtrev/nrvgen.pl if ( ($dataset =~ /iii_prep/) && ($ENV{PATH_FILE_NAME} =~ /cons/) )), check Science Window pipeline for completion.

=cut

	elsif (-e "$ENV{REV_INPUT}/${revno}_rev.done") {
		print ">>>>>>>     Found $ENV{REV_INPUT}/${revno}_rev.done;  checking ScW pipeline\n";
		#	If all scws for this rev complete, touch arc_prep trigger
		&CRVLIB::CheckPipeline ( "consscw", "$revno", "$ENV{SCW_INPUT}", "$osf_stati{SCW_COMPLETE}", "arc_prep.trigger" );
	}

	######################################################################

=item

If iii_prep (touched by CheckPipeline(consscw/DP) when all science windows have finished the dp step) has already been triggered and is in process, then we do nothing.

=cut

	elsif (`$myls $ENV{REV_INPUT}/${revno}_iii_prep.trigger* 2> /dev/null`) {
		print ">>>>>>>     Found $ENV{REV_INPUT}/${revno}_iii_prep.trigger;  skipping.\n";
		next;		#	050126 - Jake - I don't really think that this "next" is necessary.
		#	UNLESS we put some code after the if and before the endfor (but then they may all need one)
	}

	######################################################################

=item

If there's a scwdp.started (touched by CheckPipeline(consrev) when all revolution files have processed), check Science Window pipeline

=cut

	elsif (-e "$ENV{REV_INPUT}/${revno}_scwdp.started") {
		print ">>>>>>>     Found $ENV{REV_INPUT}/${revno}_scwdp.started;  checking ScW pipeline DP step\n";
		#	If scwdp done for all scws in this rev, touch iii_prep trigger and mv scwdp.started to scwdp.done
		&CRVLIB::CheckPipeline ( "consscw", "$revno", "$ENV{SCW_INPUT}", "$osf_stati{SCW_DP_C_COR_H}", "iii_prep.trigger" );
	}

	######################################################################

=item

If there's a inp.done (touched by CheckPipeline(consinput) when all the science windows get through the Input pipeline), check Rev pipeline.

=cut

	elsif (-e "$ENV{REV_INPUT}/${revno}_inp.done") {
		print ">>>>>>>     Found $ENV{REV_INPUT}/${revno}_inp.done;  checking Rev pipeline\n";
		#	If rev pipeline done with all this rev, touch scwdp.started and change all scws to DP waiting for this rev
		&CRVLIB::CheckPipeline ( "consrev", "$revno", "$ENV{REV_INPUT}", "$osf_stati{REV_COMPLETE}", "scwdp.started" );
	}

	######################################################################

=item

Otherwise, if there's only the pp.done, check the Input pipeline:

pp.done is touched when Preproc is done

=cut

	else {
		print ">>>>>>>     Checking Input pipeline\n";
		#	If input done with all this rev, touch inp.done
		&CRVLIB::CheckPipeline ( "consinput", "$revno", "$ENV{INP_INPUT}", "$osf_stati{INP_COMPLETE}", "inp.done" );
	}
	
} # foreach pp.done file

exit 0;


##########################################################################

__END__ 

=back

=head1 RESOURCE FILE ENVIRONMENT ENTRIES 

=item B<>


=head1 REFERENCES

For further information on the other processes in this pipeline, please run perldoc on each, e.g. C<perldoc nrtdp.pl>.

For further information about B<OPUS> please see C<file:///isdc/software/opus/html/opusfaq.html> on the office network or C<file:///isdc/opus/html/opusfaq.html> on the operations network.  Note that understanding this document requires that you understand B<OPUS> first.

For further information about the NRT pipelines, please see the Top Level Architectural Design Document.

=head1 AUTHORS

Tess Jaffe <theresa.jaffe@obs.unige.ch>

Jake Wendt <jake.wendt@obs.unige.ch>

=cut

