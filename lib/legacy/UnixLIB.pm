package UnixLIB;

=head1 NAME

I<UnixLIB.pm> - library defining fixed global versions of executables as well as has wrappers for gzip and gunzip.

=head1 SYNOPSIS

use I<UnixLIB.pm>;

=head1 DESCRIPTION

=item

=head1 SUBROUTINES

=over

=cut

sub UnixLIB::Gunzip;
sub UnixLIB::Gzip;

use base qw(Exporter);
use vars qw($VERSION @EXPORT);

@EXPORT = qw ( 
	$mytar    
	$mycp     
	$myrm     
	$myln     
	$mychmod  
	$mymkdir  
	$mymv     
	$myls     
	$mygzip   
	$mygunzip 
	$myecho   
	$mytouch  
	$mycat    
	$mywc     
	$myawk    
	$mysed    
	$myfind   
	$mybc     
	$myhead     
	$mytail 
	$mygrep 
	$myw 
	$mydate 
	$myps 
	$myrsh 
	$myssh 
	$myptree 
	$mypstree 
	$myuname 
	);

$| = 1; #	disable output buffering

$root     = "/bin/";
$mytar    = $root."tar";
$mycp     = $root."cp";
$myrm     = $root."rm";
$myln     = $root."ln";
$mychmod  = $root."chmod";
$mymkdir  = $root."mkdir";
$mymv     = $root."mv";
$myls     = $root."ls";
$mygzip   = $root."gzip";
$mygunzip = $root."gunzip";
$myecho   = $root."echo";
$mytouch  = $root."touch";
$mycat    = $root."cat";
$myawk    = $root."awk";
$mysed    = $root."sed";
$mygrep   = $root."grep";
$mydate   = $root."date";		#	050412 - Jake - SCREW 1704
$myrsh    = $root."rsh";		#	050412 - Jake - SCREW 1704
$myuname  = $root."uname";		#	050414 - Jake

$root    = "/usr/bin/";			#	050301 - Jake - SCREW 1667
$myw      = $root."w";			#	050412 - Jake - SCREW 1704
$mywc     = $root."wc";
$mybc     = $root."bc";
$myfind   = $root."find";
$myhead   = $root."head";
$mytail   = $root."tail";
$myptree  = $root."ptree";		#	050414 - Jake - Solaris only
$mypstree = $root."pstree";	#	050414 - Jake - Linux only

$root    = "/usr/ucb/";			#	050916 - Jake
$myps     = $root."ps";			#	050916 - Jake

$myssh    = "ssh";				#	050802 - Jake - NO SCREW - NO CONSISTANT LOCATION


########################################################################### 

=item B<Gzip> ( @filelist )

Safety wrapper around the obvious executable, gzip.  Creates actual list by doing a glob on each item in the array, so therefore, will accept wildcards.  Checks to see if the .gz file exists already as well.

=cut

sub Gzip {
	my @filelist;
	my @errors;
	my $OLDHANDLE;
	my $logfile = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";

	if ( -w $logfile ) {
		open LOGFILE, ">> $logfile";
		$OLDHANDLE = select LOGFILE;
	}

	print ( "Running Gzip on \n\t".join("\n\t",@_)."\n" );

	foreach ( @_ ) {
		#  this allows UnixLIB::Gunzip ( "somedir/*" );
		#	the if ( $_ ) filters out empty entries, but NOT " "
		push @filelist, glob ( $_ ) if ( $_ );
	}

	foreach my $file ( @filelist ) {
		next unless ( $file );	#	shouldn't be an issue bc of the previous filtering
		if ( -e $file ) {
			unless ( -e "$file.gz" ) {
				`$mygzip $file`;
				if ( $? ) {
					print "$mygzip $file failed\n";
					push @errors, $file;
				}
			} else {
				print "$file.gz already exists\n";
				push @errors, $file;
			}
		} else {
			print "$file does not exist\n";
			push @errors, $file;
		}
	}

	die ( "There was a problem gzipping the following files:\n\t".join("\n\t",@errors) )if ( @errors );

	if ( -w $logfile ) {
		close LOGFILE;
		select $OLDHANDLE;
	}

}

########################################################################### 

=item B<Gunzip> ( @filelist )

Safety wrapper around the obvious executable, gunzip.  Creates actual list by doing a glob on each item in the array, so therefore, will accept wildcards.  Checks to see if the gunzipped file exists already as well.

=cut

sub Gunzip {
	my @filelist;
	my @errors;
	my $OLDHANDLE;
	my $logfile = "$ENV{LOG_FILES}/$ENV{OSF_DATASET}.log";

	if ( -w $logfile ) {
		open LOGFILE, ">> $logfile";
		$OLDHANDLE = select LOGFILE;
	}

	print ( "Running Gunzip on \n\t".join("\n\t",@_)."\n" );

	foreach ( @_ ) {
		#  this allows UnixLIB::Gunzip ( "somedir/*" );
		#	the if ( $_ ) filters out empty entries, but NOT " "
		push @filelist, glob ( $_ ) if ( $_ );
	}

	foreach my $file ( @filelist ) {
		next unless ( $file );	#	shouldn't be an issue bc of the previous filtering
		$file =~ s/.gz$//;	#	strip off .gz to get base file name

		if ( -e "$file.gz" ) {
			unless ( -e $file ) {
				`$mygunzip $file.gz`;
				if ( $? ) {
					print "$mygunzip $file.gz failed\n";
					push @errors, $file;
				}
			} else {
				print "$file exists already\n";
				push @errors, $file;
			}
		} else {
			print "$file.gz does not exist\n";
			push @errors, $file;
		}
	}

	die ( "There was a problem gunzipping the following files:\n\t".join("\n\t",@errors) ) if ( @errors );
	if ( -w $logfile ) {
		close LOGFILE;
		select $OLDHANDLE;
	}
}

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
