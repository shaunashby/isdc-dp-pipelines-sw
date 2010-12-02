
!===========================================================================
!
SYSTEM = 'CONS SCW Pipeline'
DISPLAY_ORDER=6
DESCRIPTION = 'CONS Science Window Pipeline: OSM'
PROCESS_NAME = cswosm
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cswosm >  !the OPUS method for invoking
                          !the process
COMMAND = nswosm.pl       !the script (in your path) that
                          !runs this pipeline process
CLASS = scw	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.SO      = w  !when OSF column IC = "w" (waiting)
OSF_TRIGGER1.SC      = c  !and when OSF column DP = "c" (complete)
OSF_TRIGGER1.DATA_ID = scw !and the OSF data id = "zzz"...
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.SO = x           ! pipeline error (need this to keep log clean)
OSF_PROCESSING.SO    = p  !set this OSF stage upon being triggered
OSF_OK.SO            = c  ! Completed keyword generation
OSF_OK.SF            = w  ! next stage is 'wait'
XPOLL_ERROR.SO       = x  ! got an error
!===========================================================================
!
! These now cause the processes to stick in IOWAIT since new network
!  configuration was implemented.  
!
!MINBLOCKS = 100           ! bump this up as needed
!RESUME_BLOCKS = 100       ! bump this up as needed
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = work        ! where we work
ENV.SCWDIR = rii_scw          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.CFG_DIR = cfg_dir         ! Where the cfg files live
ENV.ALERTS = alerts       ! where to copy alerts
ENV.DAL_OPEN_ALL = FALSE      ! default mode 
ENV.REV_WORK = rev_work      ! rev workspace 
ENV.IC_ALIAS = OSA9
! moved ENV.JMX_ALERT_LEVEL to cswdp.resource 031230
!   for j_prp_verify
!ENV.JMX_ALERT_LEVEL = 2      !  for j_prp_verify executable
ENV.OSM_ACC_OVERLAP = 0      !  allows one second overlap of science windows
!  Use random number for ibis_isgr_energy?  Either 'y' or a number (2002.1017)
!  (Note that the 2002.1017 is hard-wired in opus_wrapper.)
!ENV.ISGRI_ENERGY_RANDOM = 2002.1017
ENV.ISGRI_ENERGY_RANDOM = y  
ENV.IOI_TIMEOUT = 7200  ! timeout for ibis_osm_isgr
