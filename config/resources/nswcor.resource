
!===========================================================================
!
SYSTEM = 'NRT SCW Pipeline'
DISPLAY_ORDER=5
DESCRIPTION = 'NRT Science Window Pipeline: COR'
PROCESS_NAME = nswcor
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nswcor >  !the OPUS method for invoking
                          !the process
COMMAND = nswcor.pl       !the script (in your path) that
                          !runs this pipeline process
CLASS = scw	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.SC      = w  !when OSF column IC = "w" (waiting)
OSF_TRIGGER1.SD      = c  !and when OSF column DP = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
OSF_TRIGGER1.DATA_ID = scw !and the OSF data id = "zzz"...
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status 
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.SC = x           ! pipeline error
OSF_PROCESSING.SC    = p  !set this OSF stage upon being triggered
OSF_OK.SC            = c  ! Completed keyword generation
OSF_OK.SO            = w  ! next stage is 'wait'
XPOLL_ERROR.SC       = x  ! got an error
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
ENV.REV_INPUT = rev_input
ENV.IC_ALIAS = OSA9		! IC_7.0
ENV.OSM_ACC_OVERLAP = 1      !  allows one second overlap of science windows
ENV.IOI_TIMEOUT = 7200  ! timeout for ibis_osm_isgr
