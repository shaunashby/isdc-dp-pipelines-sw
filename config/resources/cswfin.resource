
!===========================================================================
!
SYSTEM = 'CONS SCW Pipeline'
DISPLAY_ORDER=7
DESCRIPTION = 'CONS Science Window Pipeline: Finish'
PROCESS_NAME = cswfin
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cswfin >  !the OPUS method for invoking
                          !the process
COMMAND = nswfin.pl       !the script (in your path) that
                          !runs this pipeline process
CLASS = scw	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.SF      = w  !when OSF column XX = "w" (waiting)
OSF_TRIGGER1.SO      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.DATA_ID = scw  !and the OSF data id = "zzz"...
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status (00 in this 
                          !case) from the process
                          !so that XPOLL does not think the process
                          !encountered an error
XPOLL_STATE.01 = OSF_ERROR
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.SF = x           ! pipeline error (need this to keep log clean)
OSF_PROCESSING.SF    = p  !set this OSF stage upon being triggered
OSF_OK.SF            = c  ! set this on ok exection.
OSF_ERROR.SF         = x  ! error
XPOLL_ERROR.SF       = x
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
ENV.ARC_TRIG = arcingest   ! triggers for archive ingest
ENV.ALERTS = alerts    ! copy alerts to central dir
ENV.DAL_OPEN_ALL = FALSE      ! default mode 
ENV.SCW_INPUT = rii_input         ! trigger file location
