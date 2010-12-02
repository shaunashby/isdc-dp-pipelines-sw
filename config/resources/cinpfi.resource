
!===========================================================================
!
SYSTEM = 'CONS Input Pipeline'
DISPLAY_ORDER=6
DESCRIPTION = 'Input Pipeline: Finish'
PROCESS_NAME = cinpfi			! 060110 - Jake - probably not necessary, but ...
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cinpfi >  !the OPUS method for invoking
                          !the process
COMMAND = ninpfi.pl       !the script (in your path) that
                          !runs this pipeline process
CLASS = inp	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.FI      = w  !when OSF column XX = "w" (waiting)
OSF_TRIGGER1.IN      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.DATA_ID = inp  !and the OSF data id = "zzz"...
OSF_PROCESSING.FI    = p  !set this OSF stage upon being triggered
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status (00 in this 
                          !case) from the process
                          !so that XPOLL does not think the process
                          !encountered an error
OSF_OK.FI            = c  ! set this on ok exection.
XPOLL_ERROR.FI       = x
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
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.SCW_INPUT = scw_input     ! where we write a trigger.
ENV.PARFILES = parfiles       ! parfiles go here
ENV.SCWDIR = rii_scw          ! where our data is
