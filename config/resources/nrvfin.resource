
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=6
DESCRIPTION = 'NRT Rev Pipeline: Finish'
PROCESS_NAME = nrvfin
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nrvfin >  !the OPUS method for invoking
                          !the process
COMMAND = nrvfin.pl       !the script (in your path) that
                          !runs this pipeline process
CLASS = rev	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses one OSF trigger for all types of rev file
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
!
!  IBIS dumps trigger first
OSF_TRIGGER1.DATA_ID = idp  
OSF_TRIGGER1.RF      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.RV      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! then noise maps
OSF_TRIGGER2.DATA_ID = irn 
OSF_TRIGGER2.RF      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER2.RV      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER2.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!  IBIS ILTs trigger next
OSF_TRIGGER3.DATA_ID = ilt  
OSF_TRIGGER3.RF      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER3.RV      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER3.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!  then everything else in order
OSF_TRIGGER4.RF      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER4.RV      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER4.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!===========================================================================
!
! Error trapping uses OSF status;  
!
OSF_PROCESSING.RF    = p  !set this OSF stage upon being triggered
XPOLL_STATE.00 = OSF_OK   !maps a successful
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.RF         = x  ! pipeline error
OSF_OK.RF            = c  ! set this on ok exection.
XPOLL_ERROR.RF       = x
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
ENV.WORKDIR = rev_work        ! where we work
ENV.SCWDIR = rii_scw          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.REV_INPUT = rev_input     ! input trigger to move to _done
ENV.DAL_OPEN_ALL = FALSE      ! default mode 
ENV.ARC_TRIG = arcingest
ENV.SCW_WORK = scw_work
ENV.SCW_INPUT = scw_input     ! where ScW triggers go
ENV.IDX_WAIT = 300            ! time to wait for cleaing indices, in seconds
ENV.ALERTS = alerts

