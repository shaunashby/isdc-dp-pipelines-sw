
!===========================================================================
!
CLASS = adp
SYSTEM = 'ADP Pipeline'
DISPLAY_ORDER=6
DESCRIPTION = 'ADP Pipeline: Finish'
PROCESS_NAME = adpfin						! 060110 - Jake - probably not necessary, but ...

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r adpfin >  !the OPUS method for invoking
                          !the process
COMMAND = adpfin.pl       !the script (in your path) that
                          !runs this pipeline process
!===========================================================================
!
! This uses one OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.FI      = w  !when OSF column XX = "w" (waiting)
OSF_TRIGGER1.AD      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  
!
!===========================================================================
!
! Error trapping uses OSF status;  
!
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.FI = e           ! pipeline error
OSF_PROCESSING.FI    = p  !set this OSF stage upon being triggered
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
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = adp_work        ! where we work
ENV.AUXDIR = rii_aux          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles
