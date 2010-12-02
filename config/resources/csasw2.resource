
!-------------------------------------------------------------------------
!
SYSTEM = 'CONS Standard Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Standard Analysis Science Window Loop II'
PROCESS_NAME = csasw2

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r csasw2 >  !the OPUS method for invoking
                                          !the process
COMMAND = csascw.pl       !the script (in your path) that
                         !runs this pipeline process

CLASS = std	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses an OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.SA      = v  !when OSF column SA = "I" (loop 1 done;  IBIS only)
OSF_TRIGGER1.CL      = _  !when OSF column CL = "_" (not modified to clean)
OSF_TRIGGER1.DATA_ID = scw  !  when it's an scw type trigger (not obs)
OSF_TRIGGER1.DCF_NUMBER = IBI  !  IBIS only (just as safeguard)

!
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_ERROR.SA      = x   ! Extension addition if error
XPOLL_STATE.01 = OSF_ERROR ! pipeline errors
OSF_ERROR.SA   = x         ! pipeline errors
OSF_PROCESSING.SA    = p  ! set this OSF stage upon being triggered
OSF_OK.SA            = c  ! Completed 
OSF_OK.FI            = w  ! FI back to w (from o)
!
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = work            ! where we work
ENV.OBSDIR = rii_obs          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.INPUT = rii_input         ! input trigger directory
ENV.IC_ALIAS = OSA9
! when to timeout a process which may be hung in seconds (0 means never) 
ENV.TIMEOUT = 0
