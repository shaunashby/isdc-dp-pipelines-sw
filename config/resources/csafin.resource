
!-------------------------------------------------------------------------
!
SYSTEM = 'CONS Standard Analysis Pipeline'
DISPLAY_ORDER=3
DESCRIPTION = 'Standard Analysis Pipeline Startup'
PROCESS_NAME = csafin
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r csafin >  !the OPUS method for invoking
                                        !the process
COMMAND = csafin.pl        !the script (in your path) that
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
OSF_TRIGGER1.FI      = w  !when OSF column FI = "w" (waiting)
OSF_TRIGGER1.SA      = c  !and when OSF column ST = "c" (complete)
OSF_TRIGGER1.CL      = _  !when OSF column CL = "_" (not modified to clean)
!
!  Second trigger for scw types in second loop (IBIS only)
OSF_RANK = 2              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER2.FI      = w  !when OSF column FI = "w" (waiting)
OSF_TRIGGER2.SA      = g  !and when OSF column ST = "c" (complete)
!
!
!OSF_TRIGGER1.DATA_ID = obs  !  when it's an obs type trigger (not scw)
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_STATE.05 = OSF_I    !  IBIS only, Loop I finished 
XPOLL_ERROR.FI      = x   ! Extension addition if error
OSF_PROCESSING.FI    = p  ! set this OSF stage upon being triggered
OSF_OK.FI            = c  ! Completed keyword generation
OSF_I.FI             = g   !  IBIS only, Loop I finished 
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
ENV.TIMEOUT = 0               ! don't time out anything;  gzip sometimes long
