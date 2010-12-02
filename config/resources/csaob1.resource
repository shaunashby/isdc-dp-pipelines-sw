
!-------------------------------------------------------------------------
!
SYSTEM = 'CONS Standard Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Standard Analysis Observation'
PROCESS_NAME = csaob1

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r csaob1 >  !the OPUS method for invoking
                                          !the process
COMMAND = csaobs.pl       !the script (in your path) that
                         !runs this pipeline process

CLASS = std	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses five OSF triggers
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.ST      = c  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.SA      = w  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.CL      = _  !when OSF column CL = "_" (not modified to clean)
OSF_TRIGGER1.DATA_ID = obs  !  when it's an obs type trigger (not scw)
!
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully (Loop II done for IBIS)
XPOLL_STATE.05 = OSF_I    ! Loop I done (IBIS only)
XPOLL_ERROR.SA      = x   ! Extension addition if error
XPOLL_STATE.01 = OSF_ERROR ! pipeline errors
OSF_ERROR.SA   = x         ! pipeline errors
OSF_PROCESSING.SA    = p  ! set this OSF stage upon being triggered
OSF_OK.SA            = c  ! Completed 
OSF_I.SA            = s  ! Loop one done for IBIS only;  
OSF_OK.FI            = w  ! Set finish to wait
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
