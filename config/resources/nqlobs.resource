
!===========================================================================
!
SYSTEM = 'NRT Quick Look Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'QLA Analysis Science Window (Mosaic)'
PROCESS_NAME = nqlobs

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nqlobs >  !the OPUS method for invoking
                                          !the process
COMMAND = nqlobs.pl       !the script (in your path) that
                         !runs this pipeline process

CLASS = qla	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses an OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.ST      = c  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.QL      = w  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.DATA_ID = obs  !  when it's an obs type trigger (not scw)
!
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_ERROR.QL      = x   ! Extension addition if error
XPOLL_STATE.01 = OSF_ERROR ! pipeline errors
OSF_ERROR.QL   = x         ! pipeline errors
OSF_PROCESSING.QL    = p  ! set this OSF stage upon being triggered
OSF_OK.QL            = c  ! Completed 
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
!ENV.IC_ALIAS = IC_6.0
ENV.IC_ALIAS = OSA9 
!ENV.IC_ALIAS = OSA9
ENV.TIMEOUT = 57600				!	14400 was too small for some obs, 28800 to small for some as well
