
!===========================================================================
!
SYSTEM = 'NRT Quick Look Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'QLA Analysis Science Window'
PROCESS_NAME = nqlmon

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nqlmon >  !the OPUS method for invoking
                                          !the process
COMMAND = nqlmon.pl       !the script (in your path) that
                         !runs this pipeline process

CLASS = qla	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses a time trigger
!
TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:10:00   !trigger every 10 minutes from the initial 
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_STATE.01 = XPOLL_ERROR ! pipeline errors
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
!  Cleaning:  these are the age limits in days for cleaning OSFs
ENV.OSF_AGELIMIT_DEFAULT = 7    ! default (1 week)
ENV.OSF_AGELIMIT_ERRORS = 28    !  error OSFs (4 weeks)
ENV.MOSAICS = mosaics
