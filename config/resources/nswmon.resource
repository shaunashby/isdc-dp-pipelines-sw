
!===========================================================================
!
SYSTEM = 'NRT SCW Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'NRT Science Window Pipeline: Data Preparation'
PROCESS_NAME = nswmon
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nswmon >  !the OPUS method for invoking
                          !the process
COMMAND = nswmon.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = scw	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a time trigger 
TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:10:00   !trigger every 10 minutes from the initial 
!
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status
XPOLL_STATE.01 = XPOLL_ERROR ! pipeline error
XPOLL_ERROR_COUNT = 10   !keep trying up to 10 times

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
ENV.ALERTS = alerts       ! where to copy alerts
!  Cleaning:  these are the age limits in days for cleaning OSFs
ENV.OSF_AGELIMIT_DEFAULT = 7    ! default (1 week)
ENV.OSF_AGELIMIT_ERRORS = 28    !  error OSFs (4 weeks)
