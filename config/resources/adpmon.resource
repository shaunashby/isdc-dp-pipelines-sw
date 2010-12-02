
!===========================================================================
!
CLASS = adp
SYSTEM = 'ADP Pipeline'
DISPLAY_ORDER=4
DESCRIPTION = 'ADP Pipeline: Monitor'
PROCESS_NAME = adpmon
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r adpmon >  !the OPUS method for invoking
                          !the process
COMMAND = adpmon.pl        !the script (in your path) that
                          !runs this pipeline process
!===========================================================================
!
! This uses a time trigger
!
TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:10:00   !trigger every 10 min from the initial 
                          !time at which the process is started
!
!===========================================================================
!
! Error trapping;  no trigger file or OSF, so can only count errors and 
!  go absent after too many.  
!
XPOLL_STATE.00 = SUCCESS

XPOLL_ERROR_COUNT = 10    !go absent after on error
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = adp_work        ! working directory
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.ADP_INPUT = adp_input     ! where the input files are.
ENV.ALERTS = alerts           ! where to write alerts of odd files
!  Cleaning:  these are the age limits in days for cleaning OSFs
ENV.OSF_AGELIMIT_DEFAULT = 14  !  default 2 weeks (time enough for historic?)
ENV.OSF_AGELIMIT_ARCHIVED = 28 !  for completed archive triggers, 4 weeks
ENV.OSF_AGELIMIT_THF = 2       !  no reason to keep many THF OSFs around
ENV.PARFILES = parfiles
