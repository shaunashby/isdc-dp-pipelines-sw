
!===========================================================================
!
SYSTEM = 'NRT Input Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Input Pipeline: Monitor Data Receipt'
PROCESS_NAME = ninpmc			! 060110 - Jake - probably not necessary, but ...
!===========================================================================
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r ninpmc >  !the OPUS method for invoking
                          !the process
COMMAND = ninpmc.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = inp
XPOLL_STATE.00 = SUCCESS
XPOLL_STATE.01 = XPOLL_ERROR

XPOLL_ERROR_COUNT = 10   !keep trying 

TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:10:00   !trigger every 10 minutes from the initial 
                          !time at which the process is started
!MINBLOCKS = 100           ! bump this up as needed
!RESUME_BLOCKS = 100       ! bump this up as needed
ENV.LOG_FILES = OPUS_MISC_REP:log/nrt_input     ! where do we write log files?
ENV.ALERTS = rttm_alerts      ! where to write alerts
ENV.PARFILES = parfiles
ENV.OSF_AGELIMIT_DEFAULT = 7  !  default 2 weeks (time enough for historic?)
ENV.OSF_AGELIMIT_ERRORS = 28   !  for errors, 4 weeks
