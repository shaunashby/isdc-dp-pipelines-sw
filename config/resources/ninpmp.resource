
!===========================================================================
!
SYSTEM = 'NRT Input Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Input Pipeline: Monitor PreProcessing'
PROCESS_NAME = ninpmp			! 060110 - Jake - probably not necessary, but ...
!===========================================================================
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r ninpmp >  !the OPUS method for invoking
                          !the process
!COMMAND = spvspp.opus        !the script (in your path) that
                          !runs this pipeline process
COMMAND = ninpmp.pl

CLASS = inp
XPOLL_STATE.00 = SUCCESS
XPOLL_STATE.01 = XPOLL_ERROR

XPOLL_ERROR_COUNT = 10   ! should stop after 10

TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:01:00   !trigger every 01 minutes from the initial 
                          !time at which the process is started
ENV.LOG_FILES = OPUS_MISC_REP:log/nrt_input     ! where do we write log files?

ENV.ALERTS = pp_alerts        ! where to write alerts
ENV.DELTA = 30                 ! time between multiple alerts (in minutes)
ENV.PARFILES = parfiles

