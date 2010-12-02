
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=7
DESCRIPTION = 'NRT Rev: Monitor'
PROCESS_NAME = nrvmon						! 060110 - Jake - probably not necessary, but ...

!===========================================================================

POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nrvmon >  !the OPUS method for invoking
                          !the process
COMMAND = nrvmon.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = rev 
XPOLL_STATE.00 = SUCCESS
XPOLL_STATE.01 = XPOLL_ERROR

XPOLL_ERROR_COUNT = 10   !keep trying  up to 10 times

TIME_RANK = 1            !this value could change if multiple trigger 
                         !types are used in combination
DELTA_TIME = 000:00:10:00   !trigger every 10 minutes from the initial 
!DELTA_TIME = 000:00:01:00   ! should be 10 minutes; 1 for testing
                          !time at which the process is started
!MINBLOCKS = 100           ! bump this up as needed
!RESUME_BLOCKS = 100       ! bump this up as needed
!
ENV.OUTPATH = rii             ! repository 2
ENV.SCWDIR = rii_scw          ! where our data is
ENV.WORKDIR = rev_work
ENV.PARFILES = parfiles
ENV.REV_INPUT = rev_input     ! input dir for changing trigger file
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.REV_INPUT = rev_input     ! input trigger to move to _done
ENV.SCW_INPUT = scw_input     ! where ScW triggers go
! 
!  Archiving triggers:
ENV.ARC_TRIG = arcingest               ! where the pipeline writes them
ENV.ARC_TRIG_INGESTING = arcingesting  ! where archingest gets them
ENV.ARC_TRIG_DONE = archingest_done    ! where they're moved when ingested 
!
ENV.ILT_WAIT =  2820           ! minutes between IBIS dump and ILT run 
										!  (1800 min == 30 hrs;  SCREW 909)
										!  (2400 min == 40 hrs;  SCREW 1122)
										!  (2520 min == 42 hrs;  SPR 3036)
										!  (3120 min == 52 hrs;  SCREW 1122)
										!	(2820 min == 47 hours; SPR 3919)
ENV.SPI_PSD_ADC_DELTA = 5400    ! seconds between spi_psd_adcgain analyses
ENV.SPI_PSD_EFFI_DELTA = 5400   ! seconds between spi_psd_efficiency analyses
ENV.SPI_PSD_PERF_DELTA = 10800  ! seconds between spi_psd_performance analyses
ENV.SPI_PSD_SI_DELTA = 10800    ! seconds between spi_psd_si analyses
!  Cleaning:  these are the age limits in days for cleaning OSFs
ENV.OSF_AGELIMIT_DEFAULT = 7  ! default (1 week)
ENV.OSF_AGELIMIT_ERRORS = 28    !  error OSFs (4 weeks)
ENV.OSF_AGELIMIT_ARCHIVED = 7    !  archived OSFs after 4 week
!
! Allowed number of errors in percentage of total number of OSFs for archiving:
ENV.ARC_ALLOW_ERR_PERCENT_REV = 0.05       !  5 % by default
ENV.ARC_ALLOW_ERR_PERCENT_SCW = 0.05       !  5 % by default
