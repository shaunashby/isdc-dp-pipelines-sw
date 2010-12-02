
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'NRT Rev processing:  iii_prep'
PROCESS_NAME = nrviii
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nrviii >  !the OPUS method for invoking
                          !the process
COMMAND = nrvgen.pl        !the script (in your path) that
                          !runs this pipeline processC
CLASS = rev	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a trigger for each remaining data type except for spi_raw_?cur,
!	tver, tref, or omc_raw_dump, which have no processing whatsoever. 
!	This process does those which require an OBT calculation only.
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
!
!===========================================================================
!
! iii  trigger (new SPI Calibration)
OSF_TRIGGER1.DATA_ID = iii
OSF_TRIGGER1.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!===========================================================================
!
! Error trapping uses OSF status;  
!
XPOLL_STATE.00 = OSF_OK
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.RV = x           ! pipeline error
OSF_PROCESSING.RV    = p  !set this OSF stage upon being triggered
OSF_OK.RV            = c  ! Completed keyword generation
OSF_OK.RF            = w  ! next stage is 'wait'
XPOLL_ERROR.RV       = x  ! error
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
ENV.WORKDIR = rev_work        ! where we work
ENV.SCWDIR = rii_scw          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.ALERTS = alerts           ! where to copy alerts
ENV.DAL_OPEN_ALL = FALSE      ! default mode 
ENV.SCW_INPUT = scw_input       ! ScW pipeline input dir
ENV.IC_ALIAS = OSA9
ENV.ECS_DIR = OPUS_MISC_REP:ifts/outbox.tmp
ENV.ARC_TRIG = arcingest
ENV.PICSIT_ACA_DELTA = 90000 ! seconds included in PICsIT ACA analysis
ENV.SPI_PSD_ADC_DELTA = 5400    ! seconds between spi_psd_adcgain analyses
ENV.SPI_PSD_EFFI_DELTA = 5400   ! seconds between spi_psd_efficiency analyses
ENV.SPI_PSD_PERF_DELTA = 10800  ! seconds between spi_psd_performance analyses
ENV.SPI_PSD_SI_DELTA = 10800    ! seconds between spi_psd_si analyses
ENV.SPI_PSD_LIMCHECK = yes       ! do limit checks?  Need structure update.
ENV.REV_INPUT = rev_input		!	040622 - Jake
ENV.TIMEOUT = 7200 
