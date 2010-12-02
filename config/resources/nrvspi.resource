
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'NRT Rev processing:  SPI spectra'
PROCESS_NAME = nrvspi
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nrvspi >  !the OPUS method for invoking
                          !the process
COMMAND = nrvgen.pl        !the script (in your path) that
                          !runs this pipeline processC
CLASS = rev	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses one SPI spectra type trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
! First SPI AC OFF spectra files
OSF_TRIGGER1.DATA_ID = ssf  
OSF_TRIGGER1.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
! Then SPI AC On spectra files
OSF_TRIGGER2.DATA_ID = ssn 
OSF_TRIGGER2.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER2.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER2.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
! Then SPI PSD ACS analysis
OSF_TRIGGER3.DATA_ID = spa 
OSF_TRIGGER3.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER3.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER3.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
! Then SPI PSD performance analysis
OSF_TRIGGER4.DATA_ID = spp 
OSF_TRIGGER4.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER4.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER4.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
! Then SPI PSD efficiency analysis
OSF_TRIGGER5.DATA_ID = spe 
OSF_TRIGGER5.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER5.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER5.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
! Then SPI PSD sign analysis
OSF_TRIGGER6.DATA_ID = sps 
OSF_TRIGGER6.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER6.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER6.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
!	040625 - Jake - SPR 3734
! Then SPI 
!OSF_TRIGGER2.DATA_ID = ssn 
!OSF_TRIGGER2.RV      = w  !when OSF column DP = "w" (waiting)
!OSF_TRIGGER2.RS      = c  !and when OSF column YY = "c" (complete)
!OSF_TRIGGER2.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
!===========================================================================
!
! Error trapping uses OSF status;  
!
XPOLL_STATE.00 = OSF_OK
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
XPOLL_STATE.05 = OSF_HOLD ! put on hold
OSF_ERROR.RV = x           ! pipeline error
OSF_PROCESSING.RV    = p  !set this OSF stage upon being triggered
OSF_OK.RV            = c  ! Completed keyword generation
OSF_OK.RF            = w  ! next stage is 'wait'
OSF_HOLD.RV          = h  ! on hold
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
ENV.SPI_ACA_DELTA = .5   ! SPI time between ACA runs (in IJD days)
ENV.SPI_INT_TIME = .5     ! SPI time to integrate ACA results (in IJD days)
!
!  SPI PSD variables:  (also in nrvgen.resource, so update both!)
ENV.SPI_PSD_ADC_DELTA = 5400    ! seconds between spi_psd_adcgain analyses
ENV.SPI_PSD_EFFI_DELTA = 5400   ! seconds between spi_psd_efficiency analyses
ENV.SPI_PSD_PERF_DELTA = 10800  ! seconds between spi_psd_performance analyses
ENV.SPI_PSD_SI_DELTA = 10800    ! seconds between spi_psd_si analyses
ENV.SPI_PSD_LIMCHECK = yes       ! do limit checks?  Need structure update.
