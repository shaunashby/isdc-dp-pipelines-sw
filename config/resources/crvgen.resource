
!===========================================================================
!
SYSTEM = 'CONS Rev Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'CONS Rev processing:  Generic'
PROCESS_NAME = crvgen
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r crvgen >  !the OPUS method for invoking
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
!  JMX1 dumps files
OSF_TRIGGER1.DATA_ID = j1d  
OSF_TRIGGER1.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
!  JMX2 dumps files
OSF_TRIGGER2.DATA_ID = j2d  
OSF_TRIGGER2.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER2.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER2.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
!  JMX1 dfeedump files
OSF_TRIGGER3.DATA_ID = j1f
OSF_TRIGGER3.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER3.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER3.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
!  JMX2 dfeedump files
OSF_TRIGGER4.DATA_ID = j2f
OSF_TRIGGER4.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER4.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER4.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
! OMC bias calibration files
OSF_TRIGGER5.DATA_ID = obc
OSF_TRIGGER5.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER5.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER5.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
! OMC dark calibration files
OSF_TRIGGER6.DATA_ID = odc
OSF_TRIGGER6.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER6.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER6.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
! OMC flatfield calibration files
OSF_TRIGGER7.DATA_ID = ofc
OSF_TRIGGER7.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER7.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER7.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
! OMC sky calibration files
OSF_TRIGGER8.DATA_ID = osc
OSF_TRIGGER8.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER8.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER8.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!
!===========================================================================
!
! SPI dump files
OSF_TRIGGER9.DATA_ID = sdp
OSF_TRIGGER9.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER9.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER9.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!===========================================================================
!
!	040625 - Jake - SPR 3734
!		trigger numbers must be sequential and without gaps
!		arc was #10, sct was #25, commenting #10 out caused all after #10
!		to be ignored.
!
! SC TREF
OSF_TRIGGER10.DATA_ID = sct
OSF_TRIGGER10.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER10.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER10.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!===========================================================================
!
! PICsIT cal files (DP only file-by-file)
OSF_TRIGGER11.DATA_ID = prc  
OSF_TRIGGER11.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER11.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER11.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
!===========================================================================
!
! SPI acs spectra
OSF_TRIGGER12.DATA_ID = sac
OSF_TRIGGER12.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER12.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER12.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI AS dumps
OSF_TRIGGER13.DATA_ID = sas
OSF_TRIGGER13.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER13.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER13.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI DF dumps
OSF_TRIGGER14.DATA_ID = sdf
OSF_TRIGGER14.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER14.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER14.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI PD dumps
OSF_TRIGGER15.DATA_ID = spd
OSF_TRIGGER15.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER15.RS      = c  !and when OSF column YY = "c" (complete)
!
! IBIS TVER 
OSF_TRIGGER16.DATA_ID = itv
OSF_TRIGGER16.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER16.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER16.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! JEMX1 TVER 
OSF_TRIGGER17.DATA_ID = j1t
OSF_TRIGGER17.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER17.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER17.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! JEMX2 TVER 
OSF_TRIGGER18.DATA_ID = j2t
OSF_TRIGGER18.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER18.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER18.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! OMC TVER 
OSF_TRIGGER19.DATA_ID = omt
OSF_TRIGGER19.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER19.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER19.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! OMC dumps 
OSF_TRIGGER20.DATA_ID = omd
OSF_TRIGGER20.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER20.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER20.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI TVER
OSF_TRIGGER21.DATA_ID = stv
OSF_TRIGGER21.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER21.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER21.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI AS dump
OSF_TRIGGER22.DATA_ID = sas
OSF_TRIGGER22.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER22.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER22.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI DF dump
OSF_TRIGGER23.DATA_ID = sdf
OSF_TRIGGER23.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER23.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER23.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! SPI PD dump
OSF_TRIGGER24.DATA_ID = spd
OSF_TRIGGER24.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER24.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER24.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! 040624 - Jake - moving iii and arc to own resource file
! iii trigger (new SPI Calibration) 040526
!OSF_TRIGGER26.DATA_ID = iii
!OSF_TRIGGER26.RV      = w  !when OSF column DP = "w" (waiting)
!OSF_TRIGGER26.RS      = c  !and when OSF column YY = "c" (complete)
!OSF_TRIGGER26.CL      = _  !and when OSF column CL = "_" (not being cleaned)
!
! 040624 - Jake - moving iii and arc to own resource file
! Arc trigger
!OSF_TRIGGER10.DATA_ID = arc
!OSF_TRIGGER10.RV      = w  !when OSF column DP = "w" (waiting)
!OSF_TRIGGER10.RS      = c  !and when OSF column YY = "c" (complete)
!OSF_TRIGGER10.CL      = _  !and when OSF column CL = "_" (not being cleaned)
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
!
!  SPI PSD variables:  
ENV.SPI_PSD_ADC_DELTA = 5400    ! seconds between spi_psd_adcgain analyses
ENV.SPI_PSD_EFFI_DELTA = 5400   ! seconds between spi_psd_efficiency analyses
ENV.SPI_PSD_PERF_DELTA = 10800  ! seconds between spi_psd_performance analyses
ENV.SPI_PSD_SI_DELTA = 10800    ! seconds between spi_psd_si analyses
ENV.SPI_PSD_LIMCHECK = yes       ! do limit checks?  Need structure update.
ENV.REV_INPUT = rev_input       ! ScW pipeline input dir
