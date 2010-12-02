
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'NRT Rev processing:  IBIS dump'
PROCESS_NAME = nrvidp
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nrvidp >  !the OPUS method for invoking
                          !the process
COMMAND = nrvgen.pl        !the script (in your path) that
                          !runs this pipeline processC
CLASS = rev	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses one IBIS dump type trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
! First IBIS dump files
OSF_TRIGGER1.DATA_ID = idp  
OSF_TRIGGER1.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
! 
! Then ILT triggers
OSF_TRIGGER2.DATA_ID = ilt
OSF_TRIGGER2.RV      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER2.RS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER2.CL      = _  !and when OSF column CL = "_" (not being cleaned)
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
ENV.REV_INPUT = rev_input       ! ScW pipeline input dir
ENV.IC_ALIAS = OSA9
ENV.PICSIT_ICA_DELTA = 259200  ! seconds to include in PICsIT ICA analysis
!
!  Comment out the following line if ILT generation should *not* send an alert:
ENV.ILT_ALERT = 2             !  alert level to send on ILT generation.  
!  Parameters to ibis_isgr_low_thres:
ENV.ILT_RATIO_DEAD = 1.9			!  ratioDEAD param of ibis_isgr_low_thres
ENV.ILT_RATIO_PEAK = 0.042			!  ratioPEAK param of ibis_isgr_low_thres	- 20090710 from 0.045 to 0.042, ibis_isgr_low_thresh 5.6
ENV.ILT_RATIO_EFFIC = 0.75			!  ratioEffic param of ibis_isgr_low_thres
ENV.ILT_REVOL_WAIT = 7				!	20091210 MB Version 6.1 SCREW 2176
!  Compare context with ground (IC) version?  Either 1,2,3 for corresponding
!   alert levels, or 0 (or comment out) for no checking.  DON'T leave it blank.
ENV.ISGR_GRNDCTXT_COMP = 2
ENV.PICS_GRNDCTXT_COMP = 2	!  SCREW 1023 set to 2
ENV.HEPI_GRNDCTXT_COMP = 2      ! 
ENV.VETO_GRNDCTXT_COMP = 2      ! 
ENV.IASW_GRNDCTXT_COMP = 2      !  
ENV.PICS_DUMP_EMAIL = larosa@pa.iasf.cnr.it ! email of IC responsible
ENV.VETO_DUMP_EMAIL = larosa@pa.iasf.cnr.it ! email of IC responsible
ENV.HEPI_DUMP_EMAIL = larosa@pa.iasf.cnr.it ! email of IC responsible
ENV.ISGR_DUMP_EMAIL = larosa@pa.iasf.cnr.it ! email of IC responsible
ENV.IASW_DUMP_EMAIL = larosa@pa.iasf.cnr.it ! email of IC responsible
ENV.TPF_DIR     = OPUS_MISC_REP:ifts/outbox.tmp
ENV.IFTS_OUTBOX = OPUS_MISC_REP:ifts/outbox
