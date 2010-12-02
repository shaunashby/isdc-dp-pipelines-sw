
!===========================================================================
!
SYSTEM = 'CONS SCW Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'CONS Science Window Pipeline: Data Preparation'
PROCESS_NAME = cswdp
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cswdp >  !the OPUS method for invoking
                          !the process
COMMAND = nswdp.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = scw	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.SD      = w  !when OSF column DP = "w" (waiting)
OSF_TRIGGER1.SS      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.DATA_ID = scw !and the OSF data id = "zzz"...
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status 
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.SD = x           ! pipeline error (need this to keep log clean)
XPOLL_ERROR.SD       = x  ! got an error
OSF_PROCESSING.SD    = p  !set this OSF stage upon being triggered
OSF_OK.SD            = c  ! Completed keyword generation
!	040623 - Jake - SPR 3715
!	due to the fact that the status of SC is dependent, 
!	it cannot be hard coded here.
!OSF_OK.SC            = h  ! next stage is 'hold'	!	040616 - Jake - SCREW 1347 - do not trigger the next step here
!OSF_OK.SC            = w  ! next stage is 'wait'
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
ENV.CFG_DIR = cfg_dir 	  ! Where the cfg files live
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.ALERTS = alerts       ! where to copy alerts
ENV.DAL_OPEN_ALL = FALSE      ! default mode 
ENV.IC_ALIAS = OSA9
ENV.ALERT_LEVEL = 2 ! for o_prp_shot_plan only
ENV.PV_ALLOW_AUX_ERR = TRUE   !  Allows errors from dp_aux_attr
ENV.SPI_SPEC_OBT_ACCURACY = ANY    !  dal3aux accuracy.
! moved ENV.JMX_ALERT_LEVEL from cswosm.resource 031230
ENV.JMX_ALERT_LEVEL = 2      !  for j_prp_verify executable
ENV.REV_INPUT = rev_input    ! input trigger directory (040608 - Jake - SCREW 1347)
