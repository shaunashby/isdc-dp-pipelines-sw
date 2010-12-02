
!===========================================================================
!
CLASS = adp
SYSTEM = 'ADP Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'ADP Pipeline: Data Preperation'
PROCESS_NAME = adp						! 060110 - Jake - probably not necessary, but ...

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r adp >  !the OPUS method for invoking
                          !the process
COMMAND = adp.pl        !the script (in your path) that
                          !runs this pipeline process
!===========================================================================
!
! This uses one OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.AD      = w  !when OSF column AD = "w" (waiting)
OSF_TRIGGER1.ST      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  
!
!===========================================================================
!
! Error trapping uses OSF status;  
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status 
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
XPOLL_STATE.03 = OSF_DONE ! prematurely set it to ccc
XPOLL_STATE.05 = OSF_DELETE ! prematurely set it to cc_d
OSF_ERROR.AD = x           ! pipeline error
XPOLL_ERROR.AD       = x  ! got an error
OSF_PROCESSING.AD    = p  !set this OSF stage upon being triggered
OSF_OK.AD            = c  ! Completed keyword generation
OSF_OK.FI            = w  ! next stage is 'wait'
OSF_DONE.AD = c  
OSF_DONE.FI = c    !  Set OSF to done, skipping finish step
OSF_DELETE.FI = _  !  Trigger for deletion of OSF (here just placeholder)
OSF_DELETE.CL = d  !  Trigger for deletion of OSF
OSF_DELETE.AD = c


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
ENV.ADP_INPUT = adp_input      ! where the input comes from
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = adp_work        ! where we work
ENV.ALERTS = alerts           ! where the alerts go
ENV.AUXDIR = rii_aux          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.ARC_TRIG = arcingest      ! triggers for archive ingsest
ENV.SCID = 121                ! convertorbit parameter
ENV.ALERT_FILTER_DURATION = 3599.999999  ! seconds for filtering alerts (createpdef)
ENV.TSF_TOLERANCE = 30         ! tolerance between IJD and revolution number
ENV.OLF_TOLERANCE = 5         !  in convertprogram executable.
ENV.ORB_TOLERANCE = 0.005     !  orbita data tolerance

ENV.TIME_CORRELATION_MIN_OK = 16000	!	the minimum acceptable number of TIME_CORRELATION records for a revolution

