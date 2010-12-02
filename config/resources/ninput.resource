
!===========================================================================
!
SYSTEM = 'NRT Input Pipeline'
DISPLAY_ORDER=2
DESCRIPTION = 'Input Pipeline: SPI processing'
PROCESS_NAME = ninput			! 060110 - Jake - probably not necessary, but ...
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r ninput >  !the OPUS method for invoking
                          !the process
COMMAND = ninput.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = inp	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single OSF trigger
!

OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.IN      = w  !when OSF column IN = "w" (waiting)
OSF_TRIGGER1.ST      = c  !and when OSF column YY = "c" (complete)
OSF_TRIGGER1.CL      = _  !and when OSF column CL = "_" (not being cleaned)
OSF_TRIGGER1.DATA_ID = inp !and the OSF data id = "zzz"...
OSF_PROCESSING.IN    = p  !set this OSF stage upon being triggered
!===========================================================================
!
! Error trapping uses OSF status; 
!
XPOLL_STATE.00 = OSF_OK   !maps a successful exit status 
XPOLL_STATE.01 = OSF_ERROR ! pipeline error
OSF_ERROR.IN = x           ! pipeline error (need this to keep log clean)
XPOLL_ERROR.IN       = x  ! unknown error error
OSF_OK.IN            = c  ! Completed keyword generation
OSF_OK.FI            = w  ! next stage is 'wait'
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
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.SCWDIR = rii_scw          ! where our data is
ENV.PARFILES = parfiles       ! where are pfiles are
ENV.WORKDIR = inp_work           ! scratch dir
ENV.ALERTS = nrt_alerts   

