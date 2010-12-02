
!===========================================================================
!
SYSTEM = 'NRT Rev Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'NRT Rev processing: Start'
PROCESS_NAME = nrvst
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
!TASK = < /usr/bin/nice -15 xpoll -p $PATH_FILE -r nrvst >  !the OPUS method for invoking
TASK = < xpoll -p $PATH_FILE -r nrvst >  !the OPUS method for invoking
                          !the process
COMMAND = nrvst.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = rev	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
! This uses a single file trigger
!
FILE_RANK = 1             !this value could change if multiple trigger
                          !types are used in combination
FILE_DIRECTORY1 = rev_input   !incoming data found here.  Note this
                          ! this is a reference to the nrt.path file
FILE_OBJECT1 = *.trigger  !trigger on files with this format
!===========================================================================
!
! Error trapping uses trigger file extensions;  
!
XPOLL_STATE.01 = FILE_ERROR      
XPOLL_STATE.00 = FILE_SUCCESS
XPOLL_STATE.05 = FILE_IGNORE     ! code for datasets not processed but
                                 ! instead set to done as below
FILE_PROCESSING = _work   ! Extension addition during processing
FILE_SUCCESS    = _processing   ! Extension addition if normal processing
ENV.XPOLL_ERROR      = _bad    ! Extension addition if error
FILE_IGNORE     = _done   ! ignored files turned to _done
FILE_ERROR       = _bad    ! Extension addition if error
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
ENV.ARC_TRIG = arcingest     ! archive ingest triggers
ENV.REV_INPUT = rev_input     ! input dir for changing trigger file
ENV.ALERTS = alerts           ! writing or copying alerts
ENV.SCW_WORK = scw_work       ! ScW pipeline workdir
ENV.SCW_INPUT = scw_input     ! ScW pipeline input dir
ENV.INP_WORK = inp_work       ! Input pipeline work dir

