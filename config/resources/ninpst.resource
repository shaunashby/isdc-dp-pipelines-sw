
!===========================================================================
!
SYSTEM = 'NRT Input Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Input Pipeline: Start'
PROCESS_NAME = ninpst			! 060110 - Jake - probably not necessary, but ...
!===========================================================================
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r ninpst >  !the OPUS method for invoking
                          !the process
COMMAND = ninpst.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = inp	!this is the process class for ease of sorting (inp,scw,qla,etc)
XPOLL_STATE.00 = FILE_SUCCESS
XPOLL_STATE.01 = FILE_ERROR

FILE_RANK = 1             !this value could change if multiple trigger
                          !types are used in combination
FILE_DIRECTORY1 = nrtinput_input   !incoming data found here.  Note this
                          ! this is a reference to the adp.path file
FILE_OBJECT1 = *.trigger     !trigger on files with this extension
FILE_PROCESSING = _work   ! Extension addition during processing
FILE_SUCCESS    = _processing   ! Extension addition if normal processing
FILE_ERROR      = _bad    ! Extension addition if error 
!MINBLOCKS = 100           ! bump this up as needed
!RESUME_BLOCKS = 100       ! bump this up as needed
ENV.SCWDIR = rii_scw          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here

