
!-------------------------------------------------------------------------
!
SYSTEM = 'CONS Standard Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'Standard Analysis Pipeline Startup'
PROCESS_NAME = csast
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r csast >  !the OPUS method for invoking
                                        !the process
COMMAND = csast.pl        !the script (in your path) that
                         !runs this pipeline process

CLASS = std	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses a single file trigger
!
FILE_RANK = 1             !this value could change if multiple trigger
                          !types are used in combination
FILE_DIRECTORY1 = rii_input   !incoming data found here.  Note this
                          ! this is a reference to the nrt.path file
FILE_OBJECT1 = *.trigger  !trigger on files with this extension
!===========================================================================
!
! Error trapping uses trigger file extensions;  
!
XPOLL_STATE.00 = FILE_SUCCESS
XPOLL_STATE.01 = FILE_ERROR ! pipeline errors
FILE_PROCESSING = _work   ! Extension addition during processing
FILE_SUCCESS    = _processing   ! Extension addition if normal processing
FILE_ERROR = _bad
ENV.XPOLL_ERROR      = _bad    ! Extension addition if error
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = work            ! where we work
ENV.OBSDIR = rii_obs          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.PARFILES = parfiles       ! parfiles go here
ENV.INPUT = rii_input         ! input trigger directory
