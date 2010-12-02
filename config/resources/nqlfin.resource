
!===========================================================================
!
SYSTEM = 'NRT Quick Look Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'NRT QLA Pipeline Finish ScW'
PROCESS_NAME = nqlfin
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r nqlfin >  !the OPUS method for invoking
                                        !the process
COMMAND = nqlfin.pl        !the script (in your path) that
                         !runs this pipeline process

CLASS = qla	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.FI      = w  !when OSF column FI = "w" (waiting)
OSF_TRIGGER1.QL      = c  !and when OSF column QL = "c" (complete)
!
!	testing using this step for both scw and obs
!
!OSF_TRIGGER1.DATA_ID = scw  !  when it's an scw type trigger (not obs)
!
!===========================================================================
!
! Error trapping uses trigger file extensions;  
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_ERROR.FI      = x   ! Extension addition if error
OSF_PROCESSING.FI    = p  ! set this OSF stage upon being triggered
OSF_OK.FI            = c  ! Completed keyword generation
!
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
ENV.SCWDIR = rii_scw
ENV.ALERTS = alerts         !  where to copy alerts
