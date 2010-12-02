
!===========================================================================
!
SYSTEM = 'CONS SA/ScW Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'CONS SA/ScW Analysis Pipeline Finish ScW'
PROCESS_NAME = cssfin
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cssfin >  !the OPUS method for invoking
                                        !the process
COMMAND = cssfin.pl        !the script (in your path) that
                         !runs this pipeline process

CLASS = css	!this is the process class for ease of sorting (inp,scw,qla,etc)
!===========================================================================
!
!===========================================================================
!
! This uses a single OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.FI      = w  !when OSF column FI = "w" (waiting)
OSF_TRIGGER1.SA      = c  !and when OSF column SA = "c" (complete)
OSF_TRIGGER1.DATA_ID = scw  !  when it's an scw type trigger (not obs)
!
!===========================================================================
!
! Error trapping uses trigger file extensions;  
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_ERROR.FI      = x   ! Extension addition if error
OSF_PROCESSING.FI    = p  ! set this OSF stage upon being triggered
OSF_OK.FI            = c  ! Completed keyword generation
OSF_OK.CL            = o  ! Completed keyword generation
!
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = work            ! where we work
ENV.LOG_FILES = log_files     ! where do we write log files?
!ENV.PARFILES = parfiles       ! parfiles go here
ENV.INPUT = rii_input         ! input trigger directory
ENV.SCWDIR = rii_scw
ENV.ARC_TRIG = arcingest		! where ingest triggers are written

ENV.IC_ALIAS = OSA9


!===========================================================================
!
!	Advanced Processing Options (See consssa.path)
!
!
ENV.USELOCALDISKS = uselocaldisks
ENV.REDO_CORRECTION = redo_correction
ENV.OSA_VERSION = osa_version
ENV.OG_WRITE = og_write
