
!===========================================================================
!
SYSTEM = 'CONS SA/ScW Analysis Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'CONS SA/ScW Analysis'
PROCESS_NAME = cssscw

!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r cssscw >  !the OPUS method for invoking
                                          !the process
COMMAND = cssscw.pl       !the script (in your path) that
                         !runs this pipeline process

CLASS = css 	!this is the process class for ease of sorting (inp,scw,qla,etc)
!
!===========================================================================
!
! This uses an OSF trigger
!
OSF_RANK = 1              !this value could change if multiple trigger
                          !types are used in combination
OSF_TRIGGER1.ST      = c  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.SA      = w  !when OSF column SA = "w" (waiting)
OSF_TRIGGER1.DATA_ID = scw  !  when it's an scw type trigger (not obs)
!
!===========================================================================
!
! Error trapping uses OSF status:
!
XPOLL_STATE.00 = OSF_OK   ! All ran sucessfully
XPOLL_ERROR.SA      = x   ! Extension addition if error
XPOLL_STATE.01 = OSF_ERROR ! pipeline errors
OSF_ERROR.SA   = x         ! pipeline errors
OSF_PROCESSING.SA    = p  ! set this OSF stage upon being triggered
OSF_OK.SA            = c  ! Completed 
!===========================================================================
!
!  Variables used in process script.  
!
ENV.OUTPATH = rii             ! repository 2
ENV.WORKDIR = work            ! where we work
ENV.LOG_FILES = log_files     ! where do we write log files?
!ENV.PARFILES = parfiles       ! parfiles go here
ENV.INPUT = rii_input         ! input trigger directory
ENV.IC_ALIAS = OSA9
ENV.TIMEOUT = 0

!===========================================================================
!
!	Advanced Processing Options (See consssa.path)
!
!
ENV.USELOCALDISKS = uselocaldisks
ENV.REDO_CORRECTION = redo_correction
ENV.CREATE_REV_3    = create_rev_3
ENV.OSA_VERSION = osa_version
ENV.MULTISTAGE = multistage
ENV.OG_TOOL = og_tool
ENV.OG_READ = og_read
ENV.OG_WRITE = og_write
ENV.ISGRI_STARTLEVEL = isgri_startLevel
ENV.ISGRI_ENDLEVEL = isgri_endLevel

