
!===========================================================================
!
SYSTEM = 'ADP Pipeline'
DISPLAY_ORDER=1
DESCRIPTION = 'ADP Pipeline: Start'
PROCESS_NAME = adpst
!===========================================================================
!
! XPOLL configuration stuff
!
POLLING_TIME = 60         !seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r adpst >  !the OPUS method for invoking
                          !the process
COMMAND = adpst.pl        !the script (in your path) that
                          !runs this pipeline process
CLASS = adp
!===========================================================================
!
! This uses a single file trigger
!
FILE_RANK = 1             !this value could change if multiple trigger
                          !types are used in combination
FILE_DIRECTORY1 = adp_input   !incoming data found here.  Note this
                          ! this is a reference to the adp.path file
FILE_OBJECT1 = *.fits     !trigger on files with this extension
FILE_DIRECTORY2 = adp_input   !incoming data found here.  
FILE_OBJECT2 = *.PAF      !trigger on files with this extension
FILE_DIRECTORY3 = adp_input   !incoming data found here.  
FILE_OBJECT3 = *.ASF      !trigger on files with this extension
FILE_DIRECTORY4 = adp_input   !incoming data found here.  
FILE_OBJECT4 = *.AHF      !trigger on files with this extension
FILE_DIRECTORY5 = adp_input   !incoming data found here.  
FILE_OBJECT5 = *.INT      !trigger on files with this extension
FILE_DIRECTORY6 = adp_input   !incoming data found here.  
FILE_OBJECT6 = *.OLF      !trigger on files with this extension
FILE_DIRECTORY7 = adp_input   !incoming data found here.  
FILE_OBJECT7 = opp*.tar      !trigger on files with this extension
FILE_DIRECTORY8 = adp_input   !incoming data found here.  
FILE_OBJECT8 = *.orbita      !trigger on files with this extension
FILE_DIRECTORY9 = adp_input   !incoming data found here.  
FILE_OBJECT9 = *.revno      !trigger on files with this extension
FILE_DIRECTORY10 = adp_input
FILE_OBJECT10 = *.trigger     ! arc_prep trigger
FILE_DIRECTORY11 = adp_input
FILE_OBJECT11 = *.DAT         !  THF*.DAT files.
!===========================================================================
!
! Error trapping uses trigger file extensions;  
!
XPOLL_STATE.00 = FILE_SUCCESS
XPOLL_STATE.01 = FILE_ERROR
XPOLL_STATE.03 = FILE_ERROR2 

FILE_ERROR2.DANGLE = _odd        ! file picked up by adpst and ignored
FILE_PROCESSING.DANGLE = _work   ! Extension addition during processing
FILE_SUCCESS.DANGLE  = _processing   ! Extension addition if normal processing
FILE_ERROR.DANGLE      = _bad    ! Extension addition if error
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
ENV.WORKDIR = adp_work        ! where we work
ENV.AUXDIR = rii_aux          ! where our data is
ENV.LOG_FILES = log_files     ! where do we write log files?
ENV.ALERTS = alerts           ! where to write alerts for odd files
ENV.ADP_INPUT = adp_input     ! where input files are put
ENV.ARC_TRIG = arcingest      ! where archive triggers go
ENV.PARFILES = parfiles       ! parfiles go here
