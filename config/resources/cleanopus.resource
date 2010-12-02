!--------------------------------------------------------------------
!
! Name: cleanopus.resource
!
! Purpose: This file is used to construct the trigger, error, and
!          success status fields in the observation status file.
!
!       This resource file uses an OSF trigger.
!
!--------------------------------------------------------------------
!                          REVISION HISTORY
!--------------------------------------------------------------------
!           PR
! DATE     NUMBER  User   Description
! ------   ------ ------- ------------------------------
! 02/10/01  42443 Heller  first version
!
!--------------------------------------------------------------------

PROCESS_NAME = cleanopus
!TASK = <osfdelete -p $PATH_FILE -r cleanosf>
TASK = <xpoll -p $PATH_FILE -r cleanopus >
COMMAND = cleanup.pl  
CLASS = ALL
DESCRIPTION = 'Delete OSFs from the BB'
SYSTEM = 'ANY'
DISPLAY_ORDER = 1

!---------------------------------------------------------------------------
! EVNT resource.
!---------------------------------------------------------------------------

POLLING_TIME = 5		! Response time of the application
OSF_RANK = 1			! OSF event ordering.
OSF_TRIGGER1.CL = o		! archive completed will trigger OSF deletion
XPOLL_STATE.00 = OSF_SUCCESS
XPOLL_STATE.01 = OSF_ERROR

!---------------------------------------------------------------------------
! Application Specific resource
!---------------------------------------------------------------------------
OSF_PROCESSING.CL = p		! letter to be used when an OSF is processed.
OSF_ERROR.CL = x		! letter to be used when there is an error.
OSF_SUCCESS.CL = d	! set foor cleanosf
XPOLL_ERROR.CL = x
!XPOLL_ERROR_COUNT = 10000   ! every time, it errors because it can't find the
                            ! OSF to update afterward.  
