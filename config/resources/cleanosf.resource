!--------------------------------------------------------------------
!
! Name: cleanosf.resource
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

PROCESS_NAME = cleanosf
TASK = <osfdelete -p $PATH_FILE -r cleanosf>
CLASS = ALL
DESCRIPTION = 'Delete OSFs from the BB'
SYSTEM = 'ANY PIPELINE'
DISPLAY_ORDER = 1

!---------------------------------------------------------------------------
! EVNT resource.
!---------------------------------------------------------------------------

POLLING_TIME = 5		! Response time of the application
OSF_RANK = 1			! OSF event ordering.
OSF_TRIGGER1.CL = d		! will trigger OSF deletion


!---------------------------------------------------------------------------
! Application Specific resource
!---------------------------------------------------------------------------
OSF_PROCESSING.CL = p		! letter to be used when an OSF is processed.
OSF_ERROR.CL = e		! letter to be used when there is an error.

