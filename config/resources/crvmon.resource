
!===================================================================
!
SYSTEM = 'CONS Rev Pipeline'
DISPLAY_ORDER=7
DESCRIPTION = 'CONS Rev: Monitor'
PROCESS_NAME = crvmon						! 060110 - Jake - probably not necessary, but ...
POLLING_TIME = 60								!seconds between tests for a new event
TASK = < xpoll -p $PATH_FILE -r crvmon >	!the OPUS method for invoking
													!the process
COMMAND = crvmon.pl							!the script (in your path) that
													!runs this pipeline process
CLASS = rev 
XPOLL_STATE.00 = SUCCESS
XPOLL_STATE.01 = XPOLL_ERROR

XPOLL_ERROR_COUNT = 10						!keep trying  up to 10 times

TIME_RANK = 1									!this value could change if multiple trigger 
													!types are used in combination
!DELTA_TIME = 000:00:10:00					!trigger every 10 minutes from the initial 
DELTA_TIME = 000:00:01:00					! should be 10 minutes; 1 for testing
													!time at which the process is started
!MINBLOCKS = 100								! bump this up as needed
!RESUME_BLOCKS = 100							! bump this up as needed
!
ENV.OUTPATH = rii								! repository 2
ENV.SCWDIR = rii_scw							! where our data is
ENV.WORKDIR = rev_work
ENV.PARFILES = parfiles
ENV.REV_INPUT = rev_input					! input dir for changing trigger file
ENV.SSA_INPUT = ssa_input					! input dir for changing trigger file
ENV.SMA_INPUT = sma_input					! input dir for changing trigger file
ENV.INP_INPUT = inp_input					! input trigger to move to _done
ENV.SCW_INPUT = scw_input					! where ScW triggers go
ENV.LOG_FILES = log_files					! where do we write log files?
! 
!  Archiving triggers:
ENV.ARC_TRIG = arcingest					! where the pipeline writes them
ENV.ARC_TRIG_INGESTING = arcingesting	! where archingest gets them
ENV.ARC_TRIG_DONE = archingest_done		! where they're moved when ingested 
!
ENV.SPI_PSD_ADC_DELTA = 5400				! seconds between spi_psd_adcgain analyses
ENV.SPI_PSD_EFFI_DELTA = 5400				! seconds between spi_psd_efficiency analyses
ENV.SPI_PSD_PERF_DELTA = 10800			! seconds between spi_psd_performance analyses
ENV.SPI_PSD_SI_DELTA = 10800				! seconds between spi_psd_si analyses
!
! Allowed number of errors in percentage of total number of OSFs for archiving:
ENV.ARC_ALLOW_ERR_PERCENT_REV = 0		!  no errors allowed in Cons
ENV.ARC_ALLOW_ERR_PERCENT_SCW = 0		!  no errors allowed in Cons



!  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  
!
!			New stuff added to control the automation by crvmon
!
!  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  
!
!	Use PP or comment out under normal circumstances
!	If not processing input->rev->scw->rev->ssa->...
!	and only processing ssa->... set this to SSA
!	You will need to touch $OPUS_WORK/consrev/input/${revno}_pp.done for each
!	revolution, otherwise crvmon.pl will not check it's status.
ENV.START_LEVEL = PP							!	"SSA" and "SMA" are the only 2 working values right now. All else ignored.

!	if this is NO, processing will stop and cleaning will begin after the presence of ${revno}_arc.done
!	otherwise, depending on the following flags, processing may continue
ENV.USING_AUTO_TRIGGERING = 0				!	1 = Yes, 0 = No (Default this to No, but unit_test will do it anyway)

!	if this is YES and ENV.USING_AUTO_TRIGGERING is YES, cons_ssa_start.sh will be run for this $revno
!	if this is NO, processing will stop and cleaning will begin after the presence of ${revno}_arc.done
ENV.AUTO_START_SSA = 0						!	1 = Yes, 0 = No (Default this to No, but unit_test will do it anyway)

!	if this is YES, the SSA data and BB will be cleaned automatically after ingestion
ENV.AUTO_CLEAN_SSA = 0						!	1 = Yes, 0 = No (Default this to No, but unit_test will do it anyway)



!	SMA really has no actual functionality as the cons_sma_start.sh script has not been incorporated yet.
ENV.AUTO_START_SMA = 0						!	1 = Yes, 0 = No (Default this to No, but unit_test will do it anyway)

!	if this is YES, the SMA data and BB will be cleaned automatically after ingestion
ENV.AUTO_CLEAN_SMA = 0						!	1 = Yes, 0 = No (Default this to No, but unit_test will do it anyway)


