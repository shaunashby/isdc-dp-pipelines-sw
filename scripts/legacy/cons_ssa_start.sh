#!/bin/sh

###############################################################################
#
#   File:      cons_ssa_start.sh
#   Version:   0.7
#   Component: consssa
#
#   Author(s): Mathias.Beck@obs.unige.ch (MB)
#              Jake.Wendt@obs.unige.ch (JW)
#
#   Purpose:   trigger SCW_SA (CONS_SSA) runs for a given number of
#              revolutions
#
#   Revision History:
#
#      0.7                 JW   SCREW 1765 - max_duration
#                               Also: picsit test split over 2 lines
#                                     sed command missing last '
#
#      0.6    11-Aug-2005  MB   min_duration: 1000 -> 500
#
#      0.5    25-Nov-2004  MB   added support for higher SCW versions
#                               always the one in the archive idx is taken
#
#      0.4    18-Oct-2004  MB   added -n parameter, to NOT create the
#                               trigger-file, but otherwise do everything
#
#      0.3    14-Oct-2004  MB   isgr -> isgri
#                               pics -> picsit
#
#             12-Oct-2004  MB   allow wildcards for revnos
#
#      0.2    07-Oct-2004  MB   ibis -> isgr + pics
#
#      0.1    30-Sep-2004  MB   initial prototype
#
###############################################################################

ALL_INSTRUMENTS="isgri jmx1 jmx2 picsit omc spi"

if [ -z $REP_BASE_PROD ]; then
    REP_BASE_PROD="/isdc/arc/rev_2"
    echo "Using $REP_BASE_PROD for REP_BASE_PROD"
else
    echo "REP_BASE_PROD alreadyset. Using current value $REP_BASE_PROD."
fi

CONS_SSA_INPUT_DIR="${OPUS_WORK}/consssa/input"
SWG_INDEX_DIR="${REP_BASE_PROD}/idx/scw/"
CONS_SSA_BLACKLIST="${OPUS_WORK}/opus/cons_ssa_blacklist.txt"

#
# default settings for command line arguments
#
use_only_pointings="yes"
min_duration="500"
MAX_DUR="999999"					#	SCREW 1765
max_duration="${MAX_DUR}"		#	SCREW 1765
use_zero_pointings="no"
touch_triggers="yes"
instruments=${ALL_INSTRUMENTS}


###############################################################################
#
#  cons_ssa_get_instr_2_ignore_from_data
#
#     Purpose:   check the existance of data for a given instrument and SWID
#
#     Input: $1: swid_vvv -- the science window ID
#            $2: instr    -- the instrument
#
#     Output: instr_2_ignore_from_data: yes -- don't trigger the swid
#                                              for the instr
#                                       no  -- trigger the swid
#                                              for the instr
#
###############################################################################
cons_ssa_get_instr_2_ignore_from_data() {

   swid_vvv=$1
   instr=$2
   
   instr_2_ignore_from_data="yes"

   if [ ${instr} = "isgri" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/isgri_events.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      fi
   elif [ ${instr} = "jmx1" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/jmx1_events.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      fi
   elif [ ${instr} = "jmx2" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/jmx2_events.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      fi
   elif [ ${instr} = "omc" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/omc_shots.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      elif [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/omc_trigger.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      fi
   elif [ ${instr} = "picsit" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/picsit_events.fits* ] ;
      then
      	 instr_2_ignore_from_data="no"
      fi
   elif [ ${instr} = "spi" ] ; then
      if [ -r ${REP_BASE_PROD}/scw/${revno}/${swid_vvv}/spi_oper.fits* ] ; then
      	 instr_2_ignore_from_data="no"
      fi
   else
      echo "!!!!! Warning: cons_ssa_get_instr_2_ignore_from_data: unknown instrument ${instr}"
   fi
}


###############################################################################
#
#  cons_ssa_get_instr_2_ignore_from_bl
#
#     Purpose:   get the list of instruments that should be ignored
#                for a given SWID. The list is read from a blacklist
#                file. A line with only a SWID and no instruments
#                specified results in all instruments ignored
#
#     Input: $1: swid_vvv -- the science window ID
#
#     Output: instr_2_ignore_from_bl: list of instruments for which no
#                                     trigger should be created for the swid
#
###############################################################################
cons_ssa_get_instr_2_ignore_from_bl() {

   swid_vvv=$1

   if [ -r ${CONS_SSA_BLACKLIST} ] ; then
      /bin/grep "^${swid_vvv}" ${CONS_SSA_BLACKLIST} > /dev/null
      if [ $? -eq 0 ] ; then
      	 instr_2_ignore_from_bl=`/bin/grep "^${swid_vvv}" ${CONS_SSA_BLACKLIST} | /bin/cut -d' ' -f2-`
      	 if [ "${instr_2_ignore_from_bl}" = "${swid_vvv}" ] ; then
	    instr_2_ignore_from_bl=${ALL_INSTRUMENTS}
	 fi
      else
      	 instr_2_ignore_from_bl=""
      fi
   fi
}


###############################################################################
#
#  cons_ssa_revno
#
#     Purpose: trigger the SCW_SA (CONS_SSA) for one specific revolution
#
#     Input: $1: revno -- the revolution number
#
#     Output: none
#
###############################################################################
cons_ssa_revno() {

   revno=$1
   
   echo ">>>>> selecting SWIDs for revno ${revno}"
   select_string="REVOL == ${revno} && TELAPSE >= ${min_duration}"

   if [ ${max_duration} != "${MAX_DUR}" ] ; then							#	SCREW 1765
      select_string=${select_string}" && TELAPSE <= ${max_duration}"
   fi

   if [ ${use_only_pointings} = "yes" ] ; then
      select_string=${select_string}" && SW_TYPE == 'POINTING'"
   fi
   
   echo ">>>>> select_string: ${select_string}"
   
   sub_index=/tmp/$$_selected_scw_${revno}.fits
   if [ -f ${sub_index} ] ; then
      /bin/rm -f ${sub_index}
   fi
   idx_find index=${SWG_INDEX_DIR}/GNRL-SCWG-GRP-IDX.fits+1 \
      select="${select_string}" subIndex=${sub_index} > /dev/null
   
   error_code=$?
   if [ ${error_code} -ne 0 ] ; then
      echo "***** Error: idx_find terminated with error code ${error_code}"
      return
   fi

   fdump_cmd="fdump prhead=no showrow=no showcol=no page=no ${sub_index}+1 STDOUT MEMBER_LOCATION -"
   if [ ${use_zero_pointings} = "no" ] ; then
      # Don't apply cut to get field: this is incorrect when the index returns paths like "../../../../isdc/.."
      # Use basename on the full path later instead, after having removed swg.fits here (index contains file names):
      swid_list=`${fdump_cmd} | grep scw | sed -e 's#/swg\.fits##g' | /bin/sort | /bin/grep -v ${revno}0000`
      error_code=$?
   else
      swid_list=`${fdump_cmd} | grep scw | sed -e 's#/swg\.fits##g' | /bin/sort`
      error_code=$?
   fi

   if [ ${error_code} -ne 0 ] ; then
      echo "***** Error: dal_dump exited with code ${error_code}"
      echo "      -> skip revno"
      return
   fi
   
   for swid_vvv_path in ${swid_list} ; do
    # Get the SCW id from the full path using basename:
    swid_vvv=`/bin/basename ${swid_vvv_path}`
#
# note: var. instr_2_ignore_from_bl is set inside the following function
#
      swid=`echo ${swid_vvv} | /bin/cut -c1-12`
      cons_ssa_get_instr_2_ignore_from_bl ${swid_vvv}

      for instr in ${instruments} ; do

      	 create_trigger="yes"
      	 trigger_file=${swid}_${instr}.trigger

      	 if [ -n "${instr_2_ignore_from_bl}" ] ; then
	    if [ "`echo ${instr_2_ignore_from_bl} | /bin/grep ${instr}`" = "${instr_2_ignore_from_bl}" ] ; then
      	       echo ">>>>> blacklisted"
      	       create_trigger="no"
	    fi
	 fi

#
# note: var. instr_2_ignore_from_data is set inside the following function
#
      	 if [ ${create_trigger} = "yes" ] ; then
            cons_ssa_get_instr_2_ignore_from_data ${swid_vvv} ${instr}

      	    if [ "${instr_2_ignore_from_data}" = "yes" ] ; then
      	       create_trigger="no"
      	       echo ">>>>> no data"
	    fi
	 fi

      	 if [ ${create_trigger} = "yes" ] ; then
      	    if [ ${touch_triggers} = "yes" ] ; then
      	       echo ">>>>> creating trigger-file: ${trigger_file}"
      	       /bin/touch ${CONS_SSA_INPUT_DIR}/${trigger_file}
	    else
      	       echo ">>>>> skipping good trigger-file: ${trigger_file}"
            fi
      	 else
	    echo ">>>>> ignoring bad trigger-file: ${trigger_file}"
         fi
      done

   done

   /bin/rm -f ${sub_index}
}

###############################################################################
#
#  main loop
#
###############################################################################

while [ "x`echo x"$1" | /bin/grep 'x-'`" != "x" ] ; do

   if [ x"$1" = "x--h" ] ; then
      echo ">>>>> Usage: cons_ssa_start.sh [--h] [-n] [-i isgri,jmx[12],omc,picsit,spi] [--instruments isgri,jmx[12],omc,picsit,spi] [-a --all] [-d --duration --min_duration time] [--max_duration time] [-z --zeropointings] revno_1 [revno_2 ...]"
      exit 0
   elif [ x"$1" = "x-i" -o x"$1" = "x--instruments" ] ; then
      if [ x"`echo $2 | /bin/grep -w jmx`" != "x" ] ; then
      	 tmp=`echo $2 | /bin/sed -e 's/jmx/jmx1,jmx2/'`
      else
      	 tmp=$2
      fi
      instruments=`echo ${tmp} | /bin/sed -e 's/,/ /g'`
      shift
   elif [ x"$1" = "x-a" -o x"$1" = "x--all" ] ; then
      use_only_pointings="no"
   elif [ x"$1" = "x-d" -o x"$1" = "x--duration" -o x"$1" = "x--min_duration" ] ; then
      min_duration=$2
      shift
   elif [ x"$1" = "x--max_duration" ] ; then							#	SCREW 1765
      max_duration=$2
      shift
   elif [ x"$1" = "x-z" -o x"$1" = "x--zero_pointings" ] ; then
      use_zero_pointings="yes"   
   elif [ x"$1" = "x-n" ] ; then
      touch_triggers="no"   
   fi

   shift

done

while [ $# -ne 0 ] ; do

#	070921 - Jake - SPR 4735
#
#	The printf statement doesn't work correctly on the isdclin machines
#   revno_all=`/usr/bin/printf "%04s" $1`  gives /usr/bin/printf: %04s: invalid conversion specification
#   revno_all=`/usr/bin/printf "%04d" $1`  gives converts from octal if $1 has leading 0
#			/usr/bin/printf "%04d" 0100 yields 0064
#	Using echo, awk and printf seems to work correctly on all environments tested.
#
	revno_all=`echo $1 | awk '{printf "%04d", $1}'`

   for revno_dir in `/bin/ls -d ${REP_BASE_PROD}/scw/${revno_all}` ; do
      revno=`/bin/basename ${revno_dir}`

      cons_ssa_revno ${revno}
   done

   shift

done
