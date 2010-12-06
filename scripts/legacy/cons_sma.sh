#!/bin/sh

###############################################################################
#
#   File:      cons_sma.sh
#   Version:   0.4
#   Component: consssa
#
#   Author(s): Mathias.Beck@obs.unige.ch (MB)
#
#   Purpose:   a) import SCW lists from VB provided files
#              b) trigger OBS_SA_MOSAIC and SPI_SA (CONS_SSA) runs
#                 for a given number of lists
#
#   Revision History:
#
#      0.4    17-Nov-2005  MB   replace '<' and '>' in the source
#                               names by '_lt_' and '_gt_'
#
#      0.3    03-Aug-2005  MB   NOT YET IMPLEMENTED BUT NEEDED !!!
#                               added pre-creation of directories and
#                               symbolic links at the beginning of
#                               function cons_ssa_revno
#
#      0.2    02-Jun-2005  MB   added list generation action 'generate'
#
#      0.1    16-Dec-2004  MB   initial prototype
#
###############################################################################

JOIN_OBS_PER_SOURCE=yes # set this to no if you want different OBS_IDs
                        # for the same SRC_NAME go to different files

MASTER_BASE=${HOME}/work/mosaics/master
DOLS_FROM_ARC_BASE=${HOME}/work/mosaics/dols_arc

rep_base_prod=/isdc/arc/rev_2

cons_ssa_input_tmp=${OPUS_WORK}/consssa/scratch

cons_ssa_input=${OPUS_WORK}/consssa/input
#cons_ssa_input=${OPUS_WORK}/consssa/scratch/mb_test

today=`/bin/date +"%Y%m%d"`


###############################################################################
#
# cons_sma_generate
#
###############################################################################
cons_sma_generate () {

   rrrr=$1

   work_dir=${DOLS_FROM_ARC_BASE}/${today}

   if [ ! -d ${work_dir} ] ; then
    /bin/mkdir ${work_dir}
   fi

   tmp_file=/tmp/cons_sma_list_${rrrr}_$$

   echo ">>>>> Generating lists for revolution ${rrrr} ..."

   #
   # get the latest POD
   #
   aux_rrrr_vvv=`/bin/ls -d ${rep_base_prod}/aux/adp/${rrrr}.* | \
      /usr/bin/tail -1`
   pod=`/bin/ls ${aux_rrrr_vvv}/pod_${rrrr}_* | /usr/bin/tail -1` 
   echo ">>>>> POD is ${pod}"
   
   fdump showrow=no showcol=no prhead=no infile=${pod} \
   outfile=${tmp_file}.tmp columns=AMALG_STAT,OBS_ID,EXP_ID,SRC_NAME rows=-

#	Produces
#---
#
#
#
#  02201330003 03060001 Gal.Center field
#  03201090019 03060002 Gal. Bulge region
#  02201330003 03060003 Gal.Center field
#  02201330003 03060004 Gal.Center field
#  02201330003 03060005 Gal.Center field
#
#---

#	Jake - /usr/xpg4/bin/grep does not exist on isdclin machines and /bin/grep -v '^$' does remove blank lines
#   /usr/xpg4/bin/grep -v '^$' ${tmp_file}.tmp | /bin/grep -v '^A' | \
   /bin/grep -v '^$' ${tmp_file}.tmp | /bin/grep -v '^A' | \
      sed -e 's/  / /' > ${tmp_file}

   /bin/rm -f ${work_dir}/dolsrev${rrrr}_*
   








#	060630
#	The following addition of `sort` fixes the duplicate listing of 
#	science windows as the source list may not be sorted and therefore
#	is repeated and appended to the same file.

#   for obs_id in `/bin/cut -d" " -f2 ${tmp_file} | sort -k 3 | /usr/bin/uniq` ; do





   for obs_id in `/bin/cut -d" " -f2 ${tmp_file} | /usr/bin/uniq` ; do

#	Jake - added +, #, . and the final s/__/_/g  (The . needs escaped)
      src_name=`/bin/grep " ${obs_id}" ${tmp_file} | /usr/bin/tail -1 \
			| /bin/cut -d" " -f4- \
			| /bin/sed -e 's/ /_/g' \
				-e "s^+^_^" \
				-e "s^#^_^" \
				-e "s^\.^_^" \
				-e "s^>^_gt_^" \
				-e"s^<^_lt_^" \
			| /bin/sed -e 's/__/_/g'`

      echo ">>>>> OBS_ID is ${obs_id}, SRC_NAME is ${src_name}"

      list_file=${work_dir}/dolsrev${rrrr}_${src_name}_cons.txt

      if [ -f ${list_file} ] ; then
      	 if [ ${JOIN_OBS_PER_SOURCE} = "no" ] ; then
      	    /bin/mv ${list_file} ${work_dir}/dolsrev${rrrr}_${src_name}_1_cons.txt
      	    i1=2
      	    while [ ${i1} -le 9 ] ; do
      	       list_file=${work_dir}/dolsrev${rrrr}_${src_name}_${i1}_cons.txt
      	       if [ ! -f ${list_file} ] ; then
	          break
	       fi
      	       i1=`echo ${i1} + 1 | /bin/bc`
	    done
	    if [ ${i1} -eq 10 ] ; then
	       echo "***** Error: maximum number of lists per source reached"
      	       echo "      -> Abort"
	       exit 1
	    fi
      	 fi
      fi
      
      if [ -f ${list_file}.tmp ] ; then
      	 /bin/rm -f ${list_file}.tmp
      fi

#      echo "# OBS_ID is ${obs_id}, SRC_NAME is ${src_name}" >  ${list_file}

      for exp_id in `/bin/grep " ${obs_id}" ${tmp_file} | \
      	 /bin/cut -d" " -f3` ; do

	 echo ">>>>> EXP_ID is ${exp_id}"

      	 ${ISDC_ENV}/bin/idx2dol \
	    index=${rep_base_prod}/idx/scw/GNRL-SCWG-GRP-IDX.fits+1 \
	    select="EXPID == '${exp_id}' && SW_TYPE == 'POINTING'" numLog=0 outFormat=2 \
	    txtFile=${list_file}.tmp
	 /bin/sed -e "s^${rep_base_prod}/^^g" ${list_file}.tmp >> ${list_file}
      	 /bin/rm -f ${list_file}.tmp
      done

   done

   echo ">>>>> done"
}

###############################################################################
#
# cons_sma_import
#
###############################################################################
cons_sma_import () {

   dols_vb=$1

   work_dir=${MASTER_BASE}/${today}

   if [ ! -d ${work_dir} ] ; then
    /bin/mkdir ${work_dir}
   fi

   dols_vb_base=`/bin/basename ${dols_vb} _cons.txt`

   revno_tmp=`echo ${dols_vb_base} | /bin/cut -d'_' -f1 | /bin/cut -c8-`
   revno=`/usr/bin/printf "%04s" ${revno_tmp}`

   og_name=`echo ${dols_vb_base} | /bin/cut -d'_' -f2-`

   echo ">>>>> revno:   ${revno}"
   echo ">>>>> og_name: ${og_name}"

   out_list=${work_dir}/${revno}_${og_name}.list

   if [ -f ${out_list} ] ; then
      /bin/rm -f ${out_list}
   fi

   echo "# " > ${out_list}
   echo "# created ${today} from ${dols_vb}" >> ${out_list}
   echo "# " >> ${out_list}

   scws_to_skip=""
   for scw_id in `/bin/grep '^#' ${dols_vb} | /bin/cut -d'/' -f3 | /bin/cut -c1-12` ; do
      scws_to_skip=${scws_to_skip}" ${scw_id}"
   done
   
   if [ -n "${scws_to_skip}" ] ; then
      echo "# " >> ${out_list}
      echo "#_SKIP_spi ${scws_to_skip}" >> ${out_list}
   fi

   for scw_id in `/bin/cut -d'/' -f3 ${dols_vb} | /bin/cut -c1-12` ; do

      if [ -d ${rep_base_prod}/scw/${revno}/${scw_id}.* ] ; then
      	 echo ">>>>> keep scw_id: ${scw_id}"
      	 echo ${scw_id} >> ${out_list}
      else
      	 echo ">>>>> drop scw_id: ${scw_id}"
      fi
   
   done

   echo "# " >> ${out_list}
   echo "# end-of-list" >> ${out_list}

}

###############################################################################
#
# cons_sma_trigger
#
###############################################################################
cons_sma_trigger () {

   instr=$1
   master_list=$2
   rrrr_version=$3
   
   if [ ! -r ${master_list} ] ; then
      echo "***** Error: ${master_list} does not exist or is not readable"
      echo "      -> skip"
      return
   else
      echo ">>>>> processing ${master_list}"
   fi
   
   for my_dir in ${cons_ssa_input_tmp} ${cons_ssa_input} ; do
      if [ ! -d ${my_dir} ] ; then
      	 echo "!!!!! directory ${my_dir} does not exist."
      	 echo "      -> will be created now ..."
      	 /bin/mkdir -p ${my_dir}
      fi
   done

   og_name=`/bin/basename ${master_list} .list`
   revno=`echo ${og_name} | /bin/cut -c1-4`   

   trigger_tmp=${cons_ssa_input_tmp}/${og_name}_${instr}.trigger_tmp
   trigger=${cons_ssa_input}/${og_name}_${instr}.trigger

   #
   # get the highest version number available in the archive
   #
   # right now, this script always imposes the VVV as in the workarea
   # we have RRRR.000 along with RRRR.VVV causing a problem to the
   # pipeline's finddirvers
   #
   if [ ${rrrr_version} = "highest" ] ; then
      rrrr_dir=`/bin/ls -d ${rep_base_prod}/obs_${instr}/${revno}.* | /usr/bin/tail -1`
      rrrr_version=`/bin/basename ${rrrr_dir} | /bin/cut -d'.' -f2`
   fi
   if [ ! -d ${rep_base_prod}/obs_${instr}/${revno}.${rrrr_version} ] ; then
      echo "***** Error: directory not found: ${rep_base_prod}/obs_${instr}/${revno}.${rrrr_version}"
      echo "      -> skip list ${og_name}"
      return
   fi
   
   if [ -f ${trigger_tmp} ] ; then
      /bin/rm -f ${trigger_tmp}
   fi

   echo "# " > ${trigger_tmp}
   echo "# created ${today} from ${master_list}" >> ${trigger_tmp}
   echo "# " >> ${trigger_tmp}
   echo "#_RRRR_VERSION_${instr} ${rrrr_version}" >> ${trigger_tmp}

   #
   # w.r.t. to Volker's lists, the master file were cleaned for SCWs
   # not in /isdc/arc/rev_2/scw
   #
   #
   # ISGRI mosaics are built on to of obs_isgri/RRRR.VVV/ssii_*,
   # This requires further checking of SCWs are available at that
   # level as well
   #
   scws_to_skip=""
   if [ ${instr} = "isgri" ] ; then
      for scw_id in `/bin/grep -v '^#' ${master_list}` ; do
      	 if [ -d ${rep_base_prod}/obs_${instr}/${revno}.${rrrr_version}/ssii_${scw_id} ] ; then
      	    echo ">>>>> keep scw_id: ${scw_id}"
      	 else
      	    echo ">>>>> drop scw_id: ${scw_id}"
      	    scws_to_skip=${scws_to_skip}" ${scw_id}"
      	 fi
      done
   elif [ ${instr} = "spi" ] ; then
      scws_to_skip=`/bin/grep "^#_SKIP_spi" ${master_list} | /bin/cut -d' ' -f2-`
   fi

   if [ -n "${scws_to_skip}" ] ; then
      echo "# " >> ${trigger_tmp}
      echo "#_SKIP_${instr} ${scws_to_skip}" >> ${trigger_tmp}
   fi

   echo "# " >> ${trigger_tmp}
   echo "# list of SCW_IDs" >> ${trigger_tmp}
   echo "# " >> ${trigger_tmp}

   for scw_id in `/bin/grep -v '^#' ${master_list}` ; do
      echo ${scw_id} >> ${trigger_tmp}
   done

   echo "# " >> ${trigger_tmp}
   echo "# end of list" >> ${trigger_tmp}

   #
   # provide a hook to interactively change the trigger
   #
   # NOT YET IMPLEMENTED
   #
   /bin/mv ${trigger_tmp} ${trigger}
}


###############################################################################
#
# main loop
#
###############################################################################

action=$1
shift

case ${action} in

   'generate')
      while [ $# -gt 0 ] ; do
      	 for revno_dirs in `/bin/ls -d ${rep_base_prod}/scw/$1*` ; do
      	    revno=`/bin/basename ${revno_dirs}`
      	    cons_sma_generate $revno
	 done
      	 shift
      done
      ;;

   'import')
      cons_sma_import $*
      ;;

   'trigger')
      instr=$1
      shift
      rrrr_version="highest"
      if [ "$1" = "--rrrr_version" ] ; then
	 shift
      	 rrrr_version=`/usr/bin/printf "%03s" $1`
	 shift
      fi
      while [ $# -gt 0 ] ; do 
      	 cons_sma_trigger ${instr} $1 ${rrrr_version}
      	 shift
      done
      ;;
esac
