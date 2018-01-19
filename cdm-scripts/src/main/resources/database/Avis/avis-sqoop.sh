#!/bin/bash
# /etc/init.d/edh-ehw-sqoop-Export
# version 1.0.0 2017-05-03 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description:  Starts sqoop-Export for EDW from EDH
#
### BEGIN INIT INFO
# Provides:          sqoop-Export
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6

### END INIT INFO



sqoop-export  -Dmapreduce.job.queuename=CDM -D mapreduce.map.memory.mb=3072 -D mapreduce.map.java.opts=-Xmx3072m \
--connect "jdbc:teradata://edwprod.dw.medcity.net/DATABASE=EDWCDM_STAGING,CHARSET=UTF8" \
--table CDM_AVIS_STAGING \
--mapreduce-job-name EDH_CDM_AVIS_STAGING \
--username BD_CDMLoad \
--password-file "hdfs://nameservice1/user/corpsrvcdmbtch/cdm_edh_teradata.password" \
--export-dir /user/hive/warehouse/hl7/hl7_avis_staging \
--input-lines-terminated-by '\n' \
--input-fields-terminated-by '|' \
--input-null-string '\\\\N' \
--input-null-non-string '\\\\N' \
--verbose \
--num-mappers 6 \
--package-name com.hca.cdm.model \
--outdir /hadoop/cdm/avis/edw/model \
--bindir /hadoop/cdm/avis/edw/compiled \

LoadCode=$?
if [ $LoadCode!= 0 ];
	then
		echo "EDWCDM_STAGING.CDM_AVIS_STAGING Loading Failed"
		exit 1
fi

