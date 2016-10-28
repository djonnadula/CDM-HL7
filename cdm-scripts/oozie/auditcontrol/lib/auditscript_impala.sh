#!/usr/bin/env bash

export PYTHON_EGG_CACHE=./myeggs
kinit -k -t DOF7475.keytab -V dof7475@HCA.CORPAD.NET
impala-shell -i impaladev.hca.corpad.net -k --ssl -f auditscript_impala.hql > result1.txt;
hdfs dfs -put -f result1.txt .
#hdfs dfs -put -f result1.txt /user/hue/oozie/workspaces/hue-oozie-1476845447.15/lib/

