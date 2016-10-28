#!/usr/bin/env bash

export PYTHON_EGG_CACHE=./myeggs
kinit -k -t DOF7475.keytab -V dof7475@HCA.CORPAD.NET
hdfs dfs -rm result1.txt
impala-shell -i impaladev.hca.corpad.net -k --ssl -f auditscript_impala.hql > result1.txt;
hdfs dfs -put -f result1.txt .