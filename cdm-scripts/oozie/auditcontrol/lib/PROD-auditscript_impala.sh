#!/usr/bin/env bash

export PYTHON_EGG_CACHE=./myeggs
kinit -k -t corpsrvcdmbtch.keytab -V corpsrvcdmbtch@HCA.CORPAD.NET
hdfs dfs -rm result1.txt
impala-shell -i impaladev.hca.corpad.net -k --ssl -f auditscript_impala.hql > result1.txt;
hdfs dfs -put -f result1.txt .