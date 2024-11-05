timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMADT --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMMDM --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMORU --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMIPLORU --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMRAS --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMRDE --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMORM --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMORMORDERS --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMPPR --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMSIU --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMVXU --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMAUDITS --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMALLPROCREJECTED --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMJSON --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMALLPROCREJECTED --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMALLSEGMENT --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMPATIDLOAD --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMSCRIPATID_JSON --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMSCRIPATIDJSONHIST --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMDRSYNTHESYSRESULTS --max-messages 2 --from-beginning
timeout 10 kafka-console-consumer --zookeeper xrdclpbdn010001.unix..net:2181,xrdclpbdn010002.unix..net:2181,xrdclpbdu010001.unix..net:2181  --topic HL7CDMEDWTOSCRINVQ --max-messages 2 --from-beginning


if [[ $? > 0 ]]; then
  echo "Topics are not Accessible after upgrade"
   exit 1;
fi