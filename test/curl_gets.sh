curl -v -X GET -H "Content-Type: application/json" -H 'Expect:' --data @"./query_1.json" http://127.0.0.1:6070/log
curl -v -X GET -H "Content-Type: application/json" -H 'Expect:' --data @"./query_2.json" http://127.0.0.1:6070/log

