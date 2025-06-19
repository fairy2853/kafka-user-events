#!/bin/bash

python event_producer.py &
PID1=$!

python spark.py &
PID2=$!

sleep 120

kill $PID1
kill $PID2

wait $PID1
wait $PID2

python merge_output.py
