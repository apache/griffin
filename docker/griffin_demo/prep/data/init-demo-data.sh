#!/bin/bash

sleep 30

nohup ./gen-es-data.sh > gen-es.log &
nohup ./add-measure.sh > add-measure.log &
