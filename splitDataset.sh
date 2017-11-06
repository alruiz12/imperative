#!/bin/bash
rm ~/imperative/input -dfr
mkdir ~/imperative/input
cd ~/imperative/input
DATASET=~/Downloads/spambase.data
lines=$(wc -l $DATASET )
lines=${lines%$DATASET}
linesPerFile=$(($lines / $1))
split -l $linesPerFile -d $DATASET
echo $lines
