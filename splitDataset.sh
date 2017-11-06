#!/bin/bash
rm ~/imperative/input -dfr
mkdir ~/imperative/input
cd ~/imperative/input
#DATASET=~/Downloads/spambase.data
DATASET=~/Downloads/split/xaa
lines=$(wc -l $DATASET )
lines=${lines%$DATASET}
linesPerFile=$(($lines / $1))
split -l $linesPerFile -d $DATASET
echo $lines
