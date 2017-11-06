#!/bin/bash
rm ~/imperative/input -dfr
mkdir ~/imperative/input
cd ~/imperative/input
#DATASET=~/imperative/datasets/spambase.data
 if [ -d /home/alvaro ]; then DATASET=~/imperative/datasets/xaa;
 else DATASET=~/java/imperative/datasets/spambase.data; fi
echo $DATASET
lines=$(wc -l $DATASET )
lines=${lines%$DATASET}
linesPerFile=$(($lines / $1))
split -l $linesPerFile -d $DATASET
echo $lines
