#!/bin/bash
rm ~/imperative/input -dfr
if [ -d /home/alvaro ]; then
    DATASET=~/imperative/datasets/xaa
    rm ~/imperative/input -dfr
    mkdir ~/imperative/input
    cd ~/imperative/input
else
    DATASET=~/java/imperative/datasets/spambase.data
    rm ~/java/imperative/input -dfr
    mkdir ~/java/imperative/input
    cd ~/java/imperative/input
fi
mkdir ~/imperative/input
cd ~/imperative/input

lines=$(wc -l $DATASET )
lines=${lines%$DATASET}
linesPerFile=$(($lines / $1))
split -l $linesPerFile -d $DATASET
echo $lines
