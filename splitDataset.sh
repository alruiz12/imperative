#!/bin/bash
rm ~/IdeaProjects/imperative/imperative/input -dfr
if [ -d /home/alvaro ]; then
    DATASET=~/IdeaProjects/imperative/imperative/datasets/xaa
    rm ~/IdeaProjects/imperative/imperative/input -dfr
    mkdir ~/IdeaProjects/imperative/imperative/input
    cd ~/IdeaProjects/imperative/imperative/input
else
    DATASET=~/java/imperative/datasets/spambase.data
    rm ~/java/imperative/input -dfr
    mkdir ~/java/imperative/input
    cd ~/java/imperative/input
fi
#mkdir ~/IdeaProjects/imperative/imperative/input
cd ~/IdeaProjects/imperative/imperative/input

lines=$(wc -l $DATASET )
lines=${lines%$DATASET}
linesPerFile=$(($lines / $1))
split -l $linesPerFile -d $DATASET
echo $lines
