#!/bin/sh

name="$1"
pn="$2"

if [ ! -f DB.txt ]; then
	touch DB.txt
fi

echo "$name $pn" >> DB.txt
