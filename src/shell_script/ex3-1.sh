#!/bin/sh

if [ -z "$1" ]; then
	num=10
else
	num="$1"
fi

i=0
while [ "$i" -lt "$num" ]
do
    echo "hello world"
    i=$(expr $i + 1)
done

exit 0
