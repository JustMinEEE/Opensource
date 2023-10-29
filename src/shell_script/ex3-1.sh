#!/bin/sh

read num

i=0
while [ "$i" -lt "$num" ]
do
    echo "hello world"
    i=$(expr $i + 1)
done

exit 0
