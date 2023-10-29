#!/bin/sh

case "$2" in
    +)
	res=$(expr $1 + $3);;
    -)
    	res=$(expr $1 - $3);;
    *)
	res="error";; 
esac

echo $res

exit 0
