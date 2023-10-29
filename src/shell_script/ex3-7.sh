#!/bin/sh

folderName=$1

if [ -d "$folderName" ]; then
	echo "폴더 '$folderName'이 이미 존재"
else
	mkdir "$folderName"
	cd "$folderName"
	
	for i in 0 1 2 3 4 
	do
		mkdir "file$i"
		filename="file$i.txt"
		touch "$filename"
		
		ln "$filename" "file$i"
	done
fi

exit 0
