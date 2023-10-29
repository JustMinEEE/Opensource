#!/bin/sh

folderName=$1

if [ -d "$folderName" ]; then
	echo "폴더 '$folderName'이 이미 존재"
else
	mkdir "$folderName"
	cd "$folderName"
	
	for i in 0 1 2 3 4 
	do
		filename="file$i.txt"
		touch "$filename"
	done
	
	tar -czvf "$folderName.tar" *
	
	mkdir "$folderName"
	mv "$folderName.tar" "$folderName/"
	
	cd "$folderName"
	tar -xvf "$folderName.tar"
fi

exit 0
