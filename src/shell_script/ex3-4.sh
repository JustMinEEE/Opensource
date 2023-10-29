#!/bin/sh

echo "리눅스가 재미있나요? (yes / no)"
read choice

case "$choice" in
	[yY]*)
		echo "yes"
		;;
	[nN]*)
		echo "no"
		;;
	*)
		echo "yes or no로 입력해주세요."
		exit 1
		;;
esac

exit 0
