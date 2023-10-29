#!/bin/sh

func () {
	echo "함수 안으로 들어 왔음"
	str="ls $1"
	
	eval $str
}

echo "프로그램을 시작합니다."
func $1
echo "프로그램을 종료합니다."
