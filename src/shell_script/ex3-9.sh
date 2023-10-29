#!/bin/bash

search_query="$1"

found=false

while IFS= read -r line; do
  name=$(echo "$line" | cut -d' ' -f1)
  phone_number=$(echo "$line" | cut -d' ' -f2)

  if [ -n "$(echo "$name" | grep "$search_query")" ] || [ -n "$(echo "$phone_number" | grep "$search_query")" ]; then
    echo "$name $phone_number"
    found=true
  fi
done < DB.txt

if [ "$found" = false ]; then
  echo "검색 결과 없음"
fi

