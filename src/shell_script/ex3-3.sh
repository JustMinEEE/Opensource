#!/bin/sh

weight=$1
height=$(echo "scale=1; $2 / 100" | bc)

bmi=$(echo "scale=2; $weight / ($height * $height)" | bc)

if [ $(echo "$bmi >= 23" | bc) -eq 1 ]; then
    echo "Overweight"
elif [ $(echo "$bmi >= 18.5" | bc) -eq 1 ]; then
    echo "Average weight"
else
    echo "Underweight"
fi

exit 0

