#!/bin/sh
while read line
do
  case "$line" in
   "0")
      echo "0 is zero"
      ;;
   "1")
      echo "1 is one"
      ;;
   "2")
      echo "2 is two"
      ;;
   "3")
      echo "3 is three"
      ;;
   "4")
      echo "4 is four"
      ;;
   "5")
      echo "5 is five"
      ;;
   "6")
      echo "6 is six"
      ;;
   "7")
      echo "7 is seven"
      ;;
   "8")
      echo "8 is eight"
      ;;
   "9")
      echo "9 is nine"
      ;;
   *)
      echo "$line is not a digit"
     ;;
  esac
done < "${1:-/dev/stdin}"

