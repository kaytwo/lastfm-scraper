#!/bin/bash

# usage: $1=startid, $2=interval, $3=stopid

for i in $(seq $1 $2 $3) ; do
    python scraper.py azure $i $2 | gzip > azure-$i-$(($i+$2)).json.gz
done

