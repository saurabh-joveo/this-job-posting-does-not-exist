#!/usr/bin/env bash
output_prepend=$1
idx=1

max_num_processes=10

for file in 'data/job_titles/job_titles_part'*
do
    ((i=i%max_num_processes)); ((i++==0)) && wait
    python3 async_webscraper.py $file $output_prepend$idx".tsv" &
    ((idx+=1))
done
