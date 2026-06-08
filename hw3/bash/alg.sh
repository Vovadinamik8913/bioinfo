#!/bin/bash
minimap2 -d NC_000913.3.fasta.mmi NC_000913.3.fasta
minimap2 -a NC_000913.3.fasta.mmi SRR33637628.fastq > output.sam
samtools flagstat output.sam > output.txt
res=$(grep -oE '[0-9]+\.[0-9]+%' output.txt | head -1 | tr -d '%')
if (( $(echo "$res > 90" | bc -l) )); then
    echo "$res: OK"
    samtools sort output.sam > sorted.bam
    freebayes -f NC_000913.3.fasta sorted.bam >vc.vcf
else
    echo "$res: NOT OK"
fi