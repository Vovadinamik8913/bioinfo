# Домашнее задание 3
ФИО: Абраменко Владимир Максимович

## Ссылку на загруженные прочтения из NCBI SRA
[Ссылка SRR33637628](https://www.ncbi.nlm.nih.gov/sra/SRR33637628)

## Скрипт на bash с реализованным алгоритмом:
[Скрипт](bash/alg.sh)

Вывод программы:
```
[M::mm_idx_gen::0.113*0.76] collected minimizers
[M::mm_idx_gen::0.136*1.11] sorted minimizers
[M::main::0.459*0.42] loaded/built the index for 1 target sequence(s)
[M::mm_idx_stat] kmer size: 15; skip: 10; is_hpc: 0; #seq: 1
[M::mm_idx_stat::0.466*0.43] distinct minimizers: 838542 (98.18% are singletons); average occurrences: 1.034; average spacing: 5.352; total length: 4641652
[M::main] Version: 2.24-r1122
[M::main] CMD: minimap2 -d NC_000913.3.fasta.mmi NC_000913.3.fasta
[M::main] Real time: 0.471 sec; CPU: 0.203 sec; Peak RSS: 0.046 GB
[M::main::0.318*0.19] loaded/built the index for 1 target sequence(s)
[M::mm_mapopt_update::0.329*0.22] mid_occ = 12
[M::mm_idx_stat] kmer size: 15; skip: 10; is_hpc: 0; #seq: 1
[M::mm_idx_stat::0.336*0.23] distinct minimizers: 838542 (98.18% are singletons); average occurrences: 1.034; average spacing: 5.352; total length: 4641652
[M::worker_pipeline::10.499*1.89] mapped 22231 sequences
[M::main] Version: 2.24-r1122
[M::main] CMD: minimap2 -a NC_000913.3.fasta.mmi SRR33637628.fastq
[M::main] Real time: 10.502 sec; CPU: 19.804 sec; Peak RSS: 0.756 GB
85.01: NOT OK
```

## Результат команды samtools flagstat
```
25362 + 0 in total (QC-passed reads + QC-failed reads)
22231 + 0 primary
2115 + 0 secondary
1016 + 0 supplementary
0 + 0 duplicates
0 + 0 primary duplicates
21559 + 0 mapped (85.01% : N/A)
18428 + 0 primary mapped (82.89% : N/A)
0 + 0 paired in sequencing
0 + 0 read1
0 + 0 read2
0 + 0 properly paired (N/A : N/A)
0 + 0 with itself and mate mapped
0 + 0 singletons (N/A : N/A)
0 + 0 with mate mapped to a different chr
0 + 0 with mate mapped to a different chr (mapQ>=5)
```

## Инструкцию по развертыванию и установке фреймворка:
```bash
# Установка
pip install -U prefect
# Подключение
prefect cloud login # или prefect cloud login -k key
```
[Инструкция](https://www.prefect.io/get-started)

## Тестовый пайплайн

* Код:
  [`hello.py`](test/hello.py)

* Результаты выполнения и визуализация:
  [`view.png`](test/view.png)

## Пайплайн “Оценка качества картирования”

* Код:
  [`pipeline.py`](pipeline/pipeline.py)

* Результаты выполнения и визуализация:
  [`view.png`](pipeline/view.png)

* Визуализация в виде графа:
  [`graph.png`](pipeline/graph.png)

* Деплоймента:
  [`deployment.png`](pipeline/deployment.png)

## Визуализация пайплайна

Визуализация выполнена с помощью встроенного инструмента фреймворка Prefect – веб-интерфейса Prefect Server. После запуска пайплайна и его успешного выполнения, в разделе Flow Runs был выбран соответствующий запуск, и на вкладке Graph автоматически построен DAG, отражающий структуру потока данных между задачами.

В отличии от блок-схемы алгоритма фреймворк позволяет видеть время исполнения каждого этапа, в более удобной форме представлять входы/выходы каждого этапа (для программиста json точно удобнее блок схемы). Отдельные логи по каждому этапу
