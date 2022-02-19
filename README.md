# log-splitter
Spark를 사용하여 진행하는 ETL 사이드 프로젝트 repository.

## 1. Description

- json log를 읽어서 용도별로 분리해서 parquet로 저장하는 `Spark` batch job.


## 2. Environment

### 2-1. Requirements

| requirements    |  version  |
|:---------------:|:-----------:|
| Scala           |   2.12.13   |
| Java            |   ≥ 1.8     |
| SBT             |   1.6.1     |


## 3. How to run

```bash
bash bin/build.sh
bash bin/run.sh
```
