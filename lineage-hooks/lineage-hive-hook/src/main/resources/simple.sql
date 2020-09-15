use test;

DROP TABLE lineage_test001;

create table lineage_test001
(
    c1 INT,
    c2 STRING
) STORED AS PARQUET;

insert overwrite table lineage_test001
SELECT 1, "Hello"
UNION ALL
SELECT 2, "World";

drop table lineage_test002;

create table lineage_test002
(
    c1 INT,
    c2 STRING
) STORED AS PARQUET;

INSERT OVERWRITE TABLE lineage_test002
SELECT *
FROM lineage_test001;

SELECT C1, C2
FROM lineage_test002;

drop table if exists lineage_test002_1;

CREATE TABLE lineage_test002_1
(
    c1_cnt INT,
    c2_name String
) PARTITIONED BY (dayid STRING)
STORED AS PARQUET;

INSERT OVERWRITE TABLE lineage_test002_1 PARTITION (dayid='20200907');

SELECT SUM(z1.c1 + z2.c1), z1.c2
FROM lineage_test001 z1
         INNER JOIN lineage_test002 z2 ON z1.c1 = z2.c1
GROUP BY z1.c2;