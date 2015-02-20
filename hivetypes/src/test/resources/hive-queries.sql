drop table if exists t;
create table t(a int, b string);
drop table if exists t2;
create table t2 as select * from t;
