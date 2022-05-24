set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
create table hive_test_src ( col1 string ) partitioned by (pcol1 string) stored as textfile;
alter table hive_test_src add partition (pcol1 = 'test_part');
set hive.security.authorization.enabled=true;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src partition (pcol1 = 'test_part');
