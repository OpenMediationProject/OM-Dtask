CREATE TABLE IF NOT EXISTS ods_stat_adnetwork
USING JDBC
OPTIONS (
       'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
       'driver'='com.mysql.jdbc.Driver',
       'dbtable'='[(${rdsDatabase})].stat_adnetwork',
       'passwdauth' = '[(${rdsPasswdAuth})]',
       'encryption' = 'true'
);
