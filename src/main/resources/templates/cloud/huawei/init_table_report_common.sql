CREATE TABLE IF NOT EXISTS report_common
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_lr',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
