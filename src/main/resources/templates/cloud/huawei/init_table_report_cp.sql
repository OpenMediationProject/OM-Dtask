CREATE TABLE IF NOT EXISTS report_cp
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_cp',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
