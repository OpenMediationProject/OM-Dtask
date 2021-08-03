CREATE TABLE IF NOT EXISTS report_user
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_dau',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
