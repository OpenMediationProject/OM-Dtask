CREATE TABLE IF NOT EXISTS report_user_instance
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_dau_instance',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
