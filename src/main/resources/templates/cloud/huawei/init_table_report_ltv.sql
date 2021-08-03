CREATE TABLE IF NOT EXISTS report_ltv
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_user_ltv',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
