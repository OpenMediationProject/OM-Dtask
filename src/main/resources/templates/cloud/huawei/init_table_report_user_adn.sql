CREATE TABLE IF NOT EXISTS report_user_adn
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_dau_adn',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
