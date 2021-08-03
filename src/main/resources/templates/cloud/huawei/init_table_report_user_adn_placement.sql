CREATE TABLE IF NOT EXISTS report_user_adn_placement
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_dau_adn_placement',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
