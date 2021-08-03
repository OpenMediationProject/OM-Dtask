CREATE TABLE IF NOT EXISTS report_pubapp_country
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].stat_pub_app_country_uar',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
