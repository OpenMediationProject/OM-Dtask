CREATE TABLE IF NOT EXISTS ods_om_adnetwork
    USING JDBC
    OPTIONS (
                'url'='jdbc:mysql://[(${rdsJdbcUrl})]',
                'driver'='com.mysql.jdbc.Driver',
                'dbtable'='[(${rdsDatabase})].om_adnetwork',
                'passwdauth' = '[(${rdsPasswdAuth})]',
                'encryption' = 'true'
);
