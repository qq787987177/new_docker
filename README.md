docker nginx 启动失败
原因：

开机时 apache2 自动启动，占用了80端口。

解决方法：

service apache2 stop

``````
mysql主从

master:

CREATE USER 'slave'@'%' IDENTIFIED BY '123456';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'slave'@'%';

show master status;

slave:

change master to master_host='172.50.0.5', master_user='slave', master_password='123456', master_port=3306, master_log_file='mysql-bin.000001', master_log_pos= 2830, master_connect_retry=30;

start slave;
show slave status \G;
``````
