docker nginx 启动失败
原因：

开机时 apache2 自动启动，占用了80端口。

解决方法：

service apache2 stop
