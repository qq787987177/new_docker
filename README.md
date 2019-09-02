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

``````
server {
        listen 80;
        server_name  xxx.com www.xxx.com  ;
        root /var/www/xxx/;
        index   index.php  index.html;

        access_log /var/log/nginx/xxx.log aliyun_elk_format;
        error_log /var/log/nginx/xxx.error.log;

        if ($scheme != "https") {
            return 301 https://www.xxx.com$request_uri;
        } # managed by Certbot
}
server {
 
        listen 443 ssl http2;
        server_name  xxx.com www.xxx.com ;
        root /var/www/xxx/;
        index   index.php  index.html;

        access_log /var/log/nginx/xxx.access.log aliyun_elk_format;
        error_log /var/log/nginx/xxx.error.log;

        ssl_certificate /etc/letsencrypt/live/xxx.com/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/xxx.com/privkey.pem;
        ssl_trusted_certificate /etc/letsencrypt/live/xxx.com/chain.pem;

        include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot

    location / {
            if (!-e $request_filename) {
                rewrite ^(.*)$ /index.php?s=$1 last;
            }

    }


location ~ ^/(alipay|api|index|version|weixinpay)\.php($|/) {

                fastcgi_pass localhost:9000;

                fastcgi_split_path_info ^(.+\.php)(/.*)$;
                include fastcgi_params;
                fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
                fastcgi_param HTTPS off;
                fastcgi_param PHP_VALUE "upload_max_filesize=10M
                post_max_size=10M";

        }

location ~ ^/public/admin/js/ueditor/php/(controller)\.php($|/) {
                fastcgi_pass localhost:9000;

                fastcgi_split_path_info ^(.+\.php)(/.*)$;
                include fastcgi_params;
                fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
                fastcgi_param HTTPS off;
                fastcgi_param PHP_VALUE "upload_max_filesize=10M
                post_max_size=10M";

        }


location ~* /(runtime|upload)/(.*).(php)$ {
    return 403;
}

location ^~ /.git
{
    return 444;
}
location ~* \.(php|sql|ini|log)$ {
    return 404;
}

location ~* (makefile|composer\.json|composer\.phar)$ {
    return 404;
}



# letencrypt step1 , 
        location ^~ /.well-known/acme-challenge/ {
                default_type "text/plain";
                root     /usr/share/nginx/html;
        }

        location = /.well-known/acme-challenge/ {
                return 404;
        }

        location ~ .*\.(gif|jpg|jpeg|png|bmp|swf)$
        {
            expires      30d;
        }

        location ~ .*\.(js|css)?$
        {
           expires      12h;
        }

}
``````
