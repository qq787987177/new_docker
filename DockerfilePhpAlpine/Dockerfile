FROM alpine:3.10

COPY start.sh /

RUN mkdir www \
&& sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories \
&& apk update \
&& apk add php7 php7-zip zip unzip php7-fpm php7-mbstring php7-xml php7-json php7-gd php7-curl php7-gmp php7-mysqli php7-mysqlnd php7-pdo php7-pdo_mysql php7-pdo_mysql php7-pecl-redis php7-pecl-xdebug composer \
&& sed -i 's/;date.timezone =/date.timezone = "Asia\/Shanghai"/g' /etc/php7/php.ini \
&& composer config -g repo.packagist composer https://packagist.phpcomposer.com \
&& rm -rf /var/cache/apk/*

WORKDIR /www
ENTRYPOINT ["/start.sh"]
