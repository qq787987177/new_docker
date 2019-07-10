FROM debian:9

COPY sources.list /etc/apt/sources.list
COPY phpredis /etc/phpredis
COPY xdebug /etc/xdebug
COPY composer /usr/local/bin/composer
COPY start.sh /

WORKDIR /var/www/html

RUN apt-get update && apt -y install apt-transport-https ca-certificates wget \
    && wget -O /etc/apt/trusted.gpg.d/php.gpg https://packages.sury.org/php/apt.gpg \
    && sh -c 'echo "deb https://packages.sury.org/php/ stretch main" > /etc/apt/sources.list.d/php.list' \
    && apt update \
    && apt-get install -y php7.3 php7.3-zip zip unzip php7.3-common php7.3-fpm php7.3-mbstring php7.3-xml php7.3-json php7.3-gd php7.3-curl php7.3-gmp php7.3-mysql php7.3-dev \
    && sed -i 's/;date.timezone =/date.timezone = "Asia\/Shanghai"/g' /etc/php/7.3/fpm/php.ini \
    && sed -i 's/;date.timezone =/date.timezone = "Asia\/Shanghai"/g' /etc/php/7.3/cli/php.ini \
    && cd /etc/phpredis \
    && phpize && ./configure --with-php-config=/usr/bin/php-config && make && make install \
    && cd /etc/xdebug && ./rebuild.sh \
    && sed -i 's/;extension=xsl/;extension=xsl\n extension=redis.so \n extension=xdebug.so/g' /etc/php/7.3/fpm/php.ini \
    && sed -i 's/;extension=xsl/;extension=xsl\n extension=redis.so \n extension=xdebug.so/g' /etc/php/7.3/cli/php.ini \
    && chmod +x /usr/local/bin/composer \
    && composer config -g repo.packagist composer https://packagist.phpcomposer.com \
    && apt-get remove -y wget apt-transport-https ca-certificates \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /etc/phpredis

ENTRYPOINT ["/start.sh"]
