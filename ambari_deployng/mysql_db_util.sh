#!/usr/bin/env bash

drop_user()
{
    echo "Dropping mysql user $1"
    mysql -u root<<MYSQL_INPUT
    DROP USER $1@'%';
MYSQL_INPUT
}

create_user()
{
    echo "Creating mysql user $1"
    mysql -u root <<MYSQL_INPUT
    CREATE USER $1@'%' IDENTIFIED BY '$2';
    GRANT ALL PRIVILEGES ON *.* TO $1@'%' with grant option;
    FLUSH PRIVILEGES;
MYSQL_INPUT
}


create_db()
{
    echo "Creating mysql db $1"
    mysql -u root<<MYSQL_INPUT
    CREATE DATABASE $1;
MYSQL_INPUT
}

drop_db()
{
    echo "Dropping mysql db $1"
    mysql -u root<<MYSQL_INPUT
    DROP DATABASE $1;
MYSQL_INPUT
}

setup_service_db_and_user()
{
    drop_user $2
    drop_db $1
    create_db $1
    create_user $2 $3

}

setup_ranger_db()
{
     echo "Dropping mysql db for Ranger"
    mysql -u root<<MYSQL_INPUT
    grant all privileges on *.* to ambaricustomuser@'%' with grant option;
    grant all privileges on *.* to root@'%' with grant option;
    SET PASSWORD FOR 'root'@'%' = PASSWORD('$1');
    flush privileges;
    quit
MYSQL_INPUT

}

# call arguments verbatim:
$@