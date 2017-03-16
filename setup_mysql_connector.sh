#!/usr/bin/env bash

setup_mysql_connector()
{
    echo -e "\n Setup Mysql"
    yum -y install wget
    yum -y mysql-connector-java
    rm -rf /usr/share/java/mysql-connector-java*.jar
    mkdir -p /tmp/mysql-connector-java
    wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.33.tar.gz -O /tmp/mysql-connector-java.tar.gz >/dev/null 2>&1
    tar -zxvf /tmp/mysql-connector-java.tar.gz -C /tmp/mysql-connector-java >/dev/null
    mkdir -p /usr/share/java
    cp /tmp/mysql-connector-java/mysql-connector-java-*/mysql-connector-java-*-bin.jar /usr/share/java/mysql-jdbc-qe.jar
    chmod -R 777 /usr/share/java
    unlink /usr/share/java/mysql-connector-java.jar
    rm -f /usr/share/java/mysql-connector-java.jar
    ln -s /usr/share/java/mysql-jdbc-qe.jar /usr/share/java/mysql-connector-java.jar
}

setup_mysql_connector