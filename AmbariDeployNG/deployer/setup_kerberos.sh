#!/usr/bin/env bash

install_kerberos()
{
    echo -e "\n Installing kerberos RPMs"
    yum install krb5-server krb5-libs krb5-auth-dialog rng-tools krb5-workstation -y
    echo -e "\n Configuring Kerberos"
    sed -i "s/kdc = kerberos.example.com/kdc = $1/g" /etc/krb5.conf
    sed -i "s/admin_server = kerberos.example.com/admin_server = $1/g" /etc/krb5.conf
    chmod 644 /etc/krb5.conf
    cat /etc/krb5.conf
    echo -ne '\n\n' |  kdb5_util create -s
    mkdir - /etc/security/keytabs/
    echo "Creating admin principal $3 with password $2"
    kadmin.local -q "addprinc -pw $2 $3"
    /sbin/service krb5kdc start
    /sbin/service kadmin start
}

install_kerberos $1 $2 $3