#!/usr/bin/env bash

install_kerberos()
{
    echo -e "\n Installing kerberos RPMs"
    yum install krb5-server krb5-libs krb5-auth-dialog rng-tools krb5-workstation -y
    echo -e "\n Configuring Kerberos"
    if [ $1 -q 'mit' ]
    then
        echo "Configuring KRB for MIT"
        cp conf/mit_krb.conf /etc/krb5.conf
        sed -i "s/ADMIN_HOST_PH/ $2/g" /etc/krb5.conf
        sed -i "s/KDC_HOST_PH/ $2/g" /etc/krb5.conf
    fi
    chmod 644 /etc/krb5.conf
    cat /etc/krb5.conf
    #TODO : Setup for SUSE And UBUNTU need to be done
    echo -ne '\n\n' |  kdb5_util create -s
    #mkdir - /etc/security/keytabs/
    echo "Creating admin principal $4 with password $3"
    kadmin.local -q "addprinc -pw $3 $4"
    /sbin/service krb5kdc start
    /sbin/service kadmin start
    #TODO: Ambari security setup : https://docs.hortonworks.com/HDPDocuments/Ambari-2.1.2.0/bk_Ambari_Security_Guide/content/_optional_set_up_kerberos_for_ambari_server.html

}

install_kerberos $1 $2 $3