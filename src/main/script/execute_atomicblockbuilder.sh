#!/usr/bin/env bash

set -ue

initialChecks() {
    # Check that we have a valid Kerberos ticket
    # (Based on https://www.jamf.com/jamf-nation/discussions/9523/shell-script-with-output-if-there-is-a-kerberos-ticket#responseChild52469)
    kticket=`klist | grep skyetlpr@SKY.LOCAL`
    if [ -n "$kticket" ]; then
        TODAY=$(date +"%m/%d/%Y %T")
        TICKET_EXPIRY=$(echo `klist` | awk '{print $14 " " $15}')
        ts1=`date -d"${TODAY}" +%Y%m%d%H%M%S`
        ts2=`date -d"${TICKET_EXPIRY}" +%Y%m%d%H%M%S`

        if [ $ts2 -ge $ts1 ]; then
        	echo "$(date +"%Y-%m-%d %T") Kerberos ticket valid until ${TICKET_EXPIRY}"
        else
        	echo "$(date +"%Y-%m-%d %T") Kerberos ticket expired on ${TICKET_EXPIRY}"
        	renewKerberosTicket
        fi

    else
        echo "$(date +"%Y-%m-%d %T") No valid Kerberos ticket found"
        renewKerberosTicket
    fi
}

renewKerberosTicket() {
    echo "$(date +"%Y-%m-%d %T") Renewing Kerberos ticket via keytab file..."

    cd /home/skyetlpr
    kinit -kt skyetlpr.keytab skyetlpr@SKY.LOCAL
    cd -
    if [ "$?" -ne 0 ]; then
        echo "$(date +"%Y-%m-%d %T")     ERROR: Unable to renew Kerberos ticket"
        exit 1
    else
        echo "$(date +"%Y-%m-%d %T")     Successfully renewed Kerberos ticket"
    fi
}

initialChecks

# Get the latest JAR from AIP Jenkins

echo "$(date +"%Y-%m-%d %T") Getting latest JAR from Jenkins..."
CRUMB=$(curl -s 'http://build:d479af1985595008d508f45d2fe5d012@10.127.3.27:8080/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,":",//crumb)')
curl -X POST -H "$CRUMB" -O "http://build:d479af1985595008d508f45d2fe5d012@10.127.3.27:8080/view/Primary_Research/job/PR_Strategic_Data_Master_Build/ws/primary_research_karthik/target/primary_research-0.1-SNAPSHOT.jar"
if [ "$?" != "0" ]; then
    echo "$(date +"%Y-%m-%d %T")   ERROR: Unable to get latest JAR from Jenkins"
    exit 1
else
   echo "$(date +"%Y-%m-%d %T")   Successfully got latest JAR from Jenkins"
fi

# Run the extraction JAR
# Note that the Java HeapSpace needed to be increased as the HTTP request was crashing
echo "$(date +"%Y-%m-%d %T") Beginning Survey XML Parser job..."

    spark-submit \
        --master local[*] \
        --class com.sky.AtomicBlockBuilder \
        --conf spark.ui.port=4042 \
        --conf spark.executor.memory=4g \
        --conf spark.driver.memory=4g \
        --conf spark.rdd.compress=true \
        --conf spark.app.name="AtomicBlockBuilder" \
        --num-executors 10 \
        --executor-cores 1 \
        primary_research-0.1-SNAPSHOT.jar 10

if [ "$?" != "0" ]; then
    echo "$(date +"%Y-%m-%d %T")   ERROR: Survey XML Parser job was unsuccessful"
    exit 1
else
   echo "$(date +"%Y-%m-%d %T")   Survey XML Parser job was successful"
fi

# Save log file
#mv primary_research_decrypt_extraction.log /projects/strategic_data/pr/logs/response/$(date +"%Y-%m-%d_%T_pr_response_data.log")