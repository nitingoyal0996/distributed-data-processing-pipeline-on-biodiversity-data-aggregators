#!/bin/bash
# fetch the latest version#

# release_url="https://github.com/provectus/kafka-ui/releases"
# latest=$(basename $(curl -fsSLI -o /dev/null -w  %{url_effective} ${release_url}/latest))
# download the binary
curl -o "kafka-ui-api.jar" -L "https://github.com/provectus/kafka-ui/releases/download/v0.7.1/kafka-ui-api-v0.7.1.jar"
# get a new url to download proper file - this is wrong 
# curl -o "application-local.yml" -L "https://github.com/provectus/kafka-ui/blob/master/kafka-ui-api/src/main/resources/application-local.yml"

# execute the jar
java -Dspring.config.additional-location=my.yml --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED -jar kafka-ui-api.jar