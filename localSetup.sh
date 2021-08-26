#!/bin/bash
curl -o file.zip https://atlan-build-artifacts.s3.ap-south-1.amazonaws.com/atlas/deploy.zip
tar -xvf file.zip  && rm file.zip


mvn_version=$(mvn --version)
docker_compose=$(docker-compose --version)
if [[ $mvn_version == *"Maven"* ]]; 
then 
    echo $mvn_version
    if [[ $mvn_version == *"Maven 3.6.3"* ]]; 
    then
    echo "Maven Version 3.6.3"
    echo "Building Projects & packages to JAR files"
    echo "Running--> mvn clean -DskipTests package -Pdist -Drat.skip=true"
    mvn clean -DskipTests package -Pdist -Drat.skip=true
    if [[ $docker_compose == *"docker-compose version"* ]];
    then 
    echo "docker-compose present $docker_compose"
    docker-compose -f atlas_local.yaml up
    else
    echo "Problem finding docker compose"
    fi
    else
    echo "Maven Version mismatch, please check for maven version using mvn --version"
    fi
else
    echo "maven not installed or added to PATH properly."
fi




