FROM atlas

USER ubuntu

WORKDIR /home/ubuntu

COPY ./distro/src/bin/atlas_config.py ./atlas-bin/bin/atlas_config.py
COPY ./distro/src/bin/atlas_start.py ./atlas-bin/bin/atlas_start.py
COPY ./distro/src/bin/startAtlas.sh ./atlas-bin/bin/startAtlas.sh
COPY ./distro/src/conf/atlas-log4j.xml ./atlas-bin/conf/atlas-log4j.xml
ENV DRY=true