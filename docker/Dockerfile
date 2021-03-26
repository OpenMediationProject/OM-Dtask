FROM adtiming/ombuilder as builder
ARG BUILDER_URL="https://github.com/AdTiming/OM-Dtask.git"
ARG BUILDER_DIR="OM-Dtask"
RUN git clone ${BUILDER_URL} && \
    cd ${BUILDER_DIR} && \
    mvn clean package -Dmaven.test.skip=true

FROM adtiming/omjava
MAINTAINER openmediation
ENV SYSTEMNAME "omsystem"
ENV SERVERNAME "om-dtask"
ARG GET_BUILDER_DIR="/data/OM-Dtask"
RUN yum install -y rsync && \
    mkdir -p ${SERVERNAME}/{cache,log}
ADD docker-entrypoint.sh /
COPY --from=builder ${GET_BUILDER_DIR}/src/main/resources/application-prod.yml /${SERVERNAME}/application-loc.yml
ADD rsyncd.conf /etc/
COPY --from=builder ${GET_BUILDER_DIR}/target/${SERVERNAME}.jar /${SERVERNAME}/
WORKDIR /
RUN chmod +x /docker-entrypoint.sh /bin/rsync /${SERVERNAME}/${SERVERNAME}.jar
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:19012/ || exit 1
EXPOSE 19012 873
ENTRYPOINT ["/docker-entrypoint.sh"] 
CMD ["om-dtask"]
