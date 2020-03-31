FROM adtiming/omjava
MAINTAINER openmediation
ENV SYSTEMNAME "omsystem"
ENV SERVERNAME "om-dtask"
ARG JAR_FILE
RUN mkdir -p ${SERVERNAME}/cache
ADD ./docker/docker-entrypoint.sh /
ADD ./docker/application-loc.yml /${SERVERNAME}/
ADD ${JAR_FILE} /${SERVERNAME}/
ADD ./docker/rsync /bin/
ADD ./docker/rsyncd.conf /etc/
WORKDIR /
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:19012/ || exit 1
EXPOSE 19012
ENTRYPOINT ["/docker-entrypoint.sh"] 
CMD ["om-dtask"]
