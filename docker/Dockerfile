FROM adtiming/omjava
MAINTAINER openmediation
ENV SYSTEMNAME "omsystem"
ENV SERVERNAME "om-dtask"
ARG JAR_FILE
RUN mkdir -p ${SERVERNAME}/cache
ADD docker-entrypoint.sh /
ADD application-loc.yml /${SERVERNAME}/
COPY om-dtask.jar /${SERVERNAME}/om-dtask.jar
ADD rsync /bin/
ADD rsyncd.conf /etc/
WORKDIR /
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:19012/ || exit 1
EXPOSE 19012
ENTRYPOINT ["/docker-entrypoint.sh"] 
CMD ["om-dtask"]
