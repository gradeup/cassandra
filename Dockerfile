FROM openjdk:8u151-jre-alpine3.7
# Setup cqlsh dependencies
RUN apk add --no-cache python
RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python /tmp/get-pip.py
RUN pip install cassandra-driver
ENV CQLSH_NO_BUNDLED=TRUE

# Add cassandra
RUN mkdir /cassandra
RUN CASSANDRA_HOME=/cassandra
ADD ./cassandra /cassandra/
RUN PATH=$PATH:/cassandra/bin

WORKDIR /cassandra/bin
ENTRYPOINT ["/cassandra/bin/cassandra", "-fR"]
