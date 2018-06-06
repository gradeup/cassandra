# Cassandra Docker Images

This repo provides docker images to run a single node cluster on dev machines and is tuned for fast container startup.
Only cassandra 3.11 is supported currently.

## Optimizations
- Alpine based base image
- Updated cqlsh
- Disabled vnodes
- Disabled "waiting for gossip to settle down"
- Extra libs for gradeup specific customizations

## Usage

``` 
docker pull gradeup/cassandra:{$tag} 
```

Using docker compose:
```
cassandra:
  image: "gradeup/cassandra:{$tag}"
  container_name: "cassandra"
  volumes:
    - ./cassandra/data:/cassandra/data
    - ./cassandra/conf:/cassandra/conf
  ports:
    - 9042:9042
    - 9160:9160
```

## Credits
* [docker-cassandra](https://github.com/spotify/docker-cassandra)
