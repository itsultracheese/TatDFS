rm -r /var/lib/docker/volumes/namenode/_data/*
rm -r /var/lib/docker/volumes/datanode1/_data/*
rm -r /var/lib/docker/volumes/datanode2/_data/*
rm -r /var/lib/docker/volumes/datanode3/_data/*
docker rmi -f datanode1:latest datanode2:latest datanode3:latest namenode:latest
docker build -t namenode namenode/
docker build -t datanode1 datanode1/
docker build -t datanode2 datanode2/
docker build -t datanode3 datanode3/



