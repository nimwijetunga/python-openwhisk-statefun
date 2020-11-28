docker rm -f $(docker ps -a -q)
docker volume rm $(docker volume ls -q)
./build-example.sh
docker-compose up -d
