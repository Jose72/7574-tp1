sudo docker build -f Dockerfile.server_get -t 7574-tp1-server-get .
sudo docker build -f Dockerfile.server_post -t 7574-tp1-server-post .
sudo docker build -f Dockerfile.database -t 7574-tp1-db .
sudo docker-compose up

