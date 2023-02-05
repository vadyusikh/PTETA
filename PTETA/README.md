## Docker
For Ubuntu 

```commandline
sudo snap install docker
```

try to check docker

```commandline
docker images
```

If You get an error like this 

```commandline
Got permission denied while trying to connect to the Docker daemon socket at unix
```

```commandline
Got permission denied while trying to connect to the Docker daemon socket at unix
```

[Guide](https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket)

```commandline
sudo groupadd docker
sudo usermod -aG docker ${USER}
```
Restart machine



