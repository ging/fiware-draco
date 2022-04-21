# Draco docker

Content:

-   [Before starting](#section1)
-   [Getting an image](#section2)
    -   [Building form sources](#section2.1)
    -   [Using docker hub image](#section2.2)
-   [Using the image](#section3)
    -   [As it is](#section3.1)

## Before starting

Obviously, you will need docker installed and running in you machine. Please, check
[this](https://docs.docker.com/linux/started/) official start guide.

## Getting an image

### Building from sources

Start by cloning the `fiware-Draco` repository:

```bash
    git clone https://github.com/ging/fiware-Draco.git
    cd fiware-Draco
    git checkout release/2.0.0
```

Change directory:

```bash
    cd nifi-ngsi-resources/docker
```

And run the following command:

```bash
    sudo docker build -f ./Dockerfile -t Draco .
```

Once finished (it may take a while), you can check the available images at your docker by typing:

```text
$ docker images
REPOSITORY          TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
Draco              latest              6a9e16550c82        10 seconds ago      462.1 MB
centos              6                   273a1eca2d3a        2 weeks ago         194.6 MB
```

### Using docker hub image

Instead of building an image from the scratch, you may download it from
[hub.docker.com](https://hub.docker.com/ging/Draco):

```bash
    $ docker pull ging/fiware-draco
```

It can be listed the same way than above:

```text
$ docker images
REPOSITORY          TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
Draco              latest              6a9e16550c82        10 seconds ago      462.1 MB
centos              6                   273a1eca2d3a        2 weeks ago         194.6 MB
```

## Using the image

### As it is

The Draco image (either built from the scratch, either downloaded from hub.docker.com) allows running a Draco in charge
of receiving NGSI-like notifications and persisting them into wide variety of storages: MySQL (Running in a `iot-mysql`
host), MongoDB, etc.

Start a container for this image by typing in a terminal:

```bash
    $ docker run --name draco -p 8443:8443 -p 5050:5050 -d ging/fiware-draco
```

Immediately after, you will start seeing Draco-ngsi you can access to the Draco GUI Interface putting this direction
into your browser `https://localhost:8443/nifi`

You can check the running container (in a second terminal shell):

```text
$ docker ps
CONTAINER ID        IMAGE               COMMAND                CREATED              STATUS              PORTS                NAMES
9ce0f09f5676        Draco            "/entrypoint.   About a minute ago   Up About a minute   5050/tcp, 8443/tcp   focused_kilby
```

You can check the IP address of the container above by doing:

```bash
$ docker inspect 9ce0f09f5676 | grep \"IPAddress\"
        "IPAddress": "172.17.0.13",
```

You can stop the container as:

```text
$ docker stop 9ce0f09f5676
9ce0f09f5676
$ docker ps
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
```

Now you can continue and go tho the [Quick Start Guide](../quick_start_guide.md) for testing your deployment with a
MySQL database.

If you want to use a specific configuration of Draco you can use the guide provided by the official repository of
[NIFI](https://hub.docker.com/r/apache/nifi/) since Draco was built using the same engine as NIFi but including some
additional processors and templates.
