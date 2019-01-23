# <a name="top"></a>Draco docker
Content:

* [Before starting](#section1)
* [Getting an image](#section2)
    * [Building form sources](#section2.1)
    * [Using docker hub image](#section2.2)
* [Using the image](#section3)
    * [As it is](#section3.1)

## <a name="section1"></a>Before starting
Obviously, you will need docker installed and running in you machine. Please, check [this](https://docs.docker.com/linux/started/) official start guide.

[Top](#top)

## <a name="section2"></a>Getting an image
### <a name="section2.1"></a>Building from sources
Start by cloning the `fiware-Draco` repository:

    git clone https://github.com/ging/fiware-Draco.git
    cd fiware-Draco
    git checkout release/2.0.0

Change directory:

    cd nifi-ngsi-resources/docker

And run the following command:

    sudo docker build -f ./Dockerfile -t Draco .

Once finished (it may take a while), you can check the available images at your docker by typing:

```
$ docker images
REPOSITORY          TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
Draco              latest              6a9e16550c82        10 seconds ago      462.1 MB
centos              6                   273a1eca2d3a        2 weeks ago         194.6 MB
```

[Top](#top)

### <a name="section2.2"></a>Using docker hub image
Instead of building an image from the scratch, you may download it from [hub.docker.com](https://hub.docker.com/ging/Draco):

    $ docker pull ging/fiware-draco

It can be listed the same way than above:

```
$ docker images
REPOSITORY          TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
Draco              latest              6a9e16550c82        10 seconds ago      462.1 MB
centos              6                   273a1eca2d3a        2 weeks ago         194.6 MB
```

[Top](#top)

## <a name="section3"></a>Using the image
### <a name="section3.1"></a>As it is
The Draco image (either built from the scratch, either downloaded from hub.docker.com) allows running a Draco in charge of receiving NGSI-like notifications and persisting them into wide variety of storages: MySQL (Running in a  `iot-mysql` host), MongoDB, etc.

Start a container for this image by typing in a terminal:

    $ docker run --name draco -p 8080:8080 -p 5050:5050 -d ging/fiware-draco 

Immediately after, you will start seeing Draco-ngsi you can access to the Draco 
GUI Interface putting this direction into your browser (http://localhost:8080/nifi)

You can check the running container (in a second terminal shell):

```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                CREATED              STATUS              PORTS                NAMES
9ce0f09f5676        Draco            "/entrypoint.   About a minute ago   Up About a minute   5050/tcp, 8081/tcp   focused_kilby
```

You can check the IP address of the container above by doing:

```
$ docker inspect 9ce0f09f5676 | grep \"IPAddress\"
        "IPAddress": "172.17.0.13",
```

You can stop the container as:

```
$ docker stop 9ce0f09f5676
9ce0f09f5676
$ docker ps
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
```

Now you can continue and go tho the [Quick Start Guide](../quick_start_guide.md) for testing your deployment with 
a MySQL database.



[Top](#top)

If you want to use a specific configuration of Draco you can use the guide 
provided by the official repository of NIFI (https://hub.docker.com/r/apache/nifi/)
since Draco was built using the 
same engine as NIFi but including some additional processors and templates.
[Top](#top)
