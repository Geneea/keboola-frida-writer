# Keboola Connection writer for Geneea Frida app


## Building a container
To build this container manually one can use:

```
git clone https://github.com/Geneea/keboola-frida-writer.git
cd keboola-frida-writer
sudo docker build --no-cache -t geneea/keboola-frida-writer .
```

## Running a container
This container can be run from the Registry using:

```
sudo docker run \
--volume=/home/ec2-user/data:/data \
--rm \
geneea/keboola-frida-writer:latest
```
Note: `--volume` needs to be adjusted accordingly.
