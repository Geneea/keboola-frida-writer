# Keboola Connection writer for Geneea Frida app

Integration of the [Geneea Frida](https://frida.geneea.com) with [Keboola Connection](https://connection.keboola.com).

This is a Docker container used for exporting Geneea NLP analysis results from KBC to Frida app.
Automatically built Docker images are available at [Docker Hub Registry](https://hub.docker.com/r/geneea/keboola-frida-writer/).

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

## Sample configuration
Mapped to `/data/config.json`

```
{
  "storage": {
    "input": {
      "tables": [
        {
          "destination": "analysis-result-full.csv"
        }
      ]
    }
  },
  "parameters": {
    "dataset": "<ENTER TARGET DATASET HERE>",
    "username": "<ENTER FRIDA USERNAME HERE>",
    "#password": "<ENTER FRIDA PASSWORD HERE>",
    "columns": {
      "id": "doc_id",
      "binaryData": "binaryData",
      "datetime": "date",
      "metadata": ["meta_1", "meta_2"],
      "metadataMultival": ["meta_3"]
    }
  }
}
```
