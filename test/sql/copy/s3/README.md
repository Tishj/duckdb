
In order to test these locally, `minio` is used. This requires Docker to be installed.

### Installing Docker on MacOS

Install `docker` using `homebrew`.


```bash
brew install docker --cask
```

Then open `/Applications/Docker`. Note that the first time you open the application you need to go to the `Applications` folder, right-click `Docker` and select `open`.

### Setting Up Docker

In order to finish setting up Docker, you need to open the Docker application, and login to your Docker account. Create a Docker account if you do not have one and finish setting up.

### Running Minio

Run the `install_s3_test_server` script. This requires root. This makes a few changes to your system, specifically to `/etc/hosts` to set up a few redirect interfaces to localhost. This only needs to be run once.

```bash
sudo ./scripts/install_s3_test_server.sh
```

Then, if this has not been done yet, we need to generate some data:

```
./scripts/generate_presigned_url.sh
```

Then run the test server in the back-ground using Docker. Note that Docker must be opened for this to work. On MacOS you can open the docker gui (`/Applications/Docker`) and leave it open to accomplish this.


```bash
source ./scripts/run_s3_test_server.sh
```

Now set up the following environment variables to enable running of the tests.

This can be done either manually:
```bash
export S3_TEST_SERVER_AVAILABLE=1
export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=minio_duckdb_user
export AWS_SECRET_ACCESS_KEY=minio_duckdb_user_password
export DUCKDB_S3_ENDPOINT=duckdb-minio.com:9000  
export DUCKDB_S3_USE_SSL=false
```

Or using the `set_s3_test_server_variables.sh` script  

```bash
# use source so it sets the environment variables in your current environment
source scripts/set_s3_test_server_variables.sh
```

Now you should be able to run the S3 tests using minio, e.g.:

```bash
build/debug/test/unittest test/sql/copy/s3/s3_hive_partition.test
```

> minio uses port 9000. Clickhouse also uses port 9000. If the tests are not working and you have a running Clickhouse service - try killing it first, e.g. using `killall -9 clickhouse`

#### Test Data

The configuration for minio is stored in `scripts/minio_s3.yml`. Data is stored in `/tmp/minio_test_data`.