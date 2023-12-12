**Prerequisites:**
**This project is made to be run on MacOS with a silicon chip**

Give executable permission to requirements.sh - `chmod +x requirements.sh`

Execute the file - ./requirements.sh

**Setup Minikube and Kuburnetes cluster:**

Start minikube with max config available, Let's say you have a M1 chip Macbook Pro with 16 GB memory - `minikube start --driver=qemu --cpus=8 --memory=14000 --disk-size=40g`

Verify that minikube started by running the command `minikube dasboard`

Setup Cassandra - go to the folder "cassandra-spark-configs" in the project directory and run the script `cassandra-pod.sh` with executable permission, this will create a pod of cassandra in the kuburnetes cluster, you can see it in the dashboard.

Setup Spark - run this command `helm install my-spark bitnami/spark --version 8.1.6` and it will create three pods in your kuburnetes cluster, one for master and 2 worker nodes.

**Populate Cassandra with data:**

This is our original dataset : https://www.kaggle.com/datasets/azathoth42/myanimelist
After cleaning the data according to our analysis requirements: https://www.dropbox.com/scl/fo/s9mbebw8tjcfguq5ff7k5/h?rlkey=jbfc5y3ne83y8fbub3o7fns30&dl=0

Download the cleaned .csv files from above dropbox link and then move them inside the folder `/bigdata` project, give executable permission to all the shell scripts inside /bigdata

Run copy_bigdata.sh, import_bigdata.sh, copy_animeinfo.sh, import_animeinfo.sh in order, and wait for the script to be completely executed before starting another one.

**Run the spark analysis:**

Build the project using `mvn clean package` and copy the shade jar to master node of spark - `Kubectl [path-to-the-jarfile] default/my-spark-master-0:/opt/bitnami/spark`, you can find the jar file in the /target folder after building the project.

Finally run the analysis with - `./bin/spark-submit --class com.project.AniflowApplication \
--master spark://my-spark-master-0.my-spark-headless.default.svc.cluster.local:7077 \
--num-executors 2 --driver-memory 1g --driver-cores 1 --executor-memory 3g --executor-cores 3 \
aniflow-0.0.1-SNAPSHOT.jar "10.244.0.16" "9042" "gender_analysis"`

The first parameter is cassandra's ip address which you can get from `kubectl describe pod cassandra-0 | grep "IP:"`, second parameter is cassandra's port which is 9042 by default, and third parameter is the type of analysis you want to run.

**Types of analysis:**

`gender_analysis` - For analysing gender distribution

`age_analysis` - For age distribution

`age_gender_analysis` - For age-gender distribution

`anime_score_analysis` - For average anime score analysis

`anime_genre_analysis` - For getting best genre 

`genre_analysis` - For getting genre distribution

`genre_analysis and <Name-of-Studio>` - For getting studio portfolio
