if [[ -z "$1" ]]; then
	echo "Syntax:"
	echo "run <server_user> [silent]"
	exit
fi

SPARK_STREAM=
if [ "$2" == "silent" ]; then
    SPARK_STREAM="2>/dev/null"
fi

# Configuration variables
MAIN_SCRIPT="MLPipeline.py"
SERVER_USER=$1
SERVER_HOST=153.19.52.196

# Script variables
REL_DIR="`dirname \"$0\"`/.."
ABS_DIR="`( cd \"$REL_DIR\" && pwd )`"
SERVER_ADDR=$SERVER_USER@$SERVER_HOST

ssh $SERVER_ADDR "rm -rf ~/ztnbd && mkdir ~/ztnbd 2>/dev/null"
scp "$ABS_DIR/Pipfile" "$SERVER_ADDR:~/ztnbd/Pipfile"
scp "$ABS_DIR/Pipfile.lock" "$SERVER_ADDR:~/ztnbd/Pipfile.lock"
scp -r "$ABS_DIR/src" "$SERVER_ADDR:~/ztnbd/src"
scp -r "$ABS_DIR/resources" "$SERVER_ADDR:~/ztnbd/resources"

ssh $SERVER_ADDR << SSH_SESS
	cd ztnbd
	pipenv install
	hdfs dfs -mkdir -p /user/TZ/$SERVER_USER/ztnbd
	hdfs dfs -copyFromLocal -f resources/pages/*.html /user/TZ/$SERVER_USER/ztnbd/
	echo "===================== $MAIN_SCRIPT ====================="
	pipenv run spark-submit --conf spark.ui.enabled=true "src/$MAIN_SCRIPT" $SERVER_USER $SPARK_STREAM
SSH_SESS