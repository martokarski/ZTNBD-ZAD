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
MAIN_SCRIPT="PipelineCV"
SERVER_USER=$1
SERVER_HOST=153.19.52.196

# Script variables
REL_DIR="`dirname \"$0\"`/.."
ABS_DIR="`( cd \"$REL_DIR\" && pwd )`"
SERVER_ADDR=$SERVER_USER@$SERVER_HOST

ssh $SERVER_ADDR "rm -rf ~/ztnbd && mkdir ~/ztnbd 2>/dev/null"
scp "$ABS_DIR/$MAIN_SCRIPT.ipynb" "$SERVER_ADDR:~/ztnbd/$MAIN_SCRIPT.ipynb"
scp "$ABS_DIR/Pipfile" "$SERVER_ADDR:~/ztnbd/Pipfile"
scp "$ABS_DIR/Pipfile.lock" "$SERVER_ADDR:~/ztnbd/Pipfile.lock"
scp -r "$ABS_DIR/external" "$SERVER_ADDR:~/ztnbd/external"
scp -r "$ABS_DIR/post_extractor" "$SERVER_ADDR:~/ztnbd/post_extractor"

ssh $SERVER_ADDR << SSH_SESS
	cd ztnbd
	python3 -m pip install --user pyspark numpy

    jupyter nbconvert --to script $MAIN_SCRIPT.ipynb

	echo "===================== $MAIN_SCRIPT.py ====================="
	python3 $MAIN_SCRIPT.py $SERVER_USER $SPARK_STREAM
SSH_SESS