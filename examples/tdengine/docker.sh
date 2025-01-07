
# remove any existing tdengine docker container
docker rm -f tdengine

# start a new tdengine docker container
echo "starting a new tdengine docker container..."
docker run -d --name tdengine \
    -e TAOS_USER=root \
    -e TAOS_PASSWORD=taosdata \
    -p 6030:6030 \
    -p 6041:6041 \
    -p 6043-6060:6043-6060 \
    -p 6043-6060:6043-6060/udp \
    tdengine/tdengine


# check the tdengine database status
echo -n "waiting for tdengine database to initiate..."
while true; do
    status=$(docker exec tdengine taos --check)
    if [[ $status == *"2: service ok"* ]]; then
        echo -e "\ntdengine database is ready!"
        break
    fi
    sleep 1
    echo -n "."
done

# populate the database with some data
echo "populating the database with test data..."
docker exec -it tdengine taosBenchmark -y

# done
echo "done!"
