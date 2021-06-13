readonly NODE_ID_0=cs-xxxx
readonly NODE_ID_1=cs-yyyy
readonly NODE_ID_2=cs-zzzz
readonly RABBITMQ_HOST=10.0.0.4
readonly NODE_0=sn-xxxx
readonly NODE_1=sn-yyyy
readonly NODE_2=sn-zzzz

docker run \
  --name "$NODE_ID_0" \
  -d  \
  --network=mynet \
  -e RABBITMQ_HOST="$RABBITMQ_HOST" \
  -e NODE_ID="$NODE_ID_0" \
  -e NODE="$NODE_0" \
  -e PRIORITY=1\
  -e BULLY_NODES.0.node-id="$NODE_ID_1" \
  -e BULLY_NODES.0.priority=2 \
  -e BULLY_NODES.1.node-id="$NODE_ID_2" \
  -e BULLY_NODES.1.priority=0 \
  -l bully \
  nachocode/bully-node

# CS - YYYY
#docker run \
#  --name "$NODE_ID_1" \
#  -d  \
#  --network=mynet \
#  -e RABBITMQ_HOST="$RABBITMQ_HOST" \
#  -e NODE_ID="$NODE_ID_1" \
#  -e NODE="$NODE_1" \
#  -e PRIORITY=2\
#  -e BULLY_NODES.0.node-id="$NODE_ID_0" \
#  -e BULLY_NODES.0.priority=1 \
#  -e BULLY_NODES.1.node-id="$NODE_ID_2" \
#  -e BULLY_NODES.1.priority=0 \
#  -e IS_LEADER="false"\
#  -l bully \
#  nachocode/bully-node
##  CS - ZZZZ
#docker run \
#  --name "$NODE_ID_2" \
#  -d  \
#  --network=mynet \
#  -e RABBITMQ_HOST="$RABBITMQ_HOST" \
#  -e NODE_ID="$NODE_ID_2" \
#  -e NODE="$NODE_2" \
#  -e PRIORITY=0\
#  -e BULLY_NODES.0.node-id="$NODE_ID_0" \
#  -e BULLY_NODES.0.priority=1 \
#  -e BULLY_NODES.1.node-id="$NODE_ID_1" \
#  -e BULLY_NODES.1.priority=2 \
#  -e IS_LEADER="false"\
#  -l bully \
#  nachocode/bully-node
