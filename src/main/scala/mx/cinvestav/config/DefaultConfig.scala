package mx.cinvestav.config

case class BullyNode(nodeId:String,priority:Int)

case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
//                          exchangeName:String,
                          priority: Int,
                          node:String,
                          maxRetries:Int,
                          maxRetriesOkMessages:Int,
                          isLeader:Boolean,
                          nodes:List[String],
                          bullyNodes:List[BullyNode],
                          leaderNode:String,
                          shadowLeader:String,
                          healthCheckTime:Int,
//
                          coordinatorWindow:Int,
                          rabbitmq: RabbitMQConfig
                        )
