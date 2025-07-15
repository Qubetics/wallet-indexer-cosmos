import zookeeperConfig from "../helpers/zookeeper.helper";
const name = 'qubetics';
export const config = {
  DB_USER: zookeeperConfig.config.DB_USER,
  DB_HOST_READ: zookeeperConfig.config.DB_HOST_READ,
  DB_HOST_WRITE: zookeeperConfig.config.DB_HOST_WRITE,
  DB_NAME: zookeeperConfig.config.DB_NAME,
  DB_PASSWORD: zookeeperConfig.config.DB_PASSWORD,
  DB_PORT: zookeeperConfig.config.DB_PORT,
  COSMOS_WALLET_ADDRESS: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.TICS_NATIVE_WALLET_ADDRESS}`,
  REDIS_CONN: zookeeperConfig.config.REDIS_CONN,
  KAFKA_BROKER: zookeeperConfig.config.KAFKA_BROKER,
  KAFKA_CLIENT_ID: zookeeperConfig.config.KAFKA_CLIENT_ID,
  MIN_BLOCK_CONFORMATIONS: zookeeperConfig.config.MIN_TICS_NATIVE_BLOCK_CONFORMATIONS,
  TOKEN_TYPE_COSMOS: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.TOKEN_TYPE_TICS}`,
  STATIC_COIN_FAMILY: zookeeperConfig.config.STATIC_COIN_FAMILY,
  REDISKEYS: {
    COSMOS_BLOCKS: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.TICS_NATIVE_BLOCKS}`,
    OLD_BLOCK_COSMOS: zookeeperConfig.config.OLD_BLOCK_TICS_NATIVE,
    NEW_BLOCK_COSMOS: zookeeperConfig.config.NEW_BLOCK_TICS_NATIVE,
    ALL_PROCESSES: zookeeperConfig.config.ALL_PROCESSES,
    COSMOS_ERROR_LOGS: zookeeperConfig.config.TICS_NATIVE_ERROR_LOGS,
    NODE_ALERT_COSMOS: zookeeperConfig.config.NODE_ALERT_TICS_NATIVE,
    SPECIFIC_BLOCK_COSMOS: zookeeperConfig.config.SPECIFIC_BLOCK_TICS_NATIVE
  },
  /**
   * KAFKA KEYS START
   */
  KAFKA_BLOCKS: {
    COSMOS_BLOCK_TX_TOPIC: zookeeperConfig.config.TICS_NATIVE_BLOCK_TX_TOPIC,
    COSMOS_BEHIND_BLOCK_TX_TOPIC: zookeeperConfig.config.TICS_NATIVE_BEHIND_BLOCK_TX_TOPIC,
    COSMOS_BLOCK_TX_GROUP_ID: zookeeperConfig.config.TICS_NATIVE_BLOCK_TX_GROUP_ID,
    COSMOS_BEHIND_BLOCK_TX_GROUP_ID: zookeeperConfig.config.TICS_NATIVE_BEHIND_BLOCK_TX_GROUP_ID
  },
  KAFKA_TRANSACTIONS: {
    DEPOSIT_WITHDRAW_GROUP_ID: zookeeperConfig.config.TICS_NATIVE_DEPOSIT_WITHDRAW_GROUP_ID,
    DEPOSIT_WITHDRAW_TOPIC: zookeeperConfig.config.TICS_NATIVE_DEPOSIT_WITHDRAW_TOPIC,
    COSMOS_PENDING_WITHDRAWAL_TX_TOPIC: zookeeperConfig.config.TICS_NATIVE_PENDING_WITHDRAWAL_TX_TOPIC,
    COSMOS_PENDING_WITHDRAWAL_TX_GROUP_ID: zookeeperConfig.config.TICS_NATIVE_PENDING_WITHDRAWAL_TX_GROUP_ID
  },
  KAFKA_BALANCE: {
    COSMOS_PENDING_BALANCE_UPDATE_TOPIC: zookeeperConfig.config.TICS_NATIVE_PENDING_BALANCE_UPDATE_TOPIC,
    COSMOS_PENDING_BALANCE_UPDATE_GROUP_ID: zookeeperConfig.config.TICS_NATIVE_PENDING_BALANCE_UPDATE_GROUP_ID
  },
  /**
   * KAFKA KEYS END
   */
  MAILGUN: {
    MAILGUN_API_KEY: zookeeperConfig.config.MAILGUN_API_KEY,
    MAILGUN_DOMAIN: zookeeperConfig.config.MAILGUN_DOMAIN,
    MAILGUN_BASE_URL: zookeeperConfig.config.MAILGUN_BASE_URL,
    FROM_GMAIL: zookeeperConfig.config.FROM_GMAIL
  },
  NODE: { 
  // COSMOS_RPC_URL: zookeeperConfig.config.TICS_NATIVE_RPC_URL 
  // COSMOS_SWAGGER_URL: zookeeperConfig.config.TICS_NATIVE_SWAGGER_URL
  },
  NODE_ISSUE_ALERT_EMAIL: zookeeperConfig.config.NODE_ISSUE_ALERT_EMAIL,
  READ_BEHINED_BLOCK_COSMOS: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.READ_BEHINED_BLOCK_COSMOS}`,
  BLOCKS: zookeeperConfig.config.BLOCKS,
}