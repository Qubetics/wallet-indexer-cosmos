import { config } from "../config/config";
import { BlockChainStatusEnum, CoinFamilyEnum, TrnxTypeEnum } from "../enum";
import { CoinInterface, CoinsModel, TrnxHistoryModel, WalletModel } from "../models";
import kafkaHelper from "../config/kafka";
import { Op } from "sequelize";
import { IChainAbsTxData } from "../interfaces/kafka.interface";
import axios from 'axios';

class ChainAbsTransactions {

    public chainAbsTxTopic =  config.KAFKA_BLOCKS.CHAIN_ABS_TX_TOPIC
    public chainAbsTxGroupId = config.KAFKA_BLOCKS.CHAIN_ABS_TX_GROUP_ID
    public chainAbsApiUrl = 'https://mainnet.ticssolver.com'

    public tica_pending_balance_update_topic = config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_TOPIC
    public btc_pending_balance_update_topic = config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_TOPIC



    public normalisedIntentHash(intentHash: string) : string {
        return intentHash.toLowerCase().replace(/^0x/, '').replace(/^0X/, '').replace(/^0/, '');
    }

    public async getChainAbsTransactions() : Promise<void> {
       try {
         //get transactions from txHistory table  where status is 2 //pending and tx_type is chain_abstraction and createded within 10 minutes and blockchain_status is confirmed
         const TIME_WITHIN = 30 // minutes

         const transactions = await TrnxHistoryModel.findAll({
            where: {
                status: 'pending', // pending from stage-chain-abstraction
                blockchain_status: BlockChainStatusEnum.CONFIRMED, 
                type: TrnxTypeEnum.CHAIN_ABSTRACTION,
                created_at: { [Op.gte]: new Date(Date.now() - TIME_WITHIN * 60 * 1000) }
            },
            attributes: ['id', 'tx_id', 'type', 'status', 'blockchain_status', 'created_at'],
            raw: true,
         });

         if(transactions.length == 0) {
            console.log('No chain abstraction transactions created within 30 minutes ☀️☀️');
            return;
         }

         console.log('Found Chain abstraction transactions created within 30 minutes ☀️☀️', transactions.length);

        
         await Promise.all(transactions.map(async (transaction) => {
            const data : IChainAbsTxData = {
                id: transaction.id,
                tx_id: transaction.tx_id,
                type: transaction.type as TrnxTypeEnum,
                status: transaction.status ,
                blockchain_status: transaction.blockchain_status as BlockChainStatusEnum,
                created_at: transaction.created_at as Date | null,
            };

            await kafkaHelper.produceMessage(this.chainAbsTxTopic, data);
            console.log('Successfully Produced chain abstraction transaction to kafka 🚅🚅🚅');
         }));


        } catch (error) {
            console.error("Error in getChainAbsTransactions:", error);

        }
    }

    public consumeChainAbsTxQueue : any = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.chainAbsTxGroupId,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.chainAbsTxTopic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ chain abstraction transaction topic >>>>", topic);
                            console.log("🚀 ~ chain abstraction transaction message.value >>>>", message.value.toString());
                            this.processChainAbsTx(JSON.parse(message.value.toString()));

                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer consumeWalletBalanceUpdateQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

     
    public processChainAbsTx = async (data: IChainAbsTxData) => {
      try {
        console.log("🔄 Processing chain abstraction transaction:", data);

        const { id, tx_id } = data;

        /** Normalize intent hash */
        const INTENT_HASH = this.normalisedIntentHash(tx_id as string);
        console.log(`🔑 Intent hash: ${INTENT_HASH} | TX ID: ${tx_id}`);

        /** API headers */
        const headers = {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        };

        /** Fetch intent details from Chain Abstraction API */
        const response = await axios.get(
          `${this.chainAbsApiUrl}/intent_details/${INTENT_HASH}`,
          { headers }
        );

        if (response.status !== 200) {
          console.error(
            "❌ Error fetching intent status:",
            response.status,
            response.statusText
          );
          return;
        }

        const responseData: any = response?.data;

        if (!responseData) {
          console.error("❌ No intent data found for hash:", INTENT_HASH);
          return;
        }

        console.log("✅ Intent data received:", responseData);

        const {
          network_to_target_tx_id,
          error_message,
          intent_status,
          target_address,
          target_token,
        } = responseData;

        console.log("Target TX:", network_to_target_tx_id);
        console.log("Intent Status:", intent_status);
        console.log("Error:", error_message);

        /**
         * ------------------------------------------------
         * FAILED STATE
         * ------------------------------------------------
         */
        if (intent_status == "Rejected") {
          console.log(`❌ Updating transaction to FAILED for TX ID: ${tx_id}`);

          await TrnxHistoryModel.update(
            { status: "failed" },
            { where: { id } }
          );

          return true;
        }

        /**
         * ------------------------------------------------
         * COMPLETED STATE
         * ------------------------------------------------
         */
        if (intent_status == "Solved") {
          console.log(`✅ Transaction SOLVED for TX ID: ${tx_id}`);

          await TrnxHistoryModel.update(
            { status: "completed" },
            { where: { id } }
          );

          console.log("🎉 Transaction marked as completed:", id);

          /** Validate target wallet and token */
          if (!target_address || !target_token) {
            console.log("⚠️ Target address or token missing");
            return true;
          }

          /** Determine coin configuration */
          const isTics = target_token == "tics";

          const COIN_ID = isTics ? 161 : 3;
          const COIN_FAMILY = isTics ? CoinFamilyEnum.TICS : CoinFamilyEnum.BTC;

          const kafkaTopic = isTics
            ? this.tica_pending_balance_update_topic
            : this.btc_pending_balance_update_topic;

          /**
           * ------------------------------------------------
           * FETCH WALLET
           * ------------------------------------------------
           */
          const walletData = await WalletModel.findOne({
            where: {
              wallet_address: { [Op.like]: `%${target_address}%` },
              coin_family: COIN_FAMILY,
              coin_id: COIN_ID,
            },
            attributes: ["wallet_address"],
            raw: true,
          });

          if (!walletData) {
            console.log("⚠️ Wallet not found for address:", target_address);
            return true;
          }

          /**
           * ------------------------------------------------
           * FETCH COIN DATA
           * ------------------------------------------------
           */
          const coinData: CoinInterface = (await CoinsModel.findOne({
            where: { coin_id: COIN_ID },
            attributes: [
              "coin_id",
              "coin_name",
              "coin_symbol",
              "coin_image",
              "coin_family",
              "cmc_id",
              "is_on_cmc",
              "coin_status",
              "is_token",
              "token_type",
              "decimals",
              "token_abi",
              "token_address",
            ],
            raw: true,
          })) as CoinInterface;

          /**
           * ------------------------------------------------
           * PRODUCE KAFKA MESSAGE FOR BALANCE UPDATE
           * ------------------------------------------------
           */
          await kafkaHelper.produceMessage(kafkaTopic, {
            coin_data: coinData,
            wallet_address: walletData.wallet_address,
            queue_count: 0,
          });

          console.log(
            `💰 Balance update message produced for wallet: ${walletData.wallet_address}`
          );

          return true;
        }

        /**
         * ------------------------------------------------
         * PENDING STATE
         * ------------------------------------------------
         */
        if (intent_status == "Pending" || intent_status == "Batched") {
          console.log(`⏳ Transaction still pending for TX ID: ${tx_id}`);

          await TrnxHistoryModel.update(
            { status: "pending" },
            { where: { id } }
          );

          return true;
        }

        console.log("⚠️ Unknown intent status:", intent_status);
        return;

      } catch (error) {
        console.error("🔥 Error in processChainAbsTx:", error);
      }
    };



}

export const chainAbsTransactions = new ChainAbsTransactions();