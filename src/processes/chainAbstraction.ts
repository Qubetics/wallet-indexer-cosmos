import { config } from "../config/config";
import { BlockChainStatusEnum, KEYS, TrnxTypeEnum } from "../enum";
import { TicsNativeOldBlockModel, TrnxHistoryModel } from "../models";
import { globalHelper } from "../helpers/global";
import kafkaHelper from "../config/kafka";
import fetch from 'node-fetch';
import { blockchainHelper } from "../helpers";
import { Op } from "sequelize";
import { IChainAbsTxData } from "../interfaces/kafka.interface";
import axios from 'axios';

class ChainAbsTransactions {

    public chainAbsTxTopic =  config.KAFKA_BLOCKS.CHAIN_ABS_TX_TOPIC
    public chainAbsTxGroupId = config.KAFKA_BLOCKS.CHAIN_ABS_TX_GROUP_ID
    public chainAbsApiUrl = 'https://prod-solver-chain-abstraction-api.qubetics.work'

    public async getChainAbsTransactions() : Promise<void> {
       try {
         //get transactions from txHistory table  where status is 2 //pending and tx_type is chain_abstraction and createded within 10 minutes and blockchain_status is confirmed
         const TIME_WITHIN = 10 // minutes

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
            console.log('No chain abstraction transactions created within 10 minutes ☀️☀️');
            return;
         }

         console.log('Found Chain abstraction transactions created within 10 minutes ☀️☀️', transactions.length);

        
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
           console.log("Processing chain abstraction transaction 🗳️🗳️🗳️🗳️🗳️", data);

           const { id, tx_id } = data;

           const INTENT_HASH : any = tx_id;

           const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
           }

           const response = await axios.get(`${this.chainAbsApiUrl}/api/routes/intent-status?intent-hash=${INTENT_HASH}`, { headers });

           if(response.status !== 200) {
            console.error("Error in fetching intent status:", response.status, response.statusText);
            return;
           }

           const responseData : any = response?.data?.data;

           console.log("Intent Data fetched successfully 🗳️🗳️🗳️🗳️🗳️", responseData);

           if(!responseData) {
            console.error("No intent data found for intent hash:", INTENT_HASH);
            return;
           }

           if(responseData.status == 1) {
            //then proceed with update status of  tx history table to completed 
            await TrnxHistoryModel.update({
                status: 'completed' , // completed
            }, {
                where: { id: id }
            });
            console.log("Transaction status updated to completed 🎉🎉🎉", id);
            return true;
           }

           if(responseData.status == 2) {
            //then proceed with update status of  tx history table to pending 
            await TrnxHistoryModel.update({
                status: 'pending' , // pending
            }, {
                where: { id: id }
            });
            console.log("Transaction status updated to pending 🖐️🖐️🖐️", id);
            return true;
           }


           if(responseData.status == 0) {
            //then proceed with update status of  tx history table to failed 
            await TrnxHistoryModel.update({
                status: 'failed' , // failed
            }, {
                where: { id: id }
            });
            console.log("Transaction status updated to failed 📢📢📢", id);
            return true;
           }
        
           return;

        }
         catch (error) {
            console.error("Error in processChainAbsTx:", error);
        }
    }



}

export const chainAbsTransactions = new ChainAbsTransactions();