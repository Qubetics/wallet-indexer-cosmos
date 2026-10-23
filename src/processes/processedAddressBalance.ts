import { config } from "../config";
import kafkaHelper from "../config/kafka";
import { walletHelper } from "../helpers";
import { WalletModel } from "../models";

class processedAddressBalance {
    public cosmos_pending_balance_update_topic = config.config.KAFKA_BALANCE.COSMOS_PENDING_BALANCE_UPDATE_TOPIC;
    public cosmosPendingBalanceUpdateGroup = config.config.KAFKA_BALANCE.COSMOS_PENDING_BALANCE_UPDATE_GROUP_ID;

    public consumeWalletBalanceUpdateQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.cosmosPendingBalanceUpdateGroup,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.cosmos_pending_balance_update_topic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ updateWalletBalance topic >>>>", topic);
                            console.log("🚀 ~ updateWalletBalance message.value >>>>", message.value.toString());
                            this.updateBalance(JSON.parse(message.value.toString()))

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

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count < 10) {

                let balance: any = await walletHelper.updateQueueAddressBalance(data.wallet_address, data.coin_data);
                if (balance !== null) {
                    await WalletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } })
                } else {
                    if (data.queue_count <= 5) {
                        await kafkaHelper.produceMessage(this.cosmos_pending_balance_update_topic, {
                            coin_data: data.coin_data,
                            wallet_address: data.wallet_address,
                            queue_count: data.queue_count + 1
                        });
                    }
                }

                console.log("balance:::", balance);
            }
            else {
                console.log(`updating balance attempts are more than 10 `);
            }

        } catch (err: any) {
            console.error("Error in updateBalance", err)
        }
    }



}


export default new processedAddressBalance();