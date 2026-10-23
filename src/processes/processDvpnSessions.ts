import { config } from "../config";
import kafkaHelper from "../config/kafka";
import { DvpnSessionsModel } from "../models";
import { DvpnSessionsInterface } from "../interfaces/global_helper";
import { DvpnSessionsInterface as DvpnSessionsModelInterface } from "../models/interfaces/interface.dvpn_sessions";

class processedDvpnSessions {
    public dvpn_session_topic = config.config.KAFKA_TRANSACTIONS.DVPN_SESSION_TOPIC;
    public dvpn_session_group_id = config.config.KAFKA_TRANSACTIONS.DVPN_SESSION_GROUP_ID;

    public consumeDvpnSessionsQueue: any = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {

                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.dvpn_session_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.dvpn_session_topic,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ dvpn_session topic >>>>", topic);
                            console.log("🚀 ~ dvpn_session message.value >>>>", message.value.toString());
                            this.processDvpnSession(JSON.parse(message.value.toString()))

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


    public filtereData = (obj: any) => {
        const filteredData = Object.fromEntries(
            Object.entries(obj).map(([key, value]) => {
                if (typeof value === "string") {
                    return [key, value.replace(/^"|"$/g, "")];
                }
                return [key, value];
            })
        );
        return filteredData;
    };


    public processDvpnSession = async (data: DvpnSessionsInterface) => {
        try {

            // check if wallet_id and user_id are present  
            if (!data.wallet_id || !data.user_id) { return };

            // check if session already exists
            const exist: DvpnSessionsModelInterface | null = await DvpnSessionsModel.findOne({ where: { wallet_id: data.wallet_id, user_id: data.user_id, tx_hash: data.tx_hash } });

            if (exist) { return };

            // create new session
            const sessionData: DvpnSessionsModelInterface = {
                wallet_id: data.wallet_id,
                user_id: data.user_id,
                status: data.status || "",
                block: data.block || 0,
                tx_hash: data.tx_hash || "",
                node_address: data.node_address || "",
                node_id: Number(String(data.node_id).replace(/"/g, "")) || 0,
                event: data.event || "",
                status_at: data.status_at || "",
                start_at: data.start_at || "",
                extra_data: data.extra_data || {},
            };


            const filteredSessionData: any = this.filtereData(sessionData);
            // save session
            await DvpnSessionsModel.create(filteredSessionData);

        } catch (err: any) {
            console.error("Error in processDvpnSession", err)
        }
    }


}


export const processedDvpnSession = new processedDvpnSessions();
