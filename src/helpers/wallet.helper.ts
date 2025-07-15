import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface, CoinsModel, TicsNativeAddressModel } from "../models";
import WalletModel from "../models/model/model.wallets";
import { Blockchain_Helper } from "./blockchain.helper";
import { Utility_Helper } from "./utility.helper";

class WalletHelper {
    public cosmos_pending_balance_update_topic = config.config.KAFKA_BALANCE.COSMOS_PENDING_BALANCE_UPDATE_TOPIC;

    Fetch_Balance = async (
        address: string,
        coin: CoinInterface,
    ) => {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.TICS:
                        balance = await Blockchain_Helper.Coin_Fetch_Balance(address);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }
            return balance !== null ? Number(await Utility_Helper.exponentialToDecimal(balance)) : null;
        } catch (error: any) {
            console.error(`WalletHelper Fetch_Balance error >>>`, error);
            return null;
        }
    }


    Update_Balance = async (
        wallet_address: string,
        coin: CoinInterface
    ) => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.Fetch_Balance(
                wallet_address,
                coin
            );
            
            if (balance !== null) {
                const wallet_data : any = await TicsNativeAddressModel.findOne({
                    where: {
                        address: wallet_address,
                    },
                    include: [{
                        model: WalletModel,
                        as: 'wallet',
                        attributes: ["wallet_id"],
                    }]
                })
                if (wallet_data) {
                    await WalletModel.update({
                        balance: Number(balance),
                        status: 1

                    }, {
                        where: {
                            wallet_id: wallet_data?.wallet?.wallet_id,   
                        }
                    });
                }
            }
            else {

                let data = {
                    coin_data: coin,
                    wallet_address: wallet_address,
                    queue_count: 0,
                }
                await kafkaHelper.produceMessage(this.cosmos_pending_balance_update_topic, data)
            }
            return balance;
        } catch (error: any) {
            console.error(`WalletHelper Update_Balance error >>>`, error);
            return null;
        }
    }


    public async Update_Balance_Queue(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.Fetch_Balance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                return balance;
            }
            return null;
        } catch (err: any) {
            console.error(`WalletHelper Update_Balance error >>>`, err);
            return null;
        }
    }
}
export let Wallet_Helper = new WalletHelper();
