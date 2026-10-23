import axios from "axios";
import { config } from "../config/config";
import { CatchErrorMsgsModel, CoinInterface } from "../models";
import { WalletModel, TicsNativeAddressModel } from "../models/model/index";
import cosmosProcessHelper from "../processes/process.helper";

class GlobalHelper {
    public async checkCoinInWallet(wallet_address: string, coin: CoinInterface, balance: any) {
        try {
            let wallet_data: any = await TicsNativeAddressModel.findOne({
                where: {
                    address: wallet_address,
                },
                attributes: ["wallet_id"],
                include: [{
                    model: WalletModel,
                    as: 'wallet',
                    attributes: ["balance", "user_id", "wallet_name"],
                }],
                raw: true
            })
            if (!wallet_data) {
                wallet_data = await cosmosProcessHelper.checkOurWalletAddress(wallet_address)
                if (!wallet_data) {
                    return;
                }
                let data_obj: any = {
                    user_id: wallet_data.user_id,
                    address: wallet_address,
                    wallet_id: wallet_data.wallet_id,
                }
                await TicsNativeAddressModel.create(data_obj)
            }
        } catch (err: any) {
            console.error("Error in checkCoinInWallet>>>", err)
            return err;
        }

    }
    public async send_mail(email: string, text: string, subject: string) {
        try {
            const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
            const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
            const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
            await axios({
                method: 'post',
                url: `${MAILGUN_BASE_URL}/messages`,
                auth: {
                    username: 'api',
                    password: MAILGUN_API_KEY
                },
                params: {
                    from: config.MAILGUN.FROM_GMAIL,
                    to: email,
                    subject: subject,
                    html: text
                }
            });
        } catch (err: any) {
            console.error("send_mail", err)
            await this.save_error_logs("MAILGUN_ERROR", err.message)
        }
    }
    public async save_error_logs(fn_name: string, error_message: any) {
        try {
            let value_exist: any = await cosmosProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.COSMOS_ERROR_LOGS)
            if (value_exist) {
                if (Number(value_exist) == 0) {
                    await CatchErrorMsgsModel.create({
                        fx_name: fn_name,
                        error_msg: error_message || {}
                    })
                    await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.COSMOS_ERROR_LOGS, '1')
                } else {
                    console.log("value other than 0 exist")
                }
            } else {
                await cosmosProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.COSMOS_ERROR_LOGS, '0')
                console.log("value does not exist")
            }
        } catch (err: any) {
            console.error("Error in save_error_logs", err)
        }
    }
}
export let globalHelper = new GlobalHelper();
