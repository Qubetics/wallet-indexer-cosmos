import {
    BooleanEnum,
    CoinFamilyEnum,
    TokenStandard,
} from "../enum";

import {
    CoinInterface,
    CoinsModel,
    DeviceTokenModel,
    SettingsModel,
    RewardHistoryModel,
    WalletModel,
    ReferralModel,
    CatchErrorMsgsModel,
    CoinPriceInFiatModel,

} from "../models/index";
import { Utility_Helper } from "../helpers";
import { Op } from "sequelize";

class DbHelper {
    GetDeviceTokens = async (user_id: number, address: string, coin_family: number) => {
        try {
            return await DeviceTokenModel.findAll({
                attributes: ["device_token"],
                where: {
                    address: {
                        [Op.like]: address,
                    },
                    coin_family: coin_family,
                    push: BooleanEnum.true
                },
                order: [
                    ["updated_at", "DESC"],
                    ['id', 'DESC']
                ],
                raw: true
            });
        } catch (error: any) {
            console.error(`getDeviceToken error >>>`, error);
            return false;
        }
    };
}
export let Db_Helper = new DbHelper();

