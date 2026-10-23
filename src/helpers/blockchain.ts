import { utilityHelper } from "./utility";
import fetch from 'node-fetch';
import { config } from "../config";

class BlockchainHelper {
    constructor() {
        this.initialize();
    }

    public initialize() { }


    public coinFetchBalance = async (
        address: string
    ) => {
        try {
            console.log("Fetching balance for address:", address);
            const response = await this.fetchFromCosmosApi(config.config.NODE.COSMOS_SWAGGER_URL, {
                method: "GET",
                headers: {
                    "Content-Type": "application/json",
                },
                params: {
                    path: `/cosmos/bank/v1beta1/balances/${address}`,
                },
            });
            if (!response?.ok) {
                throw new Error(`Failed to fetch balance from Cosmos API: ${response?.statusText}`);
            }
            const data = await response?.json();
            // Find the balance for denom 'tics'
            let balanceObj = null;
            if (Array.isArray(data.balances)) {
                balanceObj = data.balances.find((b: any) => b.denom === "tics");
            }
            if (balanceObj && balanceObj.amount) {
                // Cosmos tics has 18 decimals
                const tokenBalance = await utilityHelper.bigNumberSafeMath(
                    balanceObj.amount,
                    "/",
                    Math.pow(10, 18)
                );
                const coinBalance: string = await utilityHelper.exponentialToDecimal(tokenBalance)
                console.log("Balance:", coinBalance);
                return Number(coinBalance);
            }
            return 0;
        } catch (error: any) {
            console.error(`Cosmos_Helper coinFetchBalance error >>`, error);
            return null;
        }
    }

    public fetchFromCosmosApi = async (url: string, options: any) => {
        try {
            const response = await fetch(url + options.params.path, options);
            return response;
        } catch (error: any) {
            console.error(`Cosmos_Helper fetchFromCosmosApi error >>`, error);
            return null;
        }
    }
}
export let blockchainHelper = new BlockchainHelper();