import { utilityHelper } from "./utility";
import { ethers } from 'ethers';
import fetch from 'node-fetch';

class BlockchainHelper {
    constructor() {
        this.initialize();
    }

    public initialize() { }


    convertToWei = (value: number | string | bigint): string => {
        try {
            return ethers.parseUnits(value.toString(), 18).toString();
        } catch (error) {
            console.error(`convertToWei error >>`, error);
            return "0";
        }
    }
    public coinFetchBalance = async (
        address: string
    ) => {
        try {
            console.log("Fetching balance for address:", address);
            const url = `https://swagger-internal.qubetics.com/cosmos/bank/v1beta1/balances/${address}`;
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`Failed to fetch balance from Cosmos API: ${response.statusText}`);
            }
            const data = await response.json();
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
}
export let blockchainHelper = new BlockchainHelper();