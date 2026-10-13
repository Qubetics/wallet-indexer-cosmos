
export interface pending_trnx {
    tx_id: string;
    coin_id: number,
    is_token: boolean,
    token_type: string | null,
    token_address: string
}

export interface TxReceipt {
    txHash: string;
    blockNumber: number;
    from: string;
    to: string;
    value: string;
    trnxFee: number | string;
}

export interface DvpnSessionsInterface {
    wallet_id?: number;
    user_id?: number;
    wallet_address?: string;
    node_id?: string;
    tx_hash?: string;
    node_address?: string;
    status?: string;
    status_at?: string;
    block?: number;
    event?: string;
    start_at?: string;
    extra_data?: any;
}