export interface DvpnSessionsInterface {
    id?: number;
    wallet_id: number;
    user_id: number;
    status?: string;
    block?: number;
    tx_hash?: string;
    node_address?: string;
    event: string;
    node_id?: number;
    status_at?: string;
    start_at?: string;  
    extra_data?: any;
    created_at?: string;
    updated_at?: string;
}