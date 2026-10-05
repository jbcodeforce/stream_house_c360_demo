export interface TransactionCreate {
  account_id: string
  customer_id: string
  transaction_type: string
  amount: number
  currency?: string | null
  description?: string | null
  merchant_name?: string | null
  merchant_category?: string | null
  channel?: string | null
  status?: string | null
  reference_id?: string | null
  transacted_at?: string | null
  posted_at?: string | null
}

export interface Transaction extends TransactionCreate {
  transaction_id: string
  created_at: string
}
