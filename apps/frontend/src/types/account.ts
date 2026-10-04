export interface AccountCreate {
  customer_id: string
  account_number: string
  account_type: string
  currency?: string | null
  balance?: number | null
  credit_limit?: number | null
  opened_date?: string | null
  closed_date?: string | null
  status?: string | null
}

export type AccountUpdate = Partial<AccountCreate>

export interface Account extends AccountCreate {
  account_id: string
  created_at: string
  updated_at: string
}
