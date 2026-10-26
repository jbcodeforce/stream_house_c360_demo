export const nf = new Intl.NumberFormat('en-US')

const cf = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

export function formatCurrency(n: number): string {
  return cf.format(n)
}
