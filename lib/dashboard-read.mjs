// Validate the response before it can replace the last successful dashboard.
export async function readDashboard(fetcher = fetch, signal) {
  const response = await fetcher('/api/proxy/api/dashboard', {cache: 'no-store', signal});
  if (!response.ok) throw new Error('Dashboard refresh is temporarily unavailable.');
  const value = await response.json();
  const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
  if (!object(value) || value.error || !Array.isArray(value.events)
      || !object(value.portfolio) || !object(value.customers)
      || !['total_tickets','total_revenue','total_spend','portfolio_cac'].every(k => Number.isFinite(value.portfolio[k]))
      || !Number.isFinite(value.customers.total)
      || !value.events.every(e => object(e) && e.event_id != null && typeof e.event_name === 'string')) {
    throw new Error('The dashboard returned an incomplete response.');
  }
  return value;
}
