import { useEffect, useState } from 'react';

export default function Audiences() {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    fetch('/api/proxy/api/v2/diagnostics/mailchimp-audiences', { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error('Audience information is unavailable. Please try again later.');
        return response.json();
      })
      .then(setData)
      .catch((err) => { if (err.name !== 'AbortError') setError(err.message); });
    return () => controller.abort();
  }, []);
  const count = (value) => typeof value === 'number' ? value.toLocaleString() : 'Unknown';
  return <main style={{ maxWidth: 1100, margin: '40px auto', padding: 24, fontFamily: 'system-ui', color: '#17212f' }}>
    <a href="/">← Craft Dominant</a>
    <h1>Festival audiences</h1>
    <p>Each Mailchimp audience keeps its own subscriptions. A person can belong to more than one city or festival.</p>
    {error && <p role="alert">{error}</p>}
    {!data && !error && <p role="status">Loading audiences…</p>}
    {data && <>
      <p><strong>{data.audience_count} audiences</strong> · Sending {data.external_send_enabled ? 'enabled' : 'disabled'}</p>
      <p>Festival routing needs review. The audience marked below is the single audience in the existing configuration; it does not cover all festivals.</p>
      <div style={{ overflowX: 'auto' }}><table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
        <thead><tr>{['Audience', 'Subscribed', 'Unsubscribed', 'Cleaned'].map((title) => <th key={title} style={{ padding: 12, borderBottom: '2px solid #cbd5e1' }}>{title}</th>)}</tr></thead>
        <tbody>{data.audiences.map((audience) => <tr key={audience.audience_id}>
          <td style={{ padding: 12, borderBottom: '1px solid #e2e8f0' }}>{audience.name || 'Unnamed audience'}{audience.legacy_configured_audience && <strong> · Currently configured</strong>}<details><summary>Audience ID</summary>{audience.audience_id}</details></td>
          {[audience.subscribed, audience.unsubscribed, audience.cleaned].map((value, index) => <td key={index} style={{ padding: 12, borderBottom: '1px solid #e2e8f0' }}>{count(value)}</td>)}
        </tr>)}</tbody>
      </table></div>
      <p>Cleaned addresses are already blocked by Mailchimp. These counts alone do not identify contacts to remove.</p>
    </>}
  </main>;
}
