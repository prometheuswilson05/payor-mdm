const API = 'http://localhost:3001/api';

export async function querySnowflake(sql: string): Promise<any[]> {
  const res = await fetch(`${API}/sql`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql }),
  });
  if (!res.ok) throw new Error('SQL query failed');
  const data = await res.json();
  return data.rows;
}

export async function writeSnowflake(statements: string[]): Promise<void> {
  const res = await fetch(`${API}/write`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ statements }),
  });
  if (!res.ok) throw new Error('Write failed');
}

export async function triggerDbtRun(): Promise<string> {
  const res = await fetch(`${API}/dbt-run`, { method: 'POST' });
  if (!res.ok) throw new Error('dbt run failed');
  const data = await res.json();
  return data.output;
}

export async function checkConnection(): Promise<boolean> {
  try {
    await querySnowflake('SELECT 1');
    return true;
  } catch {
    return false;
  }
}
