const express = require('express');
const cors = require('cors');
const snowflake = require('snowflake-sdk');
const { exec } = require('child_process');
const path = require('path');

require('dotenv').config({ path: path.join(require('os').homedir(), 'workflows', '.env') });

const app = express();
app.use(cors({ origin: 'http://localhost:5173' }));
app.use(express.json());

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USER,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
});

let connected = false;

connection.connect((err) => {
  if (err) {
    console.error('Snowflake connection failed:', err.message);
  } else {
    connected = true;
    console.log('Connected to Snowflake');
  }
});

function executeQuery(sql) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows);
      },
    });
  });
}

app.post('/api/sql', async (req, res) => {
  try {
    const { query } = req.body;
    if (!query) return res.status(400).json({ error: 'Missing query' });
    const rows = await executeQuery(query);
    res.json({ rows });
  } catch (err) {
    console.error('SQL error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/write', async (req, res) => {
  try {
    const { statements } = req.body;
    if (!statements || !Array.isArray(statements)) {
      return res.status(400).json({ error: 'Missing statements array' });
    }
    for (const stmt of statements) {
      await executeQuery(stmt);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error('Write error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/dbt-run', (req, res) => {
  const repoRoot = path.resolve(__dirname, '..', '..');
  const cmd = `cd ${repoRoot}/transform/payor_mdm && dbt run --select golden_payors+`;
  exec(cmd, { maxBuffer: 1024 * 1024 * 10 }, (err, stdout, stderr) => {
    if (err) {
      console.error('dbt run error:', stderr);
      return res.status(500).json({ error: stderr || err.message });
    }
    res.json({ output: stdout });
  });
});

const PORT = 3001;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
