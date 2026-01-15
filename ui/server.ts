import express from 'express';
import axios from 'axios';
import compression from 'compression';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
const port = parseInt(process.env.PORT || '3000', 10);
const ORCHESTRATOR_API_URL = process.env.ORCHESTRATOR_API_URL || 'http://localhost:8080';
const ORCHESTRATOR_AUTH_TOKEN = process.env.ORCHESTRATOR_AUTH_TOKEN || '';

app.use(compression());
app.use(express.json());

// Proxy /api/* to orchestrator backend
app.all(/^\/api\/(.*)$/, async (req, res) => {
  try {
    const apiPath = '/api/' + (req.params[0] || '');
    const url = `${ORCHESTRATOR_API_URL}${apiPath}`;

    console.log(`[Proxy] ${req.method} ${url}`);

    const response = await axios({
      method: req.method as any,
      url,
      params: req.query,
      data: req.body,
      headers: {
        'Content-Type': req.headers['content-type'] || 'application/json',
        'Accept': req.headers['accept'] || 'application/json',
        ...(ORCHESTRATOR_AUTH_TOKEN && { 'Authorization': `Bearer ${ORCHESTRATOR_AUTH_TOKEN}` }),
      },
      timeout: 600000, // 10 min for long operations like import
      validateStatus: () => true,
    });

    if (response.headers['content-type']) {
      res.setHeader('Content-Type', response.headers['content-type']);
    }

    res.status(response.status).send(response.data);
  } catch (error: any) {
    console.error('[Proxy] Error:', error.message);
    res.status(502).json({
      error: 'Backend service unavailable',
      details: error.message,
    });
  }
});

// Serve static files
// When running compiled (node dist/server.js), __dirname ends with 'dist', so static files are in __dirname
// When running with tsx (tsx server.ts), __dirname is the project root, so static files are in __dirname/dist
const isInDistDir = __dirname.endsWith('dist');
const staticDir = isInDistDir ? __dirname : path.join(__dirname, 'dist');
app.use(express.static(staticDir));

// SPA fallback - serve index.html for all non-API routes
app.get('*', (_req, res) => {
  res.sendFile(path.join(staticDir, 'index.html'));
});

app.listen(port, () => {
  console.log(`Manticore UI Server`);
  console.log(`  Port: ${port}`);
  console.log(`  Backend: ${ORCHESTRATOR_API_URL}`);
  console.log(`  Auth Token: ${ORCHESTRATOR_AUTH_TOKEN ? 'configured' : 'NOT SET'}`);
});
