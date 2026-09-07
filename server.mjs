import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3000);
const COMFYUI_URL = (process.env.COMFYUI_URL || '').replace(/\/$/, '');
const QUALITY = {
  aspectRatio: '9:16',
  generationWidth: 720,
  generationHeight: 1280,
  finalWidth: 2160,
  finalHeight: 3840,
  fps: 24,
  codec: 'h265',
  bitrate: '35M',
  pixelFormat: 'yuv420p10le',
  upscaleMode: 'ai-4k'
};

const jobs = new Map();

function sendJson(res, status, data) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-headers': 'content-type, authorization',
    'access-control-allow-methods': 'GET,POST,OPTIONS'
  });
  res.end(JSON.stringify(data));
}

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function uid() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`;
}

async function comfy(pathname, options = {}) {
  if (!COMFYUI_URL) throw new Error('COMFYUI_URL is not configured');
  const r = await fetch(`${COMFYUI_URL}${pathname}`, {
    ...options,
    headers: {'content-type':'application/json', ...(options.headers || {})}
  });
  if (!r.ok) throw new Error(`ComfyUI ${r.status}: ${await r.text()}`);
  const type = r.headers.get('content-type') || '';
  return type.includes('application/json') ? r.json() : r.arrayBuffer();
}

async function submitWorkflow(workflow, clientId) {
  return comfy('/prompt', {
    method: 'POST',
    body: JSON.stringify({prompt: workflow, client_id: clientId})
  });
}

async function getHistory(promptId) {
  return comfy(`/history/${encodeURIComponent(promptId)}`);
}

function outputFiles(historyEntry) {
  const files = [];
  for (const node of Object.values(historyEntry?.outputs || {})) {
    for (const key of ['videos','gifs','images']) {
      for (const f of node?.[key] || []) {
        files.push({
          filename: f.filename,
          subfolder: f.subfolder || '',
          type: f.type || 'output',
          url: COMFYUI_URL ? `${COMFYUI_URL}/view?filename=${encodeURIComponent(f.filename)}&subfolder=${encodeURIComponent(f.subfolder || '')}&type=${encodeURIComponent(f.type || 'output')}` : null
        });
      }
    }
  }
  return files;
}

async function pollJob(id) {
  const j = jobs.get(id);
  if (!j || !j.promptId) return j;
  try {
    const h = await getHistory(j.promptId);
    const entry = h?.[j.promptId];
    if (entry) {
      const files = outputFiles(entry);
      j.status = files.length ? 'succeeded' : 'processing';
      j.outputs = files;
      j.updatedAt = new Date().toISOString();
    }
  } catch (e) {
    j.lastPollError = e.message;
  }
  return j;
}

async function handleGenerate(req, res) {
  const payload = await readJson(req);
  if (!payload.workflow || typeof payload.workflow !== 'object') {
    return sendJson(res, 400, {
      error: 'workflow_required',
      message: 'Send a ComfyUI API-format workflow JSON. Use the included Wan2.2 4K profile as your generation target.',
      quality: QUALITY
    });
  }
  const id = uid();
  const clientId = `huangshu-${id}`;
  const response = await submitWorkflow(payload.workflow, clientId);
  const job = {
    id,
    backend: 'comfyui',
    promptId: response.prompt_id,
    status: 'queued',
    quality: QUALITY,
    title: payload.title || 'AI video job',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    outputs: []
  };
  jobs.set(id, job);
  sendJson(res, 202, job);
}

async function handleMcp(req, res) {
  const msg = await readJson(req);
  if (msg.method === 'initialize') {
    return sendJson(res, 200, {jsonrpc:'2.0', id:msg.id, result:{protocolVersion:'2025-06-18', capabilities:{tools:{}}, serverInfo:{name:'huangshu-4k-video',version:'1.0.0'}}});
  }
  if (msg.method === 'tools/list') {
    return sendJson(res, 200, {jsonrpc:'2.0', id:msg.id, result:{tools:[
      {name:'video_quality_profile',description:'Return the locked maximum-quality production profile.',inputSchema:{type:'object',properties:{}}},
      {name:'submit_comfyui_video_workflow',description:'Submit a ComfyUI API-format Wan2.2 video workflow. The production target is 9:16 and 4K final delivery.',inputSchema:{type:'object',properties:{title:{type:'string'},workflow:{type:'object'}},required:['workflow']}},
      {name:'get_video_job',description:'Get video generation status and output URLs.',inputSchema:{type:'object',properties:{id:{type:'string'}},required:['id']}}
    ]}});
  }
  if (msg.method === 'tools/call') {
    const name = msg.params?.name;
    const args = msg.params?.arguments || {};
    try {
      let result;
      if (name === 'video_quality_profile') result = QUALITY;
      else if (name === 'submit_comfyui_video_workflow') {
        const id = uid();
        const response = await submitWorkflow(args.workflow, `huangshu-${id}`);
        result = {id, backend:'comfyui', promptId:response.prompt_id, status:'queued', quality:QUALITY, title:args.title || 'AI video job', createdAt:new Date().toISOString(), outputs:[]};
        jobs.set(id, result);
      } else if (name === 'get_video_job') {
        result = await pollJob(args.id);
        if (!result) throw new Error('Job not found');
      } else throw new Error('Unknown tool');
      return sendJson(res, 200, {jsonrpc:'2.0', id:msg.id, result:{content:[{type:'text',text:JSON.stringify(result)}], structuredContent:result}});
    } catch (e) {
      return sendJson(res, 200, {jsonrpc:'2.0', id:msg.id, error:{code:-32000,message:e.message}});
    }
  }
  sendJson(res, 200, {jsonrpc:'2.0', id:msg.id, result:{}});
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') return sendJson(res, 204, {});
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (url.pathname === '/health') return sendJson(res, 200, {ok:true, comfyuiConfigured:Boolean(COMFYUI_URL), quality:QUALITY});
    if (url.pathname === '/api/quality') return sendJson(res, 200, QUALITY);
    if (url.pathname === '/api/generate' && req.method === 'POST') return handleGenerate(req,res);
    if (url.pathname.startsWith('/api/jobs/') && req.method === 'GET') {
      const id = decodeURIComponent(url.pathname.slice('/api/jobs/'.length));
      const job = await pollJob(id);
      return job ? sendJson(res,200,job) : sendJson(res,404,{error:'not_found'});
    }
    if (url.pathname === '/mcp' && req.method === 'POST') return handleMcp(req,res);
    if (url.pathname === '/manifest.json') return sendJson(res,200,{name:'Huangshu 4K AI Video Gateway',version:'1.0.0',mcp:'/mcp',quality:QUALITY});

    const requested = url.pathname === '/' ? 'index.html' : url.pathname.replace(/^\//,'');
    const file = path.join(__dirname, requested);
    if (!file.startsWith(__dirname) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404); return res.end('Not found');
    }
    const ext = path.extname(file);
    const types = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8'};
    res.writeHead(200, {'content-type':types[ext] || 'application/octet-stream'});
    fs.createReadStream(file).pipe(res);
  } catch (e) {
    sendJson(res,500,{error:e.message});
  }
});

server.listen(PORT, '0.0.0.0', () => console.log(`Huangshu 4K video gateway listening on ${PORT}`));
