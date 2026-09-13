// dsh-remote — remote-work assistant for DeepSeek Harness.
//
// Host half. Turns "give me a remote host + login" into a usable REMOTE WORKSPACE:
//   • one persistent SSH/SFTP pool per configured remote (password OR private key),
//   • a "current remote workspace" — a remote directory the agent treats as the
//     active project root (user@host:/path) — injected into every system prompt,
//   • model tools `rw_info` / `rw_connect` / `rw_pick_workspace` /
//     `rw_list_dir` / `rw_read_file` / `rw_exec`,
//   • JSON endpoints the client settings page uses to connect → browse → select the
//     remote workspace over the harness `webServer`.
//
// The engine (path guard + shell quoting + ssh pool + exec) reuses the foundation
// proven by dsh-remote-debug, extended with password auth and a mutable workspace:
// `ctx.fs` / the local workspace registry stay untouched — this is a REMOTE workspace
// presented as such to the model and UI, not a replacement of the local one.
//
// Plugin Config MUST be a schemastery schema (zod rejects the undefined row config).
import z from '@deepseek-ai/schemastery'
import { defineTool } from '@deepseek-ai/dsh-tools'
import ssh2 from 'ssh2'
import { readFileSync, mkdirSync, writeFileSync, existsSync, readdirSync, statSync } from 'node:fs'
import { homedir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { WebSocketServer, WebSocket } from 'ws'

/** This plugin's own root dir (…/dsh-remote), used to resolve vendored assets. */
const PLUGIN_ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)))

const { Client } = ssh2

export const name = 'dsh-remote'

// tools + systemPrompt + webServer are required. webServer is INJECTED (not just
// lazily read) so this plugin activates only after the web server is up — otherwise
// apply() runs ahead of webServer and the /dsh-remote/* JSON routes never register
// (silently: the boot shows no error, but status/ls/workspace return the SPA fallback).
export const inject = ['tools', 'systemPrompt', 'webServer']

export const Config = z.object({
  /** Remote SSH host (empty → the plugin starts disconnected). */
  host: z.string().default(''),
  /** Remote SSH port (22 unless the machine uses a custom port). */
  port: z.number().step(1).min(1).max(65535).default(22),
  /** SSH login user. */
  username: z.string().default(''),
  /** Password login (only when the remote has no key. Override the fallback below). */
  password: z.string().default(''),
  /** Absolute private-key path; empty → ~/.ssh/id_rsa. */
  privateKeyPath: z.string().default(''),
  /** Key passphrase when the key is encrypted. */
  passphrase: z.string().default(''),
  /** Initial remote workspace path (absolute dir the agent should treat as root). */
  workspace: z.string().default(''),
  /** Per-command timeout. */
  commandTimeoutMs: z.number().step(1).min(1000).default(20000),
  /** SSH connection establishment timeout. */
  connectTimeoutMs: z.number().step(1).min(1000).default(15000),
  /** Hard ceiling on collected remote output per call. */
  maxOutputChars: z.number().step(1).min(1024).default(200000),
})

// ── shell / path helpers (proven in the read tooling) ───────────────────────

/** Single-quote one shell argument verbatim. */
function shq(s) {
  return "'" + String(s).replace(/'/g, "'\\''") + "'"
}

/** Collapse `.`/`..`/duplicate slashes into a clean absolute remote path. */
function normalizeRemotePath(p) {
  const parts = []
  for (const seg of String(p).split('/')) {
    if (seg === '' || seg === '.') continue
    if (seg === '..') {
      parts.pop()
      continue
    }
    parts.push(seg)
  }
  return '/' + parts.join('/')
}

/** Parent dir of a remote absolute path (string-level). */
function remoteDirname(p) {
  const norm = normalizeRemotePath(p)
  if (norm === '/') return '/'
  const idx = norm.lastIndexOf('/')
  return idx <= 0 ? '/' : norm.slice(0, idx)
}

function truncate(s, max) {
  if (s.length <= max) return s
  return s.slice(0, max) + `\n…[truncated: ${s.length - max} more chars]`
}

// ── local mirror of an OS remote workspace ────────────────────────────────
// Makes a chosen remote workspace a REAL local directory (fs.realpath passes),
// so the DSH host `workspaceRegistry.create(dir)` accepts it as a native
// workspace. dsh-remote syncs that local mirror <-> the remote over SFTP.

const remotePathBase = (p) => {
  const norm = normalizeRemotePath(p).replace(/\/+$/, '')
  const base = norm.split('/').pop()
  return base || 'workspace'
}

/** Stable local root for one remote host's mirrors */
function mirrorRootFor(host, user, port) {
  const tag = [host, user, port].filter(Boolean).join('-').replace(/[^a-zA-Z0-9._-]/g, '_')
  return path.join(homedir(), '.dsh', 'remote-workspaces', tag)
}

/** Local mirror dir for a specific remote path (idempotent → returns same dir).
 * Named after the remote directory's basename so the harness workspace label
 * reads cleanly (e.g. .../project) instead of a concatenated hash string. */
function mirrorDirFor(remotePath, host, user, port) {
  const base = remotePathBase(remotePath)
  return path.join(mirrorRootFor(host, user, port), base)
}

/** Create the local mirror dir + a meta file describing its remote origin. */
function ensureMirror(remotePath, host, user, port) {
  const dir = mirrorDirFor(remotePath, host, user, port)
  mkdirSync(dir, { recursive: true })
  writeFileSync(
    path.join(dir, '.dsh-remote-meta.json'),
    JSON.stringify({ host, port, username: user, remotePath, createdAt: new Date().toISOString() }, null, 2),
  )
  return dir
}

/** Recursively sync remote → local mirror. Bounded by depth/files. */
async function syncTree(sftp, remoteDir, localDir, maxDepth, maxFiles) {
  const entries = await sftp.readdir(remoteDir).then(
    (list) => list,
    () => [],
  )
  let files = 0
  const touched = []
  for (const e of entries) {
    const name = String(e.filename)
    if (name === '.' || name === '..') continue
    const rp = remoteDir === '/' ? '/' + name : remoteDir + '/' + name
    const lp = path.join(localDir, name)
    const isDir = e.attrs && e.attrs.isDirectory && e.attrs.isDirectory()
    if (isDir) {
      if (maxDepth <= 0) continue
      mkdirSync(lp, { recursive: true })
      const sub = await syncTree(sftp, rp, lp, maxDepth - 1, maxFiles)
      files += sub.files
      touched.push(...sub.touched)
      continue
    }
    if (files >= maxFiles) break
    try {
      const buf = await sftp.readFile(rp)
      writeFileSync(lp, buf)
      touched.push(rp)
      files++
    } catch {
      /* skip unreadable */
    }
  }
  return { files, touched }
}

/** Recursively upload a local mirror tree → remote SFTP dir. Bounded by files. */
async function pushTree(sftp, localDir, remoteDir, remoteBaseDir, maxFiles) {
  const entries = readdirSync(localDir, { withFileTypes: true }).filter((e) => e.name !== '.dsh-remote-meta.json')
  let files = 0
  let dirsCreated = 0
  const pushed = []
  for (const e of entries) {
    if (e.isDirectory()) {
      const rp = remoteDir === '/' ? '/' + e.name : remoteDir + '/' + e.name
      try {
        await sftp.mkdir(rp)
        dirsCreated++
      } catch { /* already exists */ }
      const sub = await pushTree(sftp, path.join(localDir, e.name), rp, remoteBaseDir, maxFiles)
      files += sub.files
      pushed.push(...sub.pushed)
      continue
    }
    if (files >= maxFiles) break
    const lp = path.join(localDir, e.name)
    const rp = remoteDir === '/' ? '/' + e.name : remoteDir + '/' + e.name
    try {
      const buf = readFileSync(lp)
      await sftp.writeFile(rp, buf)
      pushed.push(rp)
      files++
    } catch {
      /* skip unreadable / unwritable */
    }
  }
  return { files, pushed }
}

// ── persistent multi-machine registry ─────────────────────────────────────
const MACHINES_FILE = 'machines.json'
const machinesFile = () => path.join(homedir(), '.dsh', 'remote-workspaces', MACHINES_FILE)
function loadMachines() {
  try {
    const j = JSON.parse(readFileSync(machinesFile(), 'utf8'))
    if (Array.isArray(j.list)) return { list: j.list, currentId: j.currentId || (j.list[0] && j.list[0].id) || null }
  } catch {}
  return { list: [], currentId: null }
}
function saveMachines(list, currentId) {
  try { mkdirSync(path.dirname(machinesFile()), { recursive: true }) } catch {}
  writeFileSync(machinesFile(), JSON.stringify({ list, currentId }, null, 2))
}
// ── guard mode (per-user, persists independently of machines) ──────────────
// 'high' = agent operates freely except delete/modify (those require approval).
// 'low'  = agent must list its plan first; create/execute require approval.
const GUARD_FILE = 'guard.json'
const guardFile = () => path.join(homedir(), '.dsh', 'remote-workspaces', GUARD_FILE)
function loadGuard() {
  try {
    const j = JSON.parse(readFileSync(guardFile(), 'utf8'))
    return j.mode === 'low' ? 'low' : 'high'
  } catch { return 'high' }
}
function saveGuard(mode) {
  try { mkdirSync(path.dirname(guardFile()), { recursive: true }) } catch {}
  writeFileSync(guardFile(), JSON.stringify({ mode }, null, 2))
}
/** Keywords that mark a shell command as delete/modify (approval in high mode too). */
function isDangerousCommand(cmd) {
  const s = String(cmd || '')
  // deletion
  if (/(^|[;&|]\s*)rm\s+(-\S+\s+)*/.test(s)) return true
  if (/\brmdir\b|\bunlink\b/.test(s)) return true
  // modification / overwrite
  if (/\bmv\s+(-\S+\s+)*/.test(s)) return true
  if (/\bsed\s+(-\S+\s+)*-i\b/.test(s)) return true
  if (/\bchmod\b|\bchown\b|\btruncate\b/.test(s)) return true
  if (/\bdd\b/.test(s)) return true
  // overwrite redirect (> file), not append
  if (/[^>]>\s*\S/.test(s) && !/>>\s*/.test(s)) return true
  return false
}
function sanitizeMachine(m) {
  if (!m) return m
  const { password, ...rest } = m
  return { ...rest, passwordSet: !!(m.password && m.password.length) }
}
/** Apply a machine's fields onto the live config object (pool + tools read it). */
function applyMachine(config, m) {
  if (!m) return
  config.host = m.host
  config.port = Number(m.port) || 22
  config.username = m.username || ''
  config.password = m.password || ''
  config.privateKeyPath = m.privateKeyPath || ''
  config.passphrase = m.passphrase || ''
  config.workspace = m.workspace || (config.workspace || '')
}
function machineId() { return 'm-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 6) }

// ── SSH pool (key OR password) ──────────────────────────────────────────────

class SshPool {
  constructor(config) {
    this.config = config
    this.client = null
    this.connecting = null
    this._sftp = null
    this._sftpConnecting = null
  }

  resolveKeyPath() {
    const p = this.config.privateKeyPath
    if (!p) return path.join(homedir(), '.ssh', 'id_rsa')
    if (p.startsWith('~/') || p === '~') return path.join(homedir(), p.slice(1))
    return p
  }

  /** Configure (and reconnect with) a new target. Returns this pool for chaining. */
  setTarget({ host, port, username, password, privateKeyPath, passphrase, workspace }) {
    if (host !== undefined) this.config.host = String(host)
    if (port !== undefined && Number(port)) this.config.port = Number(port)
    if (username !== undefined) this.config.username = String(username)
    if (password !== undefined && password !== null) this.config.password = String(password)
    if (privateKeyPath !== undefined) this.config.privateKeyPath = String(privateKeyPath)
    if (passphrase !== undefined) this.config.passphrase = String(passphrase)
    if (workspace !== undefined) this.config.workspace = String(workspace)
    this.close()
    return this
  }

  connect() {
    if (this.client) return Promise.resolve(this.client)
    if (this.connecting) return this.connecting
    this.connecting = this._doConnect().finally(() => {
      this.connecting = null
    })
    return this.connecting
  }

  _doConnect() {
    return new Promise((resolve, reject) => {
      const client = new Client()
      let settled = false
      const fail = (err) => {
        if (settled) return
        settled = true
        if (this.client === client) this.client = null
        reject(err)
      }
      client.on('ready', () => {
        if (settled) return
        settled = true
        this.client = client
        resolve(client)
      })
      client.on('error', fail)
      client.on('close', () => {
        if (this.client === client) this.client = null
        fail(new Error('ssh connection closed'))
      })

      const opts = {
        host: this.config.host,
        port: this.config.port,
        username: this.config.username,
        readyTimeout: this.config.connectTimeoutMs,
        keepaliveInterval: 15000,
        keepaliveCountMax: 3,
      }
      if (this.config.password) {
        opts.password = this.config.password
      } else {
        let key
        try {
          key = readFileSync(this.resolveKeyPath())
        } catch (err) {
          return fail(
            new Error(`cannot read private key "${this.resolveKeyPath()}": ${err && err.message}`),
          )
        }
        opts.privateKey = key
        opts.passphrase = this.config.passphrase || undefined
      }
      client.connect(opts)
    })
  }

  /** Run one remote command; resolves { code, signal, stdout, stderr }. */
  exec(command, timeoutMs) {
    return this.connect().then(
      (client) =>
        new Promise((resolve, reject) => {
          client.exec(command, (err, stream) => {
            if (err) return reject(new Error('ssh exec failed: ' + ((err && err.message) || err)))
            let stdout = ''
            let stderr = ''
            let settled = false
            let exitCode = null
            let exitSignal = null
            const hardCap = Math.max(this.config.maxOutputChars * 4, 1024 * 1024)
            const settle = () => {
              if (settled) return
              settled = true
              clearTimeout(timer)
              resolve({
                code: exitCode,
                signal: exitSignal,
                stdout: truncate(stdout, this.config.maxOutputChars),
                stderr: truncate(stderr, this.config.maxOutputChars),
              })
            }
            const timer = setTimeout(() => {
              if (settled) return
              exitCode = -1
              exitSignal = 'TIMEOUT'
              try {
                stream.close()
              } catch {}
              settle()
            }, timeoutMs || this.config.commandTimeoutMs)
            stream.on('close', (code, signal) => {
              if (settled) return
              exitCode = code
              exitSignal = signal
              settle()
            })
            stream.on('data', (d) => {
              if (stdout.length < hardCap) stdout += d
            })
            stream.stderr.on('data', (d) => {
              if (stderr.length < hardCap) stderr += d
            })
            stream.on('error', (e) => {
              if (settled) return
              settled = true
              clearTimeout(timer)
              reject(new Error('ssh stream error: ' + ((e && e.message) || e)))
            })
          })
        }),
    )
  }

  /** Resolve a promisified SFTP client for binary file transfers (cached). */
  sftp() {
    if (this._sftp) return Promise.resolve(this._sftp)
    if (this._sftpConnecting) return this._sftpConnecting
    this._sftpConnecting = this.connect().then(
      (client) =>
        new Promise((resolve, reject) => {
          client.sftp((err, sftp) => {
            if (err) return reject(new Error('ssh sftp failed: ' + ((err && err.message) || err)))
            const wrap = {
              raw: sftp,
              readdir: (dir) => new Promise((r2, j2) => sftp.readdir(dir, (e, list) => (e ? j2(e) : r2(list)))),
              stat: (p) => new Promise((r2, j2) => sftp.stat(p, (e, st) => (e ? j2(e) : r2(st)))),
              mkdir: (dir) => new Promise((r2, j2) => sftp.mkdir(dir, (e) => (e ? j2(e) : r2()) )),
              readFile: (p) => new Promise((r2, j2) => sftp.readFile(p, (e, buf) => (e ? j2(e) : r2(buf)))),
              writeFile: (p, data) => new Promise((r2, j2) => sftp.writeFile(p, data, (e) => (e ? j2(e) : r2()))),
              unlink: (p) => new Promise((r2, j2) => sftp.unlink(p, (e) => (e ? j2(e) : r2()))),
              rmdir: (p) => new Promise((r2, j2) => sftp.rmdir(p, (e) => (e ? j2(e) : r2()))),
              rename: (a, b) => new Promise((r2, j2) => sftp.rename(a, b, (e) => (e ? j2(e) : r2()))),
              realpath: (p) => new Promise((r2, j2) => sftp.realpath(p, (e, rp) => (e ? j2(e) : r2(rp)))),
            }
            this._sftp = wrap
            resolve(wrap)
          })
        }),
    )
    this._sftpConnecting.finally(() => { this._sftpConnecting = null })
    return this._sftpConnecting
  }

  /**
   * Open an interactive PTY shell on a fresh SSH connection. Returns the ssh2
   * Client (so the caller owns its lifecycle) plus the shell stream, with
   * stdin/out bridged via the provided callbacks. Used by the WebSocket
   * terminal so the agent's exec/sftp pool stays independent.
   */
  openShell({ cols, rows, onData, onClose }) {
    const client = new Client()
    const opts = {
      host: this.config.host,
      port: this.config.port,
      username: this.config.username,
      readyTimeout: this.config.connectTimeoutMs,
      keepaliveInterval: 15000,
      keepaliveCountMax: 3,
    }
    if (this.config.password) {
      opts.password = this.config.password
    } else {
      try {
        opts.privateKey = readFileSync(this.resolveKeyPath())
        opts.passphrase = this.config.passphrase || undefined
      } catch (err) {
        return Promise.reject(new Error(`cannot read private key "${this.resolveKeyPath()}": ${err && err.message}`))
      }
    }
    return new Promise((resolve, reject) => {
      let settled = false
      const fail = (err) => { if (!settled) { settled = true; try { client.end() } catch {} reject(err) } }
      client.on('ready', () => {
        if (settled) return
        client.shell({ term: 'xterm-256color', cols: cols || 80, rows: rows || 24 }, (err, stream) => {
          if (err) return fail(new Error('ssh shell failed: ' + ((err && err.message) || err)))
          if (settled) return
          settled = true
          stream.on('data', (d) => { try { onData && onData(d) } catch {} })
          stream.on('close', () => { try { onClose && onClose() } catch {} try { client.end() } catch {} })
          stream.on('error', () => { try { onClose && onClose() } catch {} try { client.end() } catch {} })
          client.on('close', () => { try { onClose && onClose() } catch {} })
          resolve({ client, stream })
        })
      })
      client.on('error', fail)
      client.connect(opts)
    })
  }

  close() {
    const client = this.client
    this.client = null
    this._sftp = null
    this._sftpConnecting = null
    if (client) {
      try {
        client.end()
      } catch {}
    }
  }
}

// ── helper: run + text ─────────────────────────────────────────────────────

export async function apply(ctx, config) {
  const pool = new SshPool(config)
  ctx.effect(() => () => pool.close(), 'dsh-remote.close')

  // ── guard mode (approval gating for remote mutations) ─────────────────────
  let guardMode = loadGuard() // 'high' | 'low'
  const approval = (ctx && ctx.get && ctx.get('approval')) || (ctx && ctx.approval) || null
  const setGuardMode = (mode) => {
    const m = mode === 'low' ? 'low' : 'high'
    guardMode = m
    saveGuard(m)
  }
  /** Ask for approval (when a channel exists) for one mutating remote action. */
  const requireApproval = async (exec, toolName, reason) => {
    if (approval && exec && exec.agent) {
      const outcome = await approval.request({
        agent: exec.agent,
        toolName,
        callId: exec.callId,
        reason,
        signal: exec.signal,
      })
      if (outcome === 'allowed-once') return
      throw new Error(`remote action denied by user (${outcome}): ${reason}`)
    }
    // No approval channel (headless / no service): fail closed for mutations.
    throw new Error(`remote mutation requires approval but no approval channel is available: ${reason}`)
  }
  /** Decide whether a mutating tool call needs approval under the current mode. */
  const needsApproval = (toolName, args) => {
    // Reads never need approval.
    if (toolName === 'rw_info' || toolName === 'rw_connect' || toolName === 'rw_pick_workspace' ||
        toolName === 'rw_list_dir' || toolName === 'rw_read_file' || toolName === 'rw_disconnect') return null
    if (toolName === 'rw_sync') return null // remote → local download, no server mutation
    if (toolName === 'rw_exec') {
      const cmd = String((args && args.command) || '')
      if (guardMode === 'low') return `执行命令：${cmd}`
      if (isDangerousCommand(cmd)) return `删除/修改类命令：${cmd}`
      return null
    }
    if (toolName === 'rw_write_file') {
      if (guardMode === 'low') return `写文件：${(args && args.path) || ''}`
      return null
    }
    if (toolName === 'rw_push') {
      if (guardMode === 'low') return `推送本地改动到远程工作区`
      return null
    }
    return null
  }

  // ── machine registry (multi-host) ─────────────────────────────────────────
  const store = loadMachines()
  const machines = store.list
  const machineIndex = (id) => machines.findIndex((m) => m.id === id)
  const currentMachine = () => {
    if (store.currentId) {
      const i = machineIndex(store.currentId)
      if (i >= 0) return machines[i]
    }
    // fall back to config-derived default (host set via cordis config)
    if (config.host) return { id: machineId(), name: config.host, host: config.host, port: config.port, username: config.username, password: config.password, privateKeyPath: config.privateKeyPath, passphrase: config.passphrase }
    return null
  }
  const setCurrent = (id) => {
    const i = machineIndex(id)
    if (i < 0) return false
    store.currentId = id
    saveMachines(machines, id)
    applyMachine(config, machines[i])
    pool.setTarget({ host: config.host, port: config.port, username: config.username, password: config.password, privateKeyPath: config.privateKeyPath, passphrase: config.passphrase })
    return true
  }
  // If no stored current, adopt a CLI-provided default as the active machine.
  {
    const cur = currentMachine()
    if (cur && cur.host) applyMachine(config, cur)
  }

  const run = async (cmd, opts = {}) => {
    const res = await pool.exec(cmd, opts.timeoutMs)
    const parts = []
    if (res.stdout) parts.push(res.stdout.replace(/\s+$/, ''))
    if (res.stderr) parts.push('-- stderr --\n' + res.stderr.replace(/\s+$/, ''))
    if (!parts.length) parts.push('(no output)')
    let text = parts.join('\n')
    if (res.signal === 'TIMEOUT') text += `\n[command timed out after ${opts.timeoutMs ?? config.commandTimeoutMs}ms]`
    else if (res.code !== 0) text += `\n[exit code: ${res.code}]`
    return text
  }

  /** Structured-listing of a remote dir: name + usable type. `dir` is decided by
   * `[ -d ]`, which FOLLOWS symlinks — so a symlink to a directory is enterable
   * (the picker can drill into it), while a file symlink stays a file. */
  const listDirStructured = async (p, timeoutMs) => {
    const target = p || '/'
    const cmd =
      `cd ${shq(target)} 2>/dev/null && for f in .[!.]* *; do ` +
      `[ -e "$f" ] || [ -L "$f" ] || continue; ` +
      `if [ -d "$f" ]; then printf 'd\\t%s\\n' "$f"; else printf 'f\\t%s\\n' "$f"; fi; done`
    const res = await pool.exec(cmd, timeoutMs || config.commandTimeoutMs)
    const items = []
    if (res.code !== 0 && res.stderr) {
      throw new Error('ls failed: ' + (res.stderr || '').trim())
    }
    for (const line of String(res.stdout || '').split('\n')) {
      const idx = line.indexOf('\t')
      if (idx < 0) continue
      const type = line.slice(0, idx)
      const name = line.slice(idx + 1)
      if (name === '.' || name === '..' || !name) continue
      items.push({ type: type === 'd' ? 'dir' : 'file', name })
    }
    return { path: target, items }
  }

  // ── remote workspace state ────────────────────────────────────────────────

  const wsPath = () => (config.workspace || '').trim()
  const status = () => ({
    host: config.host,
    port: config.port,
    username: config.username,
    connected: !!pool.client,
    workspace: wsPath(),
    localMirror: wsPath() ? mirrorDirFor(wsPath(), config.host, config.username, config.port) : '',
    currentId: store.currentId || null,
    machines: machines.map(sanitizeMachine),
  })

  // ── tools ─────────────────────────────────────────────────────────────────

  const renderErr = (err) => ({
    kind: 'error',
    text: String((err && err.message) || err),
  })

  const tools = [
    defineTool({
      name: 'rw_info',
      description:
        'Show the remote environment: host/user/port, connection health, and the current remote workspace path. Call this first to orient, or when a remote_* call fails to check connectivity.',
      parameters: {},
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, v) => [{ type: 'text', text: v.text }],
      },
      async execute() {
        const s = status()
        const lines = [
          `Remote host: ${s.username || '<user>'}@${s.host || '<host>'}:${s.port}`,
          `Current remote workspace: ${s.workspace || '(none — call rw_pick_workspace to set one)'}`,
          `Connected: ${s.connected ? 'yes' : 'no'}`,
          '',
        ]
        if (s.host && s.workspace) {
          try {
            const res = await pool.exec('echo ok; hostname; pwd', Math.min(config.commandTimeoutMs, 8000))
            if (res.signal === 'TIMEOUT') lines.push('Ping: timeout')
            else if (res.code === 0) lines.push('Ping: OK — ' + res.stdout.replace(/\s+/g, ' ').trim())
            else lines.push('Ping: FAILED — ' + (res.stderr || res.stdout || `exit ${res.code}`).trim())
          } catch (err) {
            lines.push('Ping: FAILED — ' + ((err && err.message) || err))
          }
        } else {
          lines.push('No host + workspace configured — call rw_connect with a host to get started.')
        }
        return { text: lines.join('\n') }
      },
    }),

    defineTool({
      name: 'rw_connect',
      description:
        'Connect SSH to a remote host for remote workspace work. Provide host (required), user, optional password or privateKeyPath/port. Once connected, call rw_pick_workspace to pick the workspace directory this session should work in.',
      parameters: {
        host: { type: 'string', required: true, description: 'Remote host IP or hostname' },
        username: { type: 'string', description: 'SSH user (default from config or root)' },
        port: { type: 'integer', description: 'SSH port (default 22)' },
        password: { type: 'string', description: 'SSH password (prefer SSH key when possible)' },
        privateKeyPath: { type: 'string', description: 'Absolute private-key path' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args) {
        const host = String(args.host || '').trim()
        if (!host) throw new Error('rw_connect: host is required')
        pool.setTarget({
          host,
          username: args.username || config.username || 'root',
          port: args.port || undefined,
          password: args.password !== undefined ? args.password : undefined,
          privateKeyPath: args.privateKeyPath || undefined,
        })
        try {
          const res = await pool.exec('echo ok; hostname', 8000)
          if (res.code !== 0 && !res.stdout) return { text: 'connect failed: ' + (res.stderr || 'exit ' + res.code) }
          return { text: `Connected to ${host} as ${config.username}.\nhostname: ${res.stdout.replace(/\s+/g, ' ').trim()}\n\npick a workspace with rw_pick_workspace (path=<abs>).` }
        } catch (err) {
          throw err
        }
      },
    }),

    defineTool({
      name: 'rw_pick_workspace',
      description:
        'Set the remote workspace directory this session should treat as its working root on the connected remote. Verifies it exists (a directory). Use rw_list_dir to browse first if unsure.',
      parameters: {
        path: { type: 'string', required: true, description: 'Absolute remote directory path, e.g. /home/dev/code/project' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args) {
        const p = normalizeRemotePath(String(args.path || ''))
        if (!p || p === '/') throw new Error('rw_pick_workspace: path must be an absolute directory')
        const res = await pool.exec(`if [ -d ${shq(p)} ]; then echo DIR; else echo NOTDIR; fi`)
        const ok = res.stdout.trim() === 'DIR'
        if (!ok) return { text: `not a directory (or missing) on ${p}` }
        config.workspace = p
        const local = ensureMirror(p, config.host, config.username, config.port)
        return {
          text: `Remote workspace set to ${p} on ${config.username}@${config.host}.\nLocal mirror (native workspace path): ${local}\n\nRun rw_sync to download its files into the local mirror.`,
        }
      },
    }),

    defineTool({
      name: 'rw_sync',
      description:
        'Download the current remote workspace into its local mirror directory over SFTP (bounded). Makes the remote files visible/editable locally so the DSH native workspace / fs tools can operate on them.',
      parameters: {
        depth: { type: 'integer', description: 'Max directory depth to mirror (default 5)' },
        maxFiles: { type: 'integer', description: 'Max files to download (default 500)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args) {
        const p = wsPath()
        if (!p) throw new Error('rw_sync: no remote workspace set — call rw_pick_workspace first')
        const local = mirrorDirFor(p, config.host, config.username, config.port)
        mkdirSync(local, { recursive: true })
        const depth = Math.min(Math.max(Number(args.depth) || 5, 1), 8)
        const maxFiles = Math.min(Math.max(Number(args.maxFiles) || 500, 1), 2000)
        let sftp
        try {
          sftp = await pool.sftp()
        } catch (err) {
          return { text: 'sftp unavailable: ' + ((err && err.message) || err) }
        }
        const { files, touched } = await syncTree(sftp, p, local, depth, maxFiles)
        return { text: `Downloaded ${files} file(s) from ${p} → ${local}${files >= maxFiles ? ' (hit download cap)' : ''}.` }
      },
    }),

    defineTool({
      name: 'rw_push',
      description:
        'Upload the local mirror of the current remote workspace back to the remote host over SFTP (bounded). Use after editing files in the local mirror so the remote reflects your changes (bidirectional sync).',
      parameters: {
        maxFiles: { type: 'integer', description: 'Max files to upload (default 500)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args, exec) {
        const p = wsPath()
        if (!p) throw new Error('rw_push: no remote workspace set — call rw_pick_workspace first')
        const reason = needsApproval('rw_push', args)
        if (reason) await requireApproval(exec, 'rw_push', reason)
        const local = mirrorDirFor(p, config.host, config.username, config.port)
        if (!existsSync(local)) throw new Error(`rw_push: local mirror does not exist — run rw_sync first (${local})`)
        const maxFiles = Math.min(Math.max(Number(args.maxFiles) || 500, 1), 2000)
        let sftp
        try {
          sftp = await pool.sftp()
        } catch (err) {
          return { text: 'sftp unavailable: ' + ((err && err.message) || err) }
        }
        const { files } = await pushTree(sftp, local, p, p, maxFiles)
        return { text: `Uploaded ${files} file(s) from ${local} → ${p}.` }
      },
    }),

    defineTool({
      name: 'rw_list_dir',
      description:
        'List a remote directory (or a single file) via SSH. Path is absolute; if omitted, lists the current remote workspace.',
      parameters: {
        path: { type: 'string', description: 'Absolute remote path (default: current remote workspace)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args) {
        const p = args.path ? normalizeRemotePath(String(args.path)) : wsPath()
        if (!p) throw new Error('rw_list_dir: no path and no remote workspace set')
        return { text: await run(`ls -la --color=never ${shq(p)}`) }
      },
    }),

    defineTool({
      name: 'rw_read_file',
      description:
        'Read a text file on the remote host with line numbers. Supports paging with startLine/endLine. Path is absolute.', 
      parameters: {
        path: { type: 'string', required: true, description: 'Absolute remote file path' },
        startLine: { type: 'integer', description: '1-based first line (default 1)' },
        endLine: { type: 'integer', description: '1-based last line (inclusive)' },
        maxLines: { type: 'integer', description: 'Max lines (default 2000)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args) {
        const p = normalizeRemotePath(String(args.path || ''))
        if (!p) throw new Error('rw_read_file: path is required')
        const maxLines = Math.min(Math.max(Number(args.maxLines) || 2000, 1), 10000)
        let from = Math.max(Number(args.startLine) || 1, 1)
        let to = Number(args.endLine) || 0
        if (!to || to - from + 1 > maxLines) to = from + maxLines - 1
        const raw = await run(`sed -n '${from},${to}p' -- ${shq(p)}`, { timeoutMs: config.commandTimeoutMs })
        const numbered = raw.split('\n').map((l, i) => `${String(from + i).padStart(6)}\t${l}`).join('\n').replace(/\s+$/, '')
        let text = numbered === '' ? '(empty or out of range)' : numbered
        if (!args.endLine) text += '\n(shown up to ' + maxLines + ' lines; use startLine/endLine to page)'
        return { text }
      },
    }),

    defineTool({
      name: 'rw_exec',
      description:
        'Run a shell command on the remote host. Use for anything that is not reading a file (build, test, grep, etc). Output is capped.',
      parameters: {
        command: { type: 'string', required: true, description: 'Shell command (run on the remote host)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute(args, exec) {
        const cmd = String(args.command || '')
        if (!cmd) throw new Error('rw_exec: command is required')
        const reason = needsApproval('rw_exec', args)
        if (reason) await requireApproval(exec, 'rw_exec', reason)
        return { text: await run(cmd, { timeoutMs: config.commandTimeoutMs }) }
      },
    }),

    defineTool({
      name: 'rw_write_file',
      description:
        'Write text to a file on the remote host (creating parent directories if needed). Path is absolute. Use this to create or overwrite a remote file directly, instead of round-tripping through a local mirror.',
      parameters: {
        path: { type: 'string', required: true, description: 'Absolute remote file path' },
        content: { type: 'string', required: true, description: 'File content to write (overwrites existing file)' },
        mkdir: { type: 'boolean', description: 'Create missing parent directories (default true)' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { ok: { type: 'boolean', required: true }, bytes: { type: 'integer' }, text: { type: 'string' } } },
        render: (_a, a) => [{ type: 'text', text: a.text || (a.ok ? 'written' : 'failed') }],
      },
      async execute(args, exec) {
        const p = normalizeRemotePath(String(args.path || ''))
        if (!p || p === '/') throw new Error('rw_write_file: a file path is required')
        const reason = needsApproval('rw_write_file', args)
        if (reason) await requireApproval(exec, 'rw_write_file', reason)
        const content = String(args.content == null ? '' : args.content)
        let sftp
        try {
          sftp = await pool.sftp()
        } catch (err) {
          throw new Error('rw_write_file: sftp unavailable: ' + ((err && err.message) || err))
        }
        const mkdir = args.mkdir !== false
        if (mkdir) {
          const parent = remoteDirname(p)
          // create each level from the root so mkdir -p semantics survive even
          // when intermediate privileged-parent dirs don't allow create (best effort)
          const segs = parent.split('/').filter(Boolean)
          let cur = ''
          for (const s of segs) {
            cur += '/' + s
            try { await sftp.mkdir(cur) } catch { /* exists or no perms */ }
          }
        }
        const buf = Buffer.from(content, 'utf8')
        await sftp.writeFile(p, buf)
        const bytes = Buffer.byteLength(content, 'utf8')
        return { ok: true, bytes, text: `wrote ${bytes} bytes to ${p}` }
      },
    }),

    defineTool({
      name: 'rw_disconnect',
      description:
        'Close the current SSH connection to the remote host, releasing the persistent pool. Useful to rotate connections or after a long idle.',
      parameters: {},
      output: {
        schema: { type: 'object', additionalProperties: false, properties: { ok: { type: 'boolean', required: true }, text: { type: 'string' } } },
        render: (_a, a) => [{ type: 'text', text: a.text }],
      },
      async execute() {
        pool.close()
        return { ok: true, text: 'disconnected' }
      },
    }),
  ]

  for (const t of tools) {
    ctx.tools.register(t)
  }

  // ── system-prompt injection: the current remote workspace + guard mode ─────
  ctx.systemPrompt.section({
    name: 'dsh-remote',
    order: 88,
    text: () => {
      const w = wsPath()
      const guardNote = guardMode === 'low'
        ? '## Remote guard mode: LOW AUTONOMY\n' +
          'Before mutating the remote host (creating files or executing commands), first state a concrete action plan with reasons. Every remote create/execute/delete/modify then requires user approval before it runs. Reads (rw_list_dir / rw_read_file / rw_info) do not need approval.'
        : '## Remote guard mode: HIGH AUTONOMY\n' +
          'You may read, create, and execute on the remote host freely. Deleting or modifying files (rm/mv/sed -i/chmod/overwrite-redirect, or rw_write_file over existing content) still requires user approval before it runs.'
      if (!w || !config.host) return guardNote
      return (
        '## Remote workspace\n' +
        `Current remote workspace: ${config.username}@${config.host}:${w}\n` +
        'Use the remote_* tools (rw_list_dir / rw_read_file / rw_exec) to inspect and act on files on the remote host. Treat this directory as the working root for this task.\n\n' +
        guardNote
      )
    },
  })

  // ── slash command: /remote reports status + connection hints ──────────────
  const commands = ctx.get('commands')
  if (commands !== undefined) {
    commands.register({
      name: 'remote',
      description: 'Show the current remote workspace / connection status and how to use remote tools.',
      handler: (invocation) => {
        const s = status()
        return {
          kind: 'success',
          text:
            `Remote host: ${s.username}@${s.host || '<none>'} (connected: ${s.connected})\n` +
            `Remote workspace: ${s.workspace || '(none)'}\n` +
            `\nUse tools: rw_list_dir / rw_read_file / rw_exec.` +
            (s.workspace ? `\nCurrently working in ${s.workspace}.` : ''),
        }
      },
    })
    commands.register({
      name: 'remote-guard',
      description: 'Switch the remote guard mode (high autonomy / low autonomy).',
      input: { hint: '<high|low>' },
      handler: ({ rawInput }) => {
        const arg = String(rawInput || '').trim().toLowerCase()
        if (arg !== 'high' && arg !== 'low') {
          return { kind: 'success', text: `current guard mode: ${guardMode} (use /remote-guard high|low)` }
        }
        setGuardMode(arg)
        return { kind: 'success', text: `guard mode set to ${arg}` }
      },
    })
  }

  // ── JSON endpoints for settings UI ─────────────────────────────────────────
  const webServer = ctx.get('webServer')
  if (webServer === undefined) return

  const sendJson = (res, status, body) => {
    res.statusCode = status
    res.setHeader('Content-Type', 'application/json; charset=utf-8')
    res.end(JSON.stringify(body))
  }
  const readBody = (req) =>
    new Promise((resolve) => {
      const chunks = []
      req.on('data', (c) => chunks.push(c))
      req.on('end', () => resolve(chunks.join('')))
    })
  /** Read the raw request body as a Buffer (for binary uploads). */
  const readBodyBuf = (req) =>
    new Promise((resolve) => {
      const chunks = []
      req.on('data', (c) => chunks.push(c))
      req.on('end', () => resolve(Buffer.concat(chunks)))
    })

  const routes = [
    {
      kind: 'exact',
      path: '/dsh-remote/status',
      handler: async (req, res) => {
        if (req.method === 'GET') return sendJson(res, 200, status())
        sendJson(res, 405, { error: 'method not allowed' })
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/guard',
      handler: async (req, res) => {
        if (req.method === 'GET') return sendJson(res, 200, { ok: true, mode: guardMode })
        if (req.method === 'POST') {
          try {
            const body = JSON.parse((await readBody(req)) || '{}')
            const mode = String(body.mode || '').toLowerCase()
            if (mode !== 'high' && mode !== 'low') return sendJson(res, 400, { ok: false, error: 'mode must be high or low' })
            setGuardMode(mode)
            return sendJson(res, 200, { ok: true, mode: guardMode })
          } catch (err) {
            return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
          }
        }
        sendJson(res, 405, { ok: false, error: 'method not allowed' })
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/connect',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { error: 'method not allowed' })
        try {
          const payload = JSON.parse((await readBody(req)) || '{}')
          pool.setTarget({
            host: payload.host,
            port: payload.port,
            username: payload.username,
            password: payload.password !== undefined && payload.password !== '' ? payload.password : undefined,
            privateKeyPath: payload.privateKeyPath,
            workspace: payload.workspace,
          })
          await pool.exec('echo ok', Math.min(config.commandTimeoutMs, 8000))
          return sendJson(res, 200, { ok: true, ...status() })
        } catch (err) {
          return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/ls',
      handler: async (req, res) => {
        try {
          const m = (req.url || '').match(/path=([^&]*)/)
          const p = m ? decodeURIComponent(m[1]) : wsPath()
          const out = await listDirStructured(p || '/')
          return sendJson(res, 200, { path: p, items: out.items })
        } catch (err) {
          return sendJson(res, 500, { error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/workspace',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { error: 'method not allowed' })
        try {
          const payload = JSON.parse((await readBody(req)) || '{}')
          const p = normalizeRemotePath(String(payload.path || ''))
          if (!p || p === '/') return sendJson(res, 400, { error: 'path must be an absolute directory' })
          const r = await pool.exec(`if [ -d ${shq(p)} ]; then echo DIR; else echo NOTDIR; fi`)
          if (r.stdout.trim() !== 'DIR') return sendJson(res, 400, { ok: false, error: `not a directory: ${p}` })
          config.workspace = p
          const local = ensureMirror(p, config.host, config.username, config.port)
          return sendJson(res, 200, { ok: true, workspace: p, localMirror: local, ...status() })
        } catch (err) {
          return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/mirror',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { error: 'method not allowed' })
        try {
          const payload = JSON.parse((await readBody(req)) || '{}')
          const p = normalizeRemotePath(String(payload.path || ''))
          if (!p || p === '/') return sendJson(res, 400, { ok: false, error: 'path must be an absolute directory' })
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured/connected — connect first' })
          // optional: verify it's a directory over SSH when connected
          if (pool.client) {
            const r = await pool.exec(`if [ -d ${shq(p)} ]; then echo DIR; else echo NOTDIR; fi`)
            if (r.stdout.trim() !== 'DIR') return sendJson(res, 400, { ok: false, error: `not a directory (or unreachable): ${p}` })
          }
          const local = ensureMirror(p, config.host, config.username, config.port)
          config.workspace = p
          return sendJson(res, 200, { ok: true, path: p, localMirror: local, ...status() })
        } catch (err) {
          return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/local-pick',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const dp = (ctx && ctx.get && ctx.get('directoryPicker')) || null
          if (!dp || typeof dp.capability !== 'function') return sendJson(res, 400, { ok: false, error: '本地目录选择器服务不可用（缺少 DSH directory-picker backend）' })
          const cap = await Promise.resolve(dp.capability())
          if (!cap || cap.kind !== 'native' || typeof cap.pick !== 'function') return sendJson(res, 400, { ok: false, error: '本地目录选择器不可用（当前为非原生/浏览后端，请在输入框手动填本地路径）' })
          const pickAbort = new AbortController()
          const signal = pickAbort.signal || null
          const picked = await Promise.resolve(cap.pick(signal))
          pickAbort.abort()
          if (!picked || typeof picked !== 'string') return sendJson(res, 200, { ok: true, cancelled: true })
          return sendJson(res, 200, { ok: true, path: picked })
        } catch (err) {
          return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/machines',
      handler: async (req, res) => {
        if (req.method === 'GET') {
          return sendJson(res, 200, { machines: machines.map(sanitizeMachine), currentId: store.currentId })
        }
        if (req.method === 'POST') {
          try {
            const body = JSON.parse((await readBody(req)) || '{}')
            const action = body.action || 'add'
            if (action === 'add' || action === 'update') {
              const host = String(body.host || '').trim()
              if (!host) return sendJson(res, 400, { ok: false, error: 'host required' })
              const rec = {
                id: body.id || machineId(),
                name: String(body.name || '').trim() || host,
                host,
                port: Number(body.port) || 22,
                username: String(body.username || '').trim() || 'root',
                password: body.password || '',
                privateKeyPath: String(body.privateKeyPath || '').trim(),
                passphrase: body.passphrase || '',
                workspace: String(body.workspace || '').trim(),
              }
              const i = machineIndex(rec.id)
              if (i >= 0) machines[i] = rec; else machines.push(rec)
              if (!store.currentId) { store.currentId = rec.id }
              saveMachines(machines, store.currentId)
              if (store.currentId === rec.id) applyMachine(config, rec)
              return sendJson(res, 200, { ok: true, machine: sanitizeMachine(rec), machines: machines.map(sanitizeMachine), currentId: store.currentId })
            }
            if (action === 'delete') {
              const i = machineIndex(String(body.id || ''))
              if (i < 0) return sendJson(res, 404, { ok: false, error: 'machine not found' })
              machines.splice(i, 1)
              if (store.currentId === body.id) store.currentId = machines[0] ? machines[0].id : null
              saveMachines(machines, store.currentId)
              return sendJson(res, 200, { ok: true, machines: machines.map(sanitizeMachine), currentId: store.currentId })
            }
            return sendJson(res, 400, { ok: false, error: 'unknown action' })
          } catch (err) {
            return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
          }
        }
        return sendJson(res, 405, { ok: false, error: 'method not allowed' })
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/test-connect',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const body = JSON.parse((await readBody(req)) || '{}')
          const probe = new SshPool({
            ...config,
            host: String(body.host || config.host),
            port: Number(body.port) || config.port,
            username: String(body.username || config.username),
            password: String(body.password || ''),
            privateKeyPath: String(body.privateKeyPath || config.privateKeyPath),
            passphrase: String(body.passphrase || ''),
            connectTimeoutMs: Math.min(Math.max(Number(body.connectTimeoutMs) || config.connectTimeoutMs, 2000), 30000),
            commandTimeoutMs: 10000,
          })
          const started = Date.now()
          await probe.connect()
          await probe.exec('true', 10000)
          probe.close()
          return sendJson(res, 200, { ok: true, host: probe.config.host, user: probe.config.username, latencyMs: Date.now() - started })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/current',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const body = JSON.parse((await readBody(req)) || '{}')
          const okSet = setCurrent(String(body.id || ''))
          if (!okSet) return sendJson(res, 404, { ok: false, error: 'machine not found' })
          return sendJson(res, 200, { ok: true, ...status() })
        } catch (err) {
          return sendJson(res, 500, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/exec',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const body = JSON.parse((await readBody(req)) || '{}')
          const command = String(body.command || '').trim()
          if (!command) return sendJson(res, 400, { ok: false, error: 'command required' })
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured — 先在设置里连接机器' })
          const res2 = await pool.exec(command, Number(body.timeoutMs) || config.commandTimeoutMs)
          const text = await (async () => {
            const parts = []
            if (res2.stdout) parts.push(res2.stdout.replace(/\s+$/, ''))
            if (res2.stderr) parts.push('-- stderr --\n' + res2.stderr.replace(/\s+$/, ''))
            if (!parts.length) parts.push('(no output)')
            let t = parts.join('\n')
            if (res2.signal === 'TIMEOUT') t += `\n[command timed out after ${Number(body.timeoutMs) || config.commandTimeoutMs}ms]`
            else if (res2.code !== 0) t += `\n[exit code: ${res2.code}]`
            return t
          })()
          return sendJson(res, 200, { ok: true, output: text, code: res2.code, signal: res2.signal, host: config.host, user: config.username })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      kind: 'exact',
      path: '/dsh-remote/complete',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const body = JSON.parse((await readBody(req)) || '{}')
          const line = String(body.line || '')
          const cursor = Number.isFinite(body.cursor) ? Number(body.cursor) : line.length
          if (!config.host) return sendJson(res, 200, { ok: false, error: 'no remote host configured' })

          // Split the line up to the cursor into tokens. The last token is the
          // "word being typed" (may be empty right after a space).
          const before = line.slice(0, Math.max(0, Math.min(cursor, line.length)))
          const head = before.slice(0, before.lastIndexOf(' ') + 1)
          const word = before.slice(before.lastIndexOf(' ') + 1)
          const isCommandPosition = head.trim() === ''
          const hasSlash = word.includes('/')

          // If the word carries a slash (path-like) or we're at the command
          // position AND the word looks like a command name, prefer file
          // completion only for path-like words; otherwise complete commands.
          let candidates = []
          let kind = 'files'
          if (hasSlash) {
            kind = 'files'
            const r = await pool.exec(
              `bash -c 'compgen -f -- ${shq(word)}'`,
              Math.min(config.commandTimeoutMs, 8000),
            )
            if (r.code === 0 && r.stdout) candidates = r.stdout.split('\n').filter(Boolean)
          } else if (isCommandPosition) {
            kind = 'commands'
            const r = await pool.exec(
              `bash -c 'compgen -c -- ${shq(word)}'`,
              Math.min(config.commandTimeoutMs, 8000),
            )
            if (r.code === 0 && r.stdout) candidates = r.stdout.split('\n').filter(Boolean)
          } else {
            // A non-first token without a slash: prefer files in the current
            // directory (so `cat rea<Tab>` completes `readme.txt`), then fall
            // back to commands if nothing matched.
            kind = 'files'
            const rf = await pool.exec(
              `bash -c 'compgen -f -- ${shq(word)}'`,
              Math.min(config.commandTimeoutMs, 8000),
            )
            if (rf.code === 0 && rf.stdout) candidates = rf.stdout.split('\n').filter(Boolean)
            if (!candidates.length) {
              kind = 'commands'
              const rc = await pool.exec(
                `bash -c 'compgen -c -- ${shq(word)}'`,
                Math.min(config.commandTimeoutMs, 8000),
              )
              if (rc.code === 0 && rc.stdout) candidates = rc.stdout.split('\n').filter(Boolean)
            }
          }

          // Longest common prefix of all candidates: the leading part every
          // candidate shares beyond the already-typed word.
          let commonPrefix = ''
          if (candidates.length) {
            commonPrefix = candidates[0]
            for (const c of candidates.slice(1)) {
              let i = 0
              while (i < commonPrefix.length && i < c.length && commonPrefix[i] === c[i]) i++
              commonPrefix = commonPrefix.slice(0, i)
            }
          }
          // Only offer the common-prefix extension (what typing more gives you).
          const extendable = commonPrefix.length > word.length ? commonPrefix.slice(word.length) : ''

          return sendJson(res, 200, {
            ok: true,
            word,
            head,
            candidates: candidates.slice(0, 200),
            commonPrefix: extendable,
            kind,
            // Suggest a trailing '/' when every candidate is a directory.
            dirOnly: kind === 'files' && candidates.length > 0 && candidates.every((c) => !/[\s]/.test(c) && c.endsWith('/')),
          })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      // Serve a remote image over SFTP (read-only) so the browser can show it
      // without the server installing anything. path is a remote absolute path.
      kind: 'exact',
      path: '/dsh-remote/img',
      handler: async (req, res) => {
        if (req.method !== 'GET') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const u = new URL(req.url || '/', 'http://x')
          const p = u.searchParams.get('path') || ''
          if (!p) return sendJson(res, 400, { ok: false, error: 'path required' })
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          const ext = p.toLowerCase().split('.').pop() || ''
          const mimeMap = { png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif', webp: 'image/webp', bmp: 'image/bmp', svg: 'image/svg+xml', ico: 'image/x-icon' }
          const mime = mimeMap[ext] || 'application/octet-stream'
          // Cap at ~20MB so a huge accidental file can't stall the browser.
          const sftp = await pool.sftp()
          const st = await sftp.stat(p).catch(() => null)
          if (!st || st.size > 20 * 1024 * 1024) return sendJson(res, 400, { ok: false, error: 'not a file or too large (>20MB)' })
          const buf = await sftp.readFile(p)
          res.statusCode = 200
          res.setHeader('Content-Type', mime)
          res.setHeader('Content-Length', String(buf.length))
          res.setHeader('Cache-Control', 'no-store')
          res.end(Buffer.from(buf))
        } catch (err) {
          res.statusCode = 500
          res.setHeader('Content-Type', 'application/json; charset=utf-8')
          res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
        }
      },
    },
    {
      // Serve vendored xterm assets from this plugin's own node_modules so the
      // browser terminal loads without any CDN or network dependency.
      kind: 'prefix',
      path: '/dsh-remote/assets',
      handler: async (req, res) => {
        try {
          const u = new URL(req.url || '/', 'http://x')
          const name = u.pathname.slice('/dsh-remote/assets/'.length)
          if (!/^[A-Za-z0-9._-]+$/.test(name)) { res.writeHead(400); res.end(); return }
          // Resolve xterm from the plugin's OWN node_modules first, then fall back
          // to every ancestor directory's node_modules (covers hoisted installs)
          // without hard-coding any machine-specific profile path.
          const assetDirs = []
          for (let dir = PLUGIN_ROOT; ; dir = path.dirname(dir)) {
            assetDirs.push(path.join(dir, 'node_modules'))
            const parent = path.dirname(dir)
            if (parent === dir) break
          }
          const map = {
            'xterm.js': ['xterm', 'lib', 'xterm.js'],
            'xterm.css': ['xterm', 'css', 'xterm.css'],
            'addon-fit.js': ['xterm-addon-fit', 'lib', 'xterm-addon-fit.js'],
          }
          const rel = map[name]
          if (!rel) { res.writeHead(404); res.end(); return }
          let body
          for (const dir of assetDirs) {
            try { body = readFileSync(path.join(dir, ...rel)); break } catch {}
          }
          if (body === undefined) { res.writeHead(404); res.end(); return }
          const mime = name.endsWith('.css') ? 'text/css; charset=utf-8' : 'text/javascript; charset=utf-8'
          res.statusCode = 200
          res.setHeader('Content-Type', mime)
          res.setHeader('Content-Length', String(body.length))
          res.setHeader('Cache-Control', 'no-cache')
          res.end(body)
        } catch (err) {
          res.writeHead(500)
          res.end()
        }
      },
    },
    {
      // The connected user's real home directory (may not be /home/<user>).
      kind: 'exact',
      path: '/dsh-remote/home',
      handler: async (req, res) => {
        if (req.method !== 'GET') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          const r = await pool.exec('echo $HOME', Math.min(config.commandTimeoutMs, 8000))
          const home = String(r.stdout || '').trim() || ('/home/' + (config.username || 'root'))
          return sendJson(res, 200, { ok: true, home })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      // Rich directory listing (name/size/mtime/mode/type) for the file manager.
      kind: 'exact',
      path: '/dsh-remote/list',
      handler: async (req, res) => {
        if (req.method !== 'GET') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const u = new URL(req.url || '/', 'http://x')
          let p = u.searchParams.get('path') || '/'
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          // Reuse a cached SFTP session; on channel failure, reset and retry once.
          let sftp
          try {
            sftp = await pool.sftp()
            await sftp.realpath(p)
          } catch (e0) {
            pool.close()
            sftp = await pool.sftp()
          }
          const rp = await sftp.realpath(p).catch(() => p)
          const list = await sftp.readdir(rp)
          const items = list.map((e) => {
            const a = e.attrs || {}
            const isDir = (a.mode & 0o170000) === 0o040000
            const isLink = (a.mode & 0o170000) === 0o120000
            return {
              name: e.filename,
              size: isDir ? 0 : (a.size || 0),
              mtime: a.mtime ? a.mtime * 1000 : 0,
              mode: a.mode || 0,
              dir: isDir,
              link: isLink,
            }
          }).filter((e) => e.name !== '.' && e.name !== '..')
          items.sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1))
          return sendJson(res, 200, { ok: true, path: rp, items })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      // Download a remote file (SFTP read → browser attachment).
      kind: 'exact',
      path: '/dsh-remote/download',
      handler: async (req, res) => {
        if (req.method !== 'GET') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const u = new URL(req.url || '/', 'http://x')
          const p = u.searchParams.get('path') || ''
          if (!p) return sendJson(res, 400, { ok: false, error: 'path required' })
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          const sftp = await pool.sftp()
          const st = await sftp.stat(p).catch(() => null)
          if (!st || st.size > 200 * 1024 * 1024) return sendJson(res, 400, { ok: false, error: 'not a file or too large (>200MB)' })
          const buf = await sftp.readFile(p)
          const name = p.split('/').filter(Boolean).pop() || 'download'
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/octet-stream')
          res.setHeader('Content-Length', String(buf.length))
          res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(name)}`)
          res.setHeader('Cache-Control', 'no-store')
          res.end(Buffer.from(buf))
        } catch (err) {
          res.statusCode = 500
          res.setHeader('Content-Type', 'application/json; charset=utf-8')
          res.end(JSON.stringify({ ok: false, error: String((err && err.message) || err) }))
        }
      },
    },
    {
      // Upload a file into a remote directory. Body is raw bytes; the target
      // filename comes from the query `name`, directory from `dir`.
      kind: 'exact',
      path: '/dsh-remote/upload',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const u = new URL(req.url || '/', 'http://x')
          const dir = u.searchParams.get('dir') || '/'
          const name = u.searchParams.get('name') || ''
          if (!name) return sendJson(res, 400, { ok: false, error: 'name required' })
          if (!/^[^/\\]+$/.test(name)) return sendJson(res, 400, { ok: false, error: 'invalid filename' })
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          const body = await readBodyBuf(req)
          if (!body || !body.length) return sendJson(res, 400, { ok: false, error: 'empty body' })
          const sftp = await pool.sftp()
          const target = (dir.endsWith('/') ? dir : dir + '/') + name
          await sftp.writeFile(target, body)
          return sendJson(res, 200, { ok: true, path: target })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
    {
      // mkdir / delete / rename share one endpoint keyed by `action`.
      kind: 'exact',
      path: '/dsh-remote/fs',
      handler: async (req, res) => {
        if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' })
        try {
          const body = JSON.parse((await readBody(req)) || '{}')
          const action = String(body.action || '')
          if (!config.host) return sendJson(res, 400, { ok: false, error: 'no remote host configured' })
          const sftp = await pool.sftp()
          if (action === 'mkdir') {
            const p = String(body.path || '').trim()
            if (!p) return sendJson(res, 400, { ok: false, error: 'path required' })
            await sftp.mkdir(p)
            return sendJson(res, 200, { ok: true })
          }
          if (action === 'delete') {
            const p = String(body.path || '').trim()
            if (!p || p === '/') return sendJson(res, 400, { ok: false, error: 'invalid path' })
            // Try rmdir first (empty dir), fall back to unlink (file).
            try { await sftp.rmdir(p) } catch { await sftp.unlink(p) }
            return sendJson(res, 200, { ok: true })
          }
          if (action === 'rename') {
            const from = String(body.from || '').trim()
            const to = String(body.to || '').trim()
            if (!from || !to) return sendJson(res, 400, { ok: false, error: 'from/to required' })
            await sftp.rename(from, to)
            return sendJson(res, 200, { ok: true })
          }
          return sendJson(res, 400, { ok: false, error: 'unknown action' })
        } catch (err) {
          return sendJson(res, 200, { ok: false, error: String((err && err.message) || err) })
        }
      },
    },
  ]

  const disposers = routes.map((r) => webServer.register(r))

  // ── WebSocket PTY terminal ────────────────────────────────────────────────
  // Each connection gets its own SSH shell session (independent of the exec/sftp
  // pool), bridged to the browser over a raw WebSocket.
  if (typeof webServer.registerUpgrade === 'function') {
    const wss = new WebSocketServer({ noServer: true })
    const termDisposer = webServer.registerUpgrade({
      path: '/dsh-remote/term',
      handler: (req, socket, head) => {
        if (!config.host) { socket.destroy(); return }
        wss.handleUpgrade(req, socket, head, (ws) => {
          ws.on('error', () => { try { ws.close() } catch {} })
          let shellHandle = null
          let closed = false
          const cols = 80
          const rows = 24
          pool.openShell({
            cols, rows,
            onData: (d) => { if (!closed && ws.readyState === WebSocket.OPEN) ws.send(d) },
            onClose: () => { closed = true; try { ws.close() } catch {} },
          }).then((handle) => {
            shellHandle = handle
            ws.on('message', (data, isBinary) => {
              try {
                const stream = handle.stream
                if (isBinary) stream.write(data)
                else {
                  // Text frames: JSON control messages ({type:'input'|'resize'}) or raw keystrokes.
                  const s = String(data)
                  if (s.charAt(0) === '{') {
                    try {
                      const msg = JSON.parse(s)
                      if (msg.type === 'resize' && Number.isFinite(msg.cols) && Number.isFinite(msg.rows)) {
                        handle.stream.setWindow(msg.rows, msg.cols, 0, 0)
                      }
                    } catch {}
                    return
                  }
                  handle.stream.write(s)
                }
              } catch {}
            })
            ws.on('close', () => { closed = true; try { handle.stream.end() } catch {} try { handle.client.end() } catch {} })
          }).catch((err) => {
            try { ws.send(JSON.stringify({ type: 'error', error: String((err && err.message) || err) })) } catch {}
            try { ws.close() } catch {}
          })
        })
      },
    })
    ctx.effect(() => () => { try { termDisposer() } catch {} try { wss.close() } catch {} }, 'dsh-remote.term')
  }

  ctx.effect(() => () => disposers.forEach((d) => d && d()), 'dsh-remote.routes')
}