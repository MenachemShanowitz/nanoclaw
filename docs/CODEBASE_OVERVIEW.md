# NanoClaw Codebase Overview

A plain-language walkthrough of the entire NanoClaw codebase. Assumes basic TypeScript knowledge.

---

## What Is NanoClaw?

NanoClaw is a personal AI assistant you talk to through WhatsApp. You message it from your phone (e.g., "@Andy what's the weather?"), and it replies using Claude (Anthropic's AI model). It runs on your Mac as a background service.

The key idea: the AI agent runs inside an **isolated Linux container** (a lightweight virtual machine), so even though it can run bash commands and read/write files, it can only touch the specific folders you've allowed. It can't access your Mac's filesystem freely.

---

## The Big Picture

Here's the flow in plain English:

1. You send a WhatsApp message like `@Andy what's the weather?`
2. The **baileys** library (a WhatsApp Web client for Node.js) receives that message
3. The message gets stored in a **SQLite database**
4. A **polling loop** checks the database every 2 seconds for new messages
5. If the message is from a registered group and starts with the trigger word, it gets processed
6. The app **spawns a Linux container** (using Apple Container) and runs Claude inside it
7. Claude does its thing (searches the web, reads files, etc.) and produces a response
8. The response gets sent back to WhatsApp

```
Your Phone (WhatsApp)
        |
        v
  baileys library  --->  SQLite database
                              |
                              v
                     Polling loop (every 2s)
                              |
                              v
                     Message matches trigger?
                         Yes |
                              v
                     Spawn Linux container
                     Run Claude Agent SDK
                              |
                              v
                     Response sent to WhatsApp
```

---

## Project Structure

```
nanoclaw/
  src/                    <-- Host-side TypeScript code (runs on your Mac)
  container/              <-- Container-side code (runs inside the Linux VM)
  groups/                 <-- Per-group memory and files (persists between conversations)
  store/                  <-- WhatsApp auth + SQLite database (gitignored)
  data/                   <-- Runtime state: sessions, registered groups, IPC files (gitignored)
  docs/                   <-- Documentation
  launchd/                <-- macOS service configuration
  .claude/skills/         <-- Claude Code skill definitions (setup, customize, debug)
```

There are two separate Node.js projects:
- **The host app** (`src/` + `package.json`) -- runs on your Mac
- **The agent runner** (`container/agent-runner/`) -- gets built into a Docker-like image and runs inside the container

---

## Source Files Explained (Host Side - `src/`)

### `src/config.ts` -- Configuration Constants

This is where all the settings live. Key values:

| Constant | Default | What it does |
|----------|---------|--------------|
| `ASSISTANT_NAME` | `'Andy'` | The trigger word. Messages must start with `@Andy` |
| `POLL_INTERVAL` | `2000` (2s) | How often to check for new WhatsApp messages |
| `SCHEDULER_POLL_INTERVAL` | `60000` (1 min) | How often to check for due scheduled tasks |
| `CONTAINER_IMAGE` | `'nanoclaw-agent:latest'` | The container image name |
| `CONTAINER_TIMEOUT` | `300000` (5 min) | Max time a container can run before being killed |
| `TRIGGER_PATTERN` | `/^@Andy\b/i` | Regex built from `ASSISTANT_NAME`. Case-insensitive |
| `PERSISTENT_CONTAINER_MODE` | `true` | Keeps containers alive between messages for faster responses |

The `TRIGGER_PATTERN` is a regex that matches messages starting with `@Andy` (or whatever name you set). The `\b` means "word boundary" so `@Andyyyy` won't match.

### `src/types.ts` -- TypeScript Interfaces

Defines the shapes of data used throughout the app. The key types:

- **`RegisteredGroup`** -- A WhatsApp group the bot listens to. Has a `name`, `folder` (where its files live), `trigger` word, and optional `containerConfig` (extra directory mounts, timeout overrides).

- **`ScheduledTask`** -- A job that runs on a schedule. Has a `schedule_type` (`cron`, `interval`, or `once`), the `prompt` to run, and a `context_mode` (`group` = shares the group's conversation history, `isolated` = fresh session each time).

- **`NewMessage`** -- A message pulled from the database, with `chat_jid` (WhatsApp's unique ID for the chat), `sender_name`, `content`, and `timestamp`.

### `src/db.ts` -- Database Layer

Uses **better-sqlite3** (a fast, synchronous SQLite library for Node.js). Creates three tables:

1. **`chats`** -- Tracks all known WhatsApp chats (JID, name, last message time). Used for group discovery.

2. **`messages`** -- Stores actual message content (only for registered groups). Fields: `id`, `chat_jid`, `sender`, `sender_name`, `content`, `timestamp`, `is_from_me`.

3. **`scheduled_tasks`** -- Stores scheduled jobs with their schedule, status, and last result.

4. **`task_run_logs`** -- History of task executions (when it ran, how long, success/error).

Key functions:
- `storeMessage()` -- Saves an incoming WhatsApp message
- `getNewMessages()` -- Fetches unprocessed messages since the last timestamp
- `getMessagesSince()` -- Gets all messages in a chat since a given time (used to give the AI conversation context)
- `getDueTasks()` -- Finds scheduled tasks that are due to run
- `createTask()`, `updateTask()`, `deleteTask()` -- CRUD for scheduled tasks

The database file lives at `store/messages.db` and is gitignored.

### `src/utils.ts` -- Utility Functions

Two simple helper functions:
- `loadJson(path, default)` -- Reads a JSON file, returns default if it doesn't exist
- `saveJson(path, data)` -- Writes data as formatted JSON, creating directories if needed

### `src/index.ts` -- Main Application (The Brain)

This is the largest file and the entry point. It ties everything together. Here's what it does, section by section:

**Startup (`main()` function):**
1. Ensures the Apple Container system is running (starts it if not)
2. Initializes the SQLite database
3. Loads saved state (which groups are registered, session IDs, timestamps)
4. Optionally creates a `PersistentContainerManager` for faster responses
5. Connects to WhatsApp

**WhatsApp Connection (`connectWhatsApp()`):**
- Uses baileys to connect as a "linked device" (like WhatsApp Web)
- If not authenticated, shows an error and exits (you need to run `/setup` first)
- On successful connection, starts three subsystems:
  1. **Scheduler loop** -- checks for due tasks every minute
  2. **IPC watcher** -- monitors filesystem for messages from containers
  3. **Message loop** -- polls for new messages every 2 seconds

**Message Processing (`processMessage()`):**
- Checks if the message is from a registered group
- For the main group (your private self-chat): responds to ALL messages
- For other groups: only responds if the message starts with `@Andy`
- Gathers all messages since the last time the AI spoke in that chat (so the AI has context)
- Formats them as XML: `<message sender="John" time="...">the text</message>`
- Sends this to the agent and returns the response

**Running the Agent (`runAgent()`):**
- Prepares "snapshots" -- JSON files that tell the container about available tasks, groups, and projects
- Two modes:
  - **Persistent mode** (main group): Uses a long-running container with real-time streaming. Sends progress updates to WhatsApp while working.
  - **One-shot mode** (other groups): Spawns a fresh container, waits for it to finish, returns the result

**IPC Watcher (`startIpcWatcher()`):**
- The containers can't directly call WhatsApp or the database -- they're isolated
- Instead, containers write JSON files to a shared folder (`data/ipc/{group}/`)
- The IPC watcher polls this folder every second and processes the files
- Handles: sending messages, creating/pausing/resuming/cancelling tasks, registering groups, switching projects
- **Authorization is enforced**: non-main groups can only send messages to their own chat, schedule tasks for themselves, etc.

**Group Management:**
- `registerGroup()` -- Adds a group to `registered_groups.json` and creates its folder
- `syncGroupMetadata()` -- Fetches group names from WhatsApp (runs on startup and daily)
- Groups are identified by their WhatsApp JID (a unique string like `120363336345536173@g.us`)

### `src/container-runner.ts` -- Container Spawning

This file handles launching one-shot containers (spawn, wait, get result). Key concepts:

**Volume Mounts (`buildVolumeMounts()`):**

When a container starts, specific host folders are "mounted" into it. What gets mounted depends on the group:

| What | Container Path | Main Group | Other Groups |
|------|---------------|------------|--------------|
| Group's own folder | `/workspace/group` | Yes (rw) | Yes (rw) |
| Entire project root | `/workspace/project` | Yes (rw) | No |
| Global memory folder | `/workspace/global` | No (has project) | Yes (read-only) |
| Claude sessions | `/home/node/.claude` | Yes (rw) | Yes (rw) |
| IPC directory | `/workspace/ipc` | Yes (rw) | Yes (rw) |
| Auth credentials | `/workspace/env-dir` | Yes (ro) | Yes (ro) |
| Extra directories | `/workspace/extra/*` | If configured | If configured |

The container literally cannot see anything else on your Mac.

**Running a Container (`runContainerAgent()`):**
1. Builds the list of volume mounts
2. Constructs the `container run` command with all the mount flags
3. Spawns the process, pipes the input JSON to stdin
4. Reads stdout for the result (looks for sentinel markers `---NANOCLAW_OUTPUT_START---` and `---NANOCLAW_OUTPUT_END---`)
5. Has a timeout (default 5 minutes) -- kills the container if it takes too long
6. Writes a log file to `groups/{name}/logs/` for each run

### `src/persistent-container.ts` -- Long-Running Containers

An optimization for the main group. Instead of spawning a new container for every message:

1. Keeps a container running and connected via stdin/stdout pipes
2. Sends messages as JSON lines and reads responses as JSON lines
3. Supports **streaming**: the container sends `progress` and `tool` messages as it works, which get batched and forwarded to WhatsApp as "[working...]" updates
4. Has health checking (sends `ping`, expects `pong`)
5. Auto-restarts if the container dies (up to 3 attempts)

The `ProgressBatcher` class in `index.ts` throttles these progress messages to avoid WhatsApp rate limiting (minimum 3 seconds between updates, max 10 updates per request).

### `src/mount-security.ts` -- Mount Allowlist System

Controls which host directories can be mounted into containers. The security configuration lives at `~/.config/nanoclaw/mount-allowlist.json` -- deliberately **outside** the project folder so containers can't modify it.

How it works:
1. You define "allowed roots" -- directories that are OK to mount (e.g., `~/projects`)
2. You define "blocked patterns" -- filenames/paths that should never be mounted (e.g., `.ssh`, `.env`, `credentials`)
3. When a mount is requested, it:
   - Resolves symlinks (prevents symlink attacks)
   - Checks against blocked patterns
   - Checks if the path is under an allowed root
   - Enforces read-only for non-main groups if configured

Built-in blocked patterns include: `.ssh`, `.gnupg`, `.aws`, `.kube`, `.docker`, `credentials`, `.env`, `id_rsa`, `private_key`, and more.

### `src/task-scheduler.ts` -- Scheduled Task Runner

Simple loop that runs every 60 seconds:
1. Queries the database for tasks where `next_run <= now` and `status = 'active'`
2. For each due task, spawns a container and runs the task's prompt
3. After running, calculates the next run time:
   - `cron`: Parses the cron expression to find the next occurrence
   - `interval`: Adds the interval milliseconds to now
   - `once`: No next run (task marked as `completed`)
4. Logs the run result to `task_run_logs`

Tasks run with the same container setup as regular messages -- they have access to the group's files, web search, bash, etc. If a task needs to message the user, it uses the `send_message` MCP tool.

### `src/whatsapp-auth.ts` -- Authentication Script

A standalone script (not part of the main app) for first-time WhatsApp setup:
1. Displays a QR code in the terminal
2. You scan it with your phone's WhatsApp app
3. Saves the credentials to `store/auth/`
4. Exits

Run separately via `npm run auth` or as part of the `/setup` skill.

---

## Source Files Explained (Container Side - `container/`)

### `container/Dockerfile` -- Container Image Definition

Builds a Linux image with:
- **Node.js 22** (base image)
- **Chromium** + dependencies (for browser automation)
- **agent-browser** (CLI tool for web page interaction)
- **Claude Code** (the `@anthropic-ai/claude-code` CLI, installed globally)
- The **agent-runner** code (compiled TypeScript)
- Workspace directories at `/workspace/group`, `/workspace/global`, etc.

The container runs as a non-root `node` user (uid 1000). The entrypoint script sources auth credentials from `/workspace/env-dir/env` if present, then runs the agent-runner.

### `container/build.sh` -- Build Script

A short shell script that runs `container build -t nanoclaw-agent:latest .` to build the container image using Apple Container (similar to `docker build`).

### `container/agent-runner/src/index.ts` -- The Agent Brain

This is the code that runs **inside** the container. It's the bridge between the host and Claude.

**Two modes:**

1. **One-shot mode** (default): Reads a single JSON input from stdin, runs Claude, outputs the result as JSON to stdout, exits.

2. **Persistent mode** (when `NANOCLAW_PERSISTENT=1`): Reads JSON messages line by line from stdin in a loop. Supports `ping`/`pong` health checks, `message` for running prompts, and `shutdown` for clean exit.

**How it runs Claude:**

Uses the `query()` function from `@anthropic-ai/claude-agent-sdk`:
```typescript
for await (const message of query({
  prompt: "the user's message",
  options: {
    cwd: '/workspace/group',
    resume: sessionId,          // continues previous conversation
    allowedTools: ['Bash', 'Read', 'Write', ...],
    permissionMode: 'bypassPermissions',
    mcpServers: { nanoclaw: ipcMcp }
  }
})) {
  // Process streaming messages from Claude
}
```

Key things:
- `cwd` is set to the group's workspace, so Claude sees the group's CLAUDE.md
- `resume` with a session ID lets Claude continue a previous conversation
- `permissionMode: 'bypassPermissions'` -- since the container is the sandbox, Claude doesn't need its own permission system
- Tools include Bash, file operations, web search, and the custom `nanoclaw` MCP tools

**Conversation archiving:**

Has a `PreCompact` hook -- when Claude's context gets too long and needs to be compacted, this hook archives the conversation transcript as a markdown file in `conversations/` before it's summarized. This provides searchable history.

### `container/agent-runner/src/ipc-mcp.ts` -- Custom Tools for Claude

Defines the tools that Claude can use to interact with the host system. These are implemented as an MCP (Model Context Protocol) server -- a standard way to give AI models tools.

Since the container is isolated, these tools work by **writing JSON files** to the `/workspace/ipc/` directory, which the host's IPC watcher picks up and processes.

The tools:

| Tool | What it does |
|------|-------------|
| `send_message` | Sends a WhatsApp message to the current group |
| `schedule_task` | Creates a new scheduled task (cron, interval, or one-time) |
| `list_tasks` | Reads the current tasks snapshot from a JSON file |
| `pause_task` | Writes a "pause" request to the IPC directory |
| `resume_task` | Writes a "resume" request to the IPC directory |
| `cancel_task` | Writes a "cancel" request to the IPC directory |
| `register_group` | Registers a new WhatsApp group (main only) |
| `list_projects` | Shows available projects from the mount allowlist |
| `set_project` | Switches the active project directory |

Each tool writes a JSON file like `1706745600000-abc123.json` to either `messages/` or `tasks/` inside the IPC directory. The host picks these up within 1 second.

### `container/skills/agent-browser.md` -- Browser Automation Skill

A reference document that teaches Claude how to use the `agent-browser` CLI tool inside the container. This lets the AI navigate websites, fill forms, take screenshots, click buttons, etc. -- all headless using Chromium.

---

## The Groups System

Groups are WhatsApp chats that the bot listens to. Each group has:

### Registration (`data/registered_groups.json`)
```json
{
  "120363336345536173@g.us": {
    "name": "Family Chat",
    "folder": "family-chat",
    "trigger": "@Andy",
    "added_at": "2026-01-31T12:00:00Z"
  }
}
```

### Folder Structure (`groups/{folder-name}/`)
```
groups/
  global/
    CLAUDE.md          <-- Shared memory, read by all groups, writable only from main
  main/
    CLAUDE.md          <-- Main channel's memory (has admin instructions)
    conversations/     <-- Archived conversation transcripts
    logs/              <-- Container run logs
  family-chat/
    CLAUDE.md          <-- This group's memory
    conversations/
    logs/
```

### The Main Group
The "main" group is your private self-chat in WhatsApp. It has special privileges:
- Responds to ALL messages (no trigger word needed)
- Can read/write global memory
- Can see and manage all groups' tasks
- Can register new groups
- Gets the entire project directory mounted (so the AI can modify NanoClaw itself)
- Uses persistent containers for faster responses with progress streaming

### Memory System
Claude automatically reads `CLAUDE.md` files when it starts:
- `groups/global/CLAUDE.md` -- Read by all groups (shared facts, preferences)
- `groups/{name}/CLAUDE.md` -- Read by that group only (group-specific memory)

When Claude learns something important, it writes to these files so it remembers next time.

---

## Communication Between Host and Container (IPC)

Since containers are isolated, the host and container communicate through a **file-based IPC system**:

```
Host writes:                          Container reads:
  data/ipc/{group}/current_tasks.json   /workspace/ipc/current_tasks.json
  data/ipc/{group}/available_groups.json /workspace/ipc/available_groups.json
  data/ipc/{group}/available_projects.json /workspace/ipc/available_projects.json

Container writes:                     Host reads:
  /workspace/ipc/messages/*.json        data/ipc/{group}/messages/*.json
  /workspace/ipc/tasks/*.json           data/ipc/{group}/tasks/*.json
```

The host writes snapshot files before each agent run so the container has current info. The container writes request files that the host picks up via polling.

**Authorization**: Each group has its own IPC directory. The host determines which group wrote a file based on which directory it's in -- the container can't fake being a different group because it only has access to its own IPC directory.

---

## Security Model

### Layers of Protection

1. **Container isolation** (primary) -- Agents run in Linux VMs. They can only see mounted directories.

2. **Mount allowlist** -- Stored at `~/.config/nanoclaw/mount-allowlist.json`, outside the project, unreachable from containers. Controls what can be mounted.

3. **Blocked patterns** -- Sensitive paths (`.ssh`, `.env`, `credentials`, etc.) are never mounted regardless of allowlist.

4. **IPC authorization** -- Non-main groups can only send messages to their own chat, schedule tasks for themselves, etc. Enforced by the host based on filesystem directory.

5. **Session isolation** -- Each group has separate Claude sessions. Groups can't see each other's conversation history.

6. **Credential filtering** -- Only `CLAUDE_CODE_OAUTH_TOKEN` and `ANTHROPIC_API_KEY` are exposed to containers. Everything else in `.env` is filtered out.

### Known Limitation
The AI auth token IS accessible inside the container (Claude needs it to work). The agent could theoretically extract it via bash. This is documented and noted as an area for improvement.

---

## Dependencies

### Host (`package.json`)
| Package | What it does |
|---------|-------------|
| `@whiskeysockets/baileys` | Unofficial WhatsApp Web API client |
| `better-sqlite3` | Fast SQLite database driver (synchronous) |
| `cron-parser` | Parses cron expressions to calculate next run time |
| `pino` + `pino-pretty` | Structured logging library |
| `qrcode-terminal` | Displays QR codes in terminal (for WhatsApp auth) |
| `zod` | Runtime type validation (used in MCP tool definitions) |

### Container (`container/agent-runner/package.json`)
| Package | What it does |
|---------|-------------|
| `@anthropic-ai/claude-agent-sdk` | Official SDK to run Claude programmatically |
| `cron-parser` | Same as above (validates cron inside container) |
| `zod` | Same as above (MCP tool schemas) |

---

## Deployment

NanoClaw runs as a **macOS launchd service** -- similar to a system service on Linux. The config is in `launchd/com.nanoclaw.plist`:

- **RunAtLoad**: Starts automatically when you log in
- **KeepAlive**: Restarts if it crashes
- Logs go to `logs/nanoclaw.log` and `logs/nanoclaw.error.log`
- Template placeholders (`{{NODE_PATH}}`, `{{PROJECT_ROOT}}`, `{{HOME}}`) get filled in during setup

---

## Skills System

Instead of adding features to the codebase, NanoClaw uses **Claude Code skills** -- markdown files that teach Claude Code how to modify the codebase. Located in `.claude/skills/`:

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `/setup` | First run | Install dependencies, authenticate WhatsApp, build container, configure service |
| `/customize` | Adding features | Interactive guide for adding channels, integrations, behaviors |
| `/debug` | Something broken | Diagnose container issues, read logs, check configuration |
| `/add-gmail` | Add email | Set up Gmail integration (OAuth, reading/sending email) |

The idea is that contributors add new skills rather than new features. A `/add-telegram` skill would teach Claude Code how to modify your specific fork to support Telegram, rather than the codebase trying to support every platform simultaneously.
