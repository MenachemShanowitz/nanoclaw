# NanoClaw User Guide

Everything you need to know to use NanoClaw, from first install to daily use.

---

## What Is This?

NanoClaw is a personal AI assistant you talk to through WhatsApp. You message it from your phone, and it replies using Claude. It runs as a background service on your Mac.

The assistant can search the web, read and write files, run commands, browse websites, schedule recurring tasks, and remember things across conversations. It runs inside isolated Linux containers, so it can only access directories you explicitly allow.

---

## Prerequisites

- macOS Tahoe (26) or later
- Node.js 20+
- [Claude Code](https://claude.ai/download) installed
- [Apple Container](https://github.com/apple/container) installed
- A WhatsApp account on your phone
- Either a Claude subscription (Pro/Team) or an Anthropic API key

---

## Setup

```bash
git clone https://github.com/gavrielc/nanoclaw.git
cd nanoclaw
claude
```

Once Claude Code opens, type `/setup`. It handles everything interactively:

1. **Installs dependencies** — runs `npm install`
2. **Checks Apple Container** — verifies it's installed, starts the container system
3. **Configures authentication** — asks for your Claude subscription token or API key, saves it to `.env`
4. **Builds the container image** — runs `./container/build.sh` (takes a few minutes the first time)
5. **Authenticates WhatsApp** — runs `npm run auth`, shows a QR code in your terminal. Open WhatsApp on your phone → Settings → Linked Devices → Link a Device → scan the QR code
6. **Asks your assistant's name** — default is "Andy", but you can pick anything. This becomes the trigger word (`@Andy`)
7. **Registers your main channel** — your WhatsApp self-chat (messages to yourself). This is your private admin channel
8. **Configures directory access** — asks which folders on your Mac the assistant should be able to access (e.g., `~/projects`)
9. **Starts the background service** — installs a launchd plist so NanoClaw runs automatically

When setup finishes, send a test message in your WhatsApp self-chat (the chat where you message yourself). Type anything — the main channel responds to all messages. If you get a reply, you're up and running.

---

## How It Works (The Short Version)

```
You send a WhatsApp message
        ↓
NanoClaw (running on your Mac) receives it via WhatsApp Web
        ↓
If it's from a registered chat and has the trigger word → process it
        ↓
Spawn an isolated Linux container, run Claude inside it
        ↓
Claude thinks, uses tools (web search, files, bash, etc.)
        ↓
Response sent back to WhatsApp
```

NanoClaw is a single Node.js process. It polls for new messages every 2 seconds, spawns containers to run Claude, and sends responses back. That's it.

---

## The Two Types of Chat

### Main Channel (Your Self-Chat)

Your main channel is your private WhatsApp self-chat — the conversation where you message yourself. It's the admin control panel.

**Key behaviors:**
- Responds to **every message** — no trigger word needed, just type normally
- Has **full project access** — can read/write the entire NanoClaw codebase
- Can **manage groups** — add, remove, and configure other chats
- Can **manage all tasks** — view and control scheduled tasks across all groups
- Can **write global memory** — update shared facts that all groups see
- Uses **persistent containers** — faster responses with real-time progress updates (you'll see "[working...]" messages while it thinks)

**Example conversation in main channel:**
```
You:    what groups do I have registered?
Andy:   You have 2 registered groups:
        • Main (self-chat) — main
        • Family Chat — family-chat

You:    add the "Work Team" group
Andy:   I found these WhatsApp groups:
        1. Work Team (last active: 2 hours ago)
        2. Work Team Old (last active: 3 months ago)
        Which one?

You:    1
Andy:   Done! "Work Team" is now registered. Messages starting with
        @Andy in that group will trigger me.
```

### Other Groups (WhatsApp Group Chats)

Every other registered chat — whether a group chat or a 1-on-1 conversation — is a regular group. These are isolated from each other and from the main channel.

**Key behaviors:**
- Only responds when the message **starts with `@Andy`** (or whatever trigger you configured)
- Has **its own memory** — separate `CLAUDE.md` that persists between conversations
- Has **its own files** — a dedicated directory for notes, data, etc.
- Can **only message its own chat** — can't send messages to other groups
- Can **only manage its own tasks** — can't see or control other groups' tasks
- Runs in **one-shot containers** — fresh container per interaction, returns the full response when done

**Example conversation in a group chat:**
```
Alice:  hey everyone, meeting at 3pm today
Bob:    sounds good
You:    @Andy summarize what we discussed in last week's meeting
Andy:   Based on the conversation history, last week you discussed:
        • Q2 roadmap priorities
        • Budget allocation for the new project
        • Hiring timeline for the engineering team

You:    @Andy remind us every Monday at 9am to share weekly updates
Andy:   Done! I'll send a reminder to this chat every Monday at 9am.
```

Messages that don't start with `@Andy` are ignored — Alice and Bob can chat normally without triggering the assistant.

---

## Adding Groups

Groups are WhatsApp chats you register with NanoClaw. Unregistered chats are completely ignored.

### Step 1: Make Sure NanoClaw Knows About the Group

NanoClaw discovers WhatsApp groups automatically — it syncs group metadata from WhatsApp on startup and once daily. For the group to appear, at least one message needs to have been sent in it recently.

### Step 2: Register the Group from Your Main Channel

Go to your main channel (self-chat) and tell the assistant to add it:

```
You:    add the "Family Chat" group
```

The assistant will:
1. Look up available WhatsApp groups
2. Show you matches
3. Ask you to confirm which one
4. Register it with a folder name (e.g., `family-chat`)
5. Create the group's directory under `groups/family-chat/`
6. Optionally create an initial `CLAUDE.md` with instructions for the assistant in that group

### Step 3: Start Chatting

Go to that WhatsApp group and type `@Andy hello`. You should get a response.

### Giving a Group Access to Directories

If you want the assistant in a specific group to access files on your Mac (e.g., a project folder), tell the main channel:

```
You:    give the "Work Team" group access to ~/projects/webapp
```

The assistant adds a `containerConfig` with `additionalMounts` to the group's registration. The directory will appear at `/workspace/extra/webapp` inside that group's container.

### Removing a Group

```
You:    remove the "Old Group" from registered groups
```

The assistant removes the registration. The group's folder and files remain (nothing is deleted).

---

## Scheduled Tasks

Any group can schedule tasks — recurring jobs that run the assistant on a timer.

### Creating Tasks

Just ask naturally:

```
@Andy every weekday at 9am, check Hacker News for AI news and send me a summary
@Andy every Monday at 8am, remind everyone to submit their weekly updates
@Andy in 2 hours, remind me to call the dentist
@Andy every 30 minutes, check if our website is responding
```

### Schedule Types

| Type | Example | How It Works |
|------|---------|-------------|
| **Cron** | "every weekday at 9am" | Standard cron schedule (e.g., `0 9 * * 1-5`) |
| **Interval** | "every 30 minutes" | Repeats at a fixed interval |
| **Once** | "in 2 hours" / "tomorrow at 3pm" | Runs once, then completes |

### Managing Tasks

```
@Andy list my scheduled tasks
@Andy pause the Monday reminder
@Andy resume the Monday reminder
@Andy cancel the website check task
```

From the main channel, you can view and manage tasks across all groups:

```
You:    list all tasks across all groups
You:    cancel task task-1706745600000-abc123
```

### How Tasks Run

When a task fires, the assistant runs in that group's context — same memory, same files, same permissions. If the task needs to tell you something, it sends a WhatsApp message to the group. Otherwise, it runs silently and logs the result.

---

## Memory

The assistant remembers things across conversations using a file-based memory system.

### How It Works

Each group has a `CLAUDE.md` file in its directory (e.g., `groups/family-chat/CLAUDE.md`). When the assistant starts a conversation, it automatically reads this file. When it learns something important, it writes to this file.

There are three levels:

| Level | File | Who Reads It | Who Writes It |
|-------|------|-------------|---------------|
| **Global** | `groups/global/CLAUDE.md` | All groups | Main only |
| **Group** | `groups/{name}/CLAUDE.md` | That group | That group |
| **Files** | `groups/{name}/*.md` | That group | That group |

### What to Remember Globally vs Per-Group

**Global memory** (tell the main channel "remember this globally"):
- Your name, preferences, timezone
- Facts that apply everywhere (e.g., "I work at Acme Corp")
- Communication style preferences

**Group memory** (automatically managed per group):
- Group-specific context (e.g., "this is the family group, members are...")
- Conversation history and key decisions
- Notes and structured data the assistant creates

### Asking the Assistant to Remember

```
@Andy remember that our team standup is at 10am EST
@Andy remember globally that I prefer concise responses
@Andy forget the thing about the old office address
```

The assistant updates its `CLAUDE.md` file accordingly.

---

## What the Assistant Can Do

Inside its container, the assistant has access to:

| Capability | Description |
|-----------|-------------|
| **Conversation** | Answer questions, discuss topics, help think through problems |
| **Web search** | Search the internet for current information |
| **Web browsing** | Fetch and read web pages |
| **Browser automation** | Navigate websites, fill forms, take screenshots (headless Chromium) |
| **File operations** | Read, write, edit files in its workspace |
| **Bash commands** | Run shell commands (safe — runs inside the container, not on your Mac) |
| **Scheduled tasks** | Create recurring or one-time jobs |
| **Send messages** | Reply to the chat or send proactive messages (for tasks) |

### Example Things to Ask

```
@Andy search for the best restaurants in downtown Austin
@Andy go to nytimes.com and summarize today's top stories
@Andy create a spreadsheet of my upcoming deadlines
@Andy write a bash script that converts all PNG files to JPG
@Andy schedule a daily weather forecast for 7am
@Andy what did we talk about last week?
```

---

## Security & Isolation

### What Groups Can See

Each group's assistant runs in a separate Linux container. It can **only** see:

- Its own `groups/{name}/` directory (read-write)
- The global memory directory `groups/global/` (read-only, non-main groups)
- Any extra directories you've explicitly mounted
- Its own IPC directory (for sending messages and managing tasks)

It **cannot** see:
- Other groups' files or memory
- Other groups' conversation history
- Your Mac's filesystem (beyond what's mounted)
- The NanoClaw codebase (except the main group)

### The Main Group Exception

The main group has elevated access because it's your admin channel. It can see the entire NanoClaw project directory, manage all groups, and control all tasks. This is by design — it's your control panel.

### Directory Access Control

A mount allowlist at `~/.config/nanoclaw/mount-allowlist.json` (outside the project, unreachable by containers) controls which Mac directories can be mounted. Sensitive paths like `.ssh`, `.aws`, `.env`, and credential files are always blocked regardless of the allowlist.

---

## Day-to-Day Usage

Once everything is set up, the typical workflow is:

1. **Message from your phone** — Open WhatsApp, go to the relevant chat, type `@Andy <your request>`
2. **Wait for a response** — The main channel shows "[working...]" progress. Other groups just reply when done (usually 10-30 seconds)
3. **Continue the conversation** — The assistant has context from recent messages in the chat, so you can refer back to what was discussed
4. **Admin from main channel** — Add groups, manage tasks, check on things from your self-chat

### Tips

- **No trigger needed in main** — Your self-chat responds to everything. Just type normally.
- **The assistant sees recent chat context** — When triggered, it reads all messages since its last response, so it knows what the group has been discussing.
- **Long tasks get acknowledged** — For requests that take time (research, multi-step work), the assistant sends a quick "I'll do X" message before diving in.
- **Conversations persist** — The assistant remembers previous conversations via its session and memory files. Say "what did we discuss yesterday?" and it can look it up.
- **Files persist too** — Anything the assistant creates in its workspace (notes, research, scripts) stays there across conversations.

---

## Troubleshooting

If something isn't working, open Claude Code in the nanoclaw directory and run `/debug`. It will diagnose the issue interactively.

### Common Issues

**No response to messages:**
- Is the service running? Check: `launchctl list | grep nanoclaw`
- Is the message in a registered group? Check from main: "list registered groups"
- Did you use the trigger word? Messages in non-main groups must start with `@Andy`
- Check logs: `tail -50 logs/nanoclaw.log`

**WhatsApp disconnected:**
- WhatsApp linked devices expire after ~20 days of inactivity
- Re-authenticate: `npm run auth` in the nanoclaw directory, scan the QR code
- Restart the service: `launchctl unload ~/Library/LaunchAgents/com.nanoclaw.plist && launchctl load ~/Library/LaunchAgents/com.nanoclaw.plist`

**Container failures:**
- Check container logs in `groups/{name}/logs/`
- Verify Apple Container is running: `container system status`
- If not: `container system start`
- Rebuild the image if needed: `./container/build.sh`

**Assistant doesn't remember things:**
- Memory is in `groups/{name}/CLAUDE.md` — check if it was written
- Sessions are per-group at `data/sessions/{name}/.claude/` — verify the directory exists
- If sessions seem lost, the assistant starts fresh but still has its CLAUDE.md memory

---

## Project Structure (Reference)

```
nanoclaw/
├── src/                          # Host-side code (runs on your Mac)
│   ├── index.ts                  # Main app: WhatsApp, routing, IPC
│   ├── config.ts                 # Settings (trigger, intervals, paths)
│   ├── container-runner.ts       # Spawns agent containers
│   ├── task-scheduler.ts         # Runs scheduled tasks
│   └── db.ts                     # SQLite operations
├── container/                    # Container-side code (runs in Linux VM)
│   ├── Dockerfile                # Container image definition
│   ├── build.sh                  # Build script
│   └── agent-runner/             # The agent brain (Claude SDK wrapper)
├── groups/                       # Per-group memory and files
│   ├── global/CLAUDE.md          # Shared memory (all groups read)
│   ├── main/CLAUDE.md            # Main channel memory + admin instructions
│   └── {other groups}/           # Each group's directory
├── store/                        # WhatsApp auth + SQLite DB (gitignored)
├── data/                         # Runtime state (gitignored)
│   ├── registered_groups.json    # Which chats are registered
│   ├── sessions.json             # Claude session IDs per group
│   └── ipc/                      # Container ↔ host communication
├── logs/                         # Service logs (gitignored)
└── .claude/skills/               # Claude Code skills (/setup, /debug, etc.)
```
