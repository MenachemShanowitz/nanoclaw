/**
 * Persistent Container Manager for NanoClaw
 * Maintains long-running containers with stdin/stdout pipes for real-time interaction
 */

import { spawn, ChildProcess } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import readline from 'readline';
import pino from 'pino';
import {
  CONTAINER_IMAGE,
  CONTAINER_TIMEOUT,
  GROUPS_DIR,
  DATA_DIR
} from './config.js';
import { RegisteredGroup } from './types.js';
import { validateAdditionalMounts } from './mount-security.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

// Message types for streaming protocol
export interface StreamMessage {
  type: 'progress' | 'tool' | 'result' | 'error' | 'pong';
  text?: string;
  toolName?: string;
  toolInput?: string;
  status?: 'success' | 'error';
  result?: string | null;
  newSessionId?: string;
  error?: string;
}

export interface ContainerMessage {
  type: 'message' | 'ping' | 'shutdown';
  prompt?: string;
  sessionId?: string;
  groupFolder?: string;
  chatJid?: string;
  isMain?: boolean;
  isScheduledTask?: boolean;
}

export type ProgressCallback = (message: StreamMessage) => void;

interface VolumeMount {
  hostPath: string;
  containerPath: string;
  readonly?: boolean;
}

function getHomeDir(): string {
  const home = process.env.HOME || os.homedir();
  if (!home) {
    throw new Error('Unable to determine home directory');
  }
  return home;
}

function buildVolumeMounts(group: RegisteredGroup, isMain: boolean): VolumeMount[] {
  const mounts: VolumeMount[] = [];
  const projectRoot = process.cwd();

  if (isMain) {
    mounts.push({
      hostPath: projectRoot,
      containerPath: '/workspace/project',
      readonly: false
    });
    mounts.push({
      hostPath: path.join(GROUPS_DIR, group.folder),
      containerPath: '/workspace/group',
      readonly: false
    });
  } else {
    mounts.push({
      hostPath: path.join(GROUPS_DIR, group.folder),
      containerPath: '/workspace/group',
      readonly: false
    });
    const globalDir = path.join(GROUPS_DIR, 'global');
    if (fs.existsSync(globalDir)) {
      mounts.push({
        hostPath: globalDir,
        containerPath: '/workspace/global',
        readonly: true
      });
    }
  }

  const groupSessionsDir = path.join(DATA_DIR, 'sessions', group.folder, '.claude');
  fs.mkdirSync(groupSessionsDir, { recursive: true });
  mounts.push({
    hostPath: groupSessionsDir,
    containerPath: '/home/node/.claude',
    readonly: false
  });

  const groupIpcDir = path.join(DATA_DIR, 'ipc', group.folder);
  fs.mkdirSync(path.join(groupIpcDir, 'messages'), { recursive: true });
  fs.mkdirSync(path.join(groupIpcDir, 'tasks'), { recursive: true });
  mounts.push({
    hostPath: groupIpcDir,
    containerPath: '/workspace/ipc',
    readonly: false
  });

  const envDir = path.join(DATA_DIR, 'env');
  fs.mkdirSync(envDir, { recursive: true });
  const envFile = path.join(projectRoot, '.env');
  if (fs.existsSync(envFile)) {
    const envContent = fs.readFileSync(envFile, 'utf-8');
    const allowedVars = ['CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_API_KEY'];
    const filteredLines = envContent
      .split('\n')
      .filter(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return false;
        return allowedVars.some(v => trimmed.startsWith(`${v}=`));
      });

    if (filteredLines.length > 0) {
      fs.writeFileSync(path.join(envDir, 'env'), filteredLines.join('\n') + '\n');
      mounts.push({
        hostPath: envDir,
        containerPath: '/workspace/env-dir',
        readonly: true
      });
    }
  }

  if (group.containerConfig?.additionalMounts) {
    const validatedMounts = validateAdditionalMounts(
      group.containerConfig.additionalMounts,
      group.name,
      isMain
    );
    mounts.push(...validatedMounts);
  }

  return mounts;
}

function buildContainerArgs(mounts: VolumeMount[]): string[] {
  // Note: no --rm flag so container can be reused
  const args: string[] = ['run', '-i'];

  for (const mount of mounts) {
    if (mount.readonly) {
      args.push('--mount', `type=bind,source=${mount.hostPath},target=${mount.containerPath},readonly`);
    } else {
      args.push('-v', `${mount.hostPath}:${mount.containerPath}`);
    }
  }

  // Add persistent mode environment variable
  args.push('-e', 'NANOCLAW_PERSISTENT=1');
  args.push(CONTAINER_IMAGE);

  return args;
}

interface PendingRequest {
  resolve: (result: StreamMessage) => void;
  reject: (error: Error) => void;
  onProgress: ProgressCallback;
  timeout: NodeJS.Timeout;
}

export class PersistentContainer {
  private process: ChildProcess | null = null;
  private rl: readline.Interface | null = null;
  private group: RegisteredGroup;
  private isMain: boolean;
  private pendingRequest: PendingRequest | null = null;
  private isReady = false;
  private restartAttempts = 0;
  private maxRestartAttempts = 3;

  constructor(group: RegisteredGroup, isMain: boolean) {
    this.group = group;
    this.isMain = isMain;
  }

  async start(): Promise<void> {
    if (this.process && !this.process.killed) {
      logger.debug({ group: this.group.name }, 'Container already running');
      return;
    }

    const groupDir = path.join(GROUPS_DIR, this.group.folder);
    fs.mkdirSync(groupDir, { recursive: true });

    const mounts = buildVolumeMounts(this.group, this.isMain);
    const containerArgs = buildContainerArgs(mounts);

    logger.info({
      group: this.group.name,
      isMain: this.isMain,
      mountCount: mounts.length
    }, 'Starting persistent container');

    this.process = spawn('container', containerArgs, {
      stdio: ['pipe', 'pipe', 'pipe']
    });

    // Set up readline for line-by-line parsing
    this.rl = readline.createInterface({
      input: this.process.stdout!,
      crlfDelay: Infinity
    });

    this.rl.on('line', (line) => {
      this.handleLine(line);
    });

    this.process.stderr?.on('data', (data) => {
      const lines = data.toString().trim().split('\n');
      for (const line of lines) {
        if (line) logger.debug({ container: this.group.folder }, line);
      }
    });

    this.process.on('close', (code) => {
      logger.warn({ group: this.group.name, code }, 'Persistent container exited');
      this.isReady = false;
      this.cleanup();

      // Reject any pending request
      if (this.pendingRequest) {
        clearTimeout(this.pendingRequest.timeout);
        this.pendingRequest.reject(new Error(`Container exited with code ${code}`));
        this.pendingRequest = null;
      }

      // Auto-restart if not too many attempts
      if (this.restartAttempts < this.maxRestartAttempts) {
        this.restartAttempts++;
        logger.info({ group: this.group.name, attempt: this.restartAttempts }, 'Restarting container');
        setTimeout(() => this.start(), 1000 * this.restartAttempts);
      }
    });

    this.process.on('error', (err) => {
      logger.error({ group: this.group.name, error: err }, 'Container process error');
      this.cleanup();
    });

    // Wait for container to be ready
    await this.waitForReady();
  }

  private async waitForReady(): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Container startup timeout'));
      }, 30000);

      const checkReady = () => {
        if (this.isReady) {
          clearTimeout(timeout);
          resolve();
        } else {
          // Send a ping to check if ready
          this.sendRaw({ type: 'ping' });
          setTimeout(checkReady, 500);
        }
      };

      // Wait a bit for process to start, then begin checking
      setTimeout(checkReady, 1000);
    });
  }

  private handleLine(line: string): void {
    if (!line.trim()) return;

    try {
      const msg: StreamMessage = JSON.parse(line);

      if (msg.type === 'pong') {
        this.isReady = true;
        this.restartAttempts = 0; // Reset on successful ping
        return;
      }

      if (!this.pendingRequest) {
        logger.warn({ group: this.group.name, msg }, 'Received message with no pending request');
        return;
      }

      if (msg.type === 'progress' || msg.type === 'tool') {
        // Stream to callback
        this.pendingRequest.onProgress(msg);
      } else if (msg.type === 'result' || msg.type === 'error') {
        // Final result
        clearTimeout(this.pendingRequest.timeout);
        this.pendingRequest.resolve(msg);
        this.pendingRequest = null;
      }
    } catch (err) {
      logger.debug({ group: this.group.name, line }, 'Non-JSON output from container');
    }
  }

  private sendRaw(msg: ContainerMessage): void {
    if (!this.process?.stdin?.writable) {
      throw new Error('Container stdin not writable');
    }
    this.process.stdin.write(JSON.stringify(msg) + '\n');
  }

  private cleanup(): void {
    this.rl?.close();
    this.rl = null;
    this.process = null;
  }

  async sendMessage(
    prompt: string,
    sessionId: string | undefined,
    chatJid: string,
    isScheduledTask: boolean,
    onProgress: ProgressCallback,
    timeout = CONTAINER_TIMEOUT
  ): Promise<StreamMessage> {
    if (!this.process || !this.isReady) {
      await this.start();
    }

    if (this.pendingRequest) {
      throw new Error('Container is busy with another request');
    }

    return new Promise((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequest) {
          this.pendingRequest = null;
          reject(new Error(`Request timed out after ${timeout}ms`));
        }
      }, timeout);

      this.pendingRequest = {
        resolve,
        reject,
        onProgress,
        timeout: timeoutHandle
      };

      const msg: ContainerMessage = {
        type: 'message',
        prompt,
        sessionId,
        groupFolder: this.group.folder,
        chatJid,
        isMain: this.isMain,
        isScheduledTask
      };

      try {
        this.sendRaw(msg);
      } catch (err) {
        clearTimeout(timeoutHandle);
        this.pendingRequest = null;
        reject(err);
      }
    });
  }

  async shutdown(): Promise<void> {
    if (!this.process) return;

    try {
      this.sendRaw({ type: 'shutdown' });
      // Give it a moment to clean up
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch {
      // Ignore errors during shutdown
    }

    if (this.process && !this.process.killed) {
      this.process.kill('SIGTERM');
    }

    this.cleanup();
  }

  isRunning(): boolean {
    return this.process !== null && !this.process.killed && this.isReady;
  }
}

/**
 * Manages persistent containers for groups
 */
export class PersistentContainerManager {
  private containers = new Map<string, PersistentContainer>();
  private registeredGroups: () => Record<string, RegisteredGroup>;

  constructor(getRegisteredGroups: () => Record<string, RegisteredGroup>) {
    this.registeredGroups = getRegisteredGroups;
  }

  async getContainer(groupFolder: string): Promise<PersistentContainer | null> {
    // Find the group by folder
    const groups = this.registeredGroups();
    const entry = Object.entries(groups).find(([, g]) => g.folder === groupFolder);
    if (!entry) {
      logger.warn({ groupFolder }, 'Group not found');
      return null;
    }

    const [, group] = entry;
    const isMain = groupFolder === 'main';

    let container = this.containers.get(groupFolder);
    if (!container) {
      container = new PersistentContainer(group, isMain);
      this.containers.set(groupFolder, container);
    }

    if (!container.isRunning()) {
      await container.start();
    }

    return container;
  }

  async runMessage(
    groupFolder: string,
    prompt: string,
    sessionId: string | undefined,
    chatJid: string,
    isScheduledTask: boolean,
    onProgress: ProgressCallback,
    timeout?: number
  ): Promise<StreamMessage> {
    const container = await this.getContainer(groupFolder);
    if (!container) {
      return {
        type: 'error',
        error: 'Container not available'
      };
    }

    return container.sendMessage(prompt, sessionId, chatJid, isScheduledTask, onProgress, timeout);
  }

  async shutdownAll(): Promise<void> {
    for (const [folder, container] of this.containers) {
      logger.info({ folder }, 'Shutting down container');
      await container.shutdown();
    }
    this.containers.clear();
  }
}
