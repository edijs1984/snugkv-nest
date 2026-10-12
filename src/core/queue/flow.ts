/* eslint-disable @typescript-eslint/no-explicit-any */
import { withTrace } from '../trace';
import { SnugKvClient, SnugKvClientOptions } from '../client';
import { Arg, RespError } from '../resp';
import { uuid } from '../util';
import type { Job } from './job';
import { localJob, Queue } from './queue';
import { JobsOptions, jobArgs } from './types';

export interface FlowJob<D = any> {
  name: string;
  queueName: string;
  data?: D;
  opts?: JobsOptions;
  /** Jobs that must finish before this one runs. */
  children?: FlowJob[];
}

export interface JobNode {
  job: Job;
  children?: JobNode[];
}

export interface FlowProducerOptions {
  client?: SnugKvClient;
  connection?: SnugKvClientOptions;
  prefix?: string;
}

/**
 * Adds a tree of jobs in one atomic step. A parent waits (state `waiting-children`) until all its
 * children have finished, then runs; `job.getChildrenValues()` returns what the children returned.
 */
export class FlowProducer {
  private readonly client: SnugKvClient;
  private readonly owns: boolean;
  private readonly prefix: string;
  private readonly queues = new Map<string, Queue>();

  constructor(options: FlowProducerOptions = {}) {
    this.owns = !options.client;
    this.client = options.client ?? new SnugKvClient(options.connection ?? {});
    this.prefix = options.prefix ?? 'snugq';
  }

  private queue(name: string): Queue {
    let q = this.queues.get(name);
    if (!q) {
      q = new Queue(name, { client: this.client, prefix: this.prefix });
      this.queues.set(name, q);
    }
    return q;
  }

  async add(flow: FlowJob): Promise<JobNode> {
    return (await this.addBulk([flow]))[0]!;
  }

  async addBulk(flows: FlowJob[]): Promise<JobNode[]> {
    if (!this.client.isReady) await this.client.connect(10_000);
    const commands: Arg[][] = [];
    const nodes: JobNode[] = [];
    const walk = (f: FlowJob, parent?: { base: string; id: string }): JobNode => {
      const q = this.queue(f.queueName);
      const id = f.opts?.jobId ?? uuid();
      const opts: JobsOptions = { ...f.opts, jobId: id, parent: undefined };
      commands.push(['FCALL', 'snugq_add', 1, q.base, ...jobArgs(f.name, withTrace(f.data ?? null), opts, { parentBase: parent?.base, parentId: parent?.id, waitChildren: (f.children?.length ?? 0) > 0 })]);
      const node: JobNode = { job: localJob(q, id, f.name, f.data ?? null, opts, parent) };
      if (f.children?.length) node.children = f.children.map((c) => walk(c, { base: q.base, id }));
      return node;
    };
    for (const f of flows) nodes.push(walk(f));
    const replies = await this.client.execBlock(commands);
    const bad = replies.find((r) => r instanceof RespError);
    if (bad) throw bad;
    return nodes;
  }

  async close(): Promise<void> {
    if (this.owns) this.client.close();
  }
}
