Model Context Protocol (MCP)
The Model Context Protocol (MCP) is an open protocol that standardizes how applications provide tools and context to LLMs. From the MCP docs:

MCP is an open protocol that standardizes how applications provide context to LLMs. Think of MCP like a USB-C port for AI applications. Just as USB-C provides a standardized way to connect your devices to various peripherals and accessories, MCP provides a standardized way to connect AI models to different data sources and tools.

There are three types of MCP servers this SDK supports:

Hosted MCP server tools – remote MCP servers used as tools by the OpenAI Responses API
Streamable HTTP MCP servers – local or remote servers that implement the Streamable HTTP transport
Stdio MCP servers – servers accessed via standard input/output (the simplest option)
Choose a server type based on your use‑case:

What you need	Recommended option
Call publicly accessible remote servers with default OpenAI responses models	1. Hosted MCP tools
Use publicly accessible remote servers but have the tool calls triggered locally	2. Streamable HTTP
Use locally running Streamable HTTP servers	2. Streamable HTTP
Use any Streamable HTTP servers with non-OpenAI-Responses models	2. Streamable HTTP
Work with local MCP servers that only support the standard-I/O protocol	3. Stdio
1. Hosted MCP server tools
Hosted tools push the entire round‑trip into the model. Instead of your code calling an MCP server, the OpenAI Responses API invokes the remote tool endpoint and streams the result back to the model.

Here is the simplest example of using hosted MCP tools. You can pass the remote MCP server’s label and URL to the hostedMcpTool utility function, which is helpful for creating hosted MCP server tools.

hostedAgent.ts
import { Agent, hostedMcpTool } from '@openai/agents';

export const agent = new Agent({
  name: 'MCP Assistant',
  instructions: 'You must always use the MCP tools to answer questions.',
  tools: [
    hostedMcpTool({
      serverLabel: 'gitmcp',
      serverUrl: 'https://gitmcp.io/openai/codex',
    }),
  ],
});

Then, you can run the Agent with the run function (or your own customized Runner instance’s run method):

Run with hosted MCP tools
import { run } from '@openai/agents';
import { agent } from './hostedAgent';

async function main() {
  const result = await run(
    agent,
    'Which language is the repo I pointed in the MCP tool settings written in?',
  );
  console.log(result.finalOutput);
}

main().catch(console.error);

To stream incremental MCP results, pass stream: true when you run the Agent:

Run with hosted MCP tools (streaming)
import { run } from '@openai/agents';
import { agent } from './hostedAgent';

async function main() {
  const result = await run(
    agent,
    'Which language is the repo I pointed in the MCP tool settings written in?',
    { stream: true },
  );

  for await (const event of result) {
    if (
      event.type === 'raw_model_stream_event' &&
      event.data.type === 'model' &&
      event.data.event.type !== 'response.mcp_call_arguments.delta' &&
      event.data.event.type !== 'response.output_text.delta'
    ) {
      console.log(`Got event of type ${JSON.stringify(event.data)}`);
    }
  }
  console.log(`Done streaming; final result: ${result.finalOutput}`);
}

main().catch(console.error);

Optional approval flow
For sensitive operations you can require human approval of individual tool calls. Pass either requireApproval: 'always' or a fine‑grained object mapping tool names to 'never'/'always'.

If you can programmatically determine whether a tool call is safe, you can use the onApproval callback to approve or reject the tool call. If you require human approval, you can use the same human-in-the-loop (HITL) approach using interruptions as for local function tools.

Human in the loop with hosted MCP tools
import { Agent, run, hostedMcpTool, RunToolApprovalItem } from '@openai/agents';

async function main(): Promise<void> {
  const agent = new Agent({
    name: 'MCP Assistant',
    instructions: 'You must always use the MCP tools to answer questions.',
    tools: [
      hostedMcpTool({
        serverLabel: 'gitmcp',
        serverUrl: 'https://gitmcp.io/openai/codex',
        // 'always' | 'never' | { never, always }
        requireApproval: {
          never: {
            toolNames: ['search_codex_code', 'fetch_codex_documentation'],
          },
          always: {
            toolNames: ['fetch_generic_url_content'],
          },
        },
      }),
    ],
  });

  let result = await run(agent, 'Which language is this repo written in?');
  while (result.interruptions && result.interruptions.length) {
    for (const interruption of result.interruptions) {
      // Human in the loop here
      const approval = await confirm(interruption);
      if (approval) {
        result.state.approve(interruption);
      } else {
        result.state.reject(interruption);
      }
    }
    result = await run(agent, result.state);
  }
  console.log(result.finalOutput);
}

import { stdin, stdout } from 'node:process';
import * as readline from 'node:readline/promises';

async function confirm(item: RunToolApprovalItem): Promise<boolean> {
  const rl = readline.createInterface({ input: stdin, output: stdout });
  const name = item.rawItem.name;
  const params = item.rawItem.providerData?.arguments;
  const answer = await rl.question(
    `Approve running tool (mcp: ${name}, params: ${params})? (y/n) `,
  );
  rl.close();
  return answer.toLowerCase().trim() === 'y';
}

main().catch(console.error);

Fully working samples (Hosted tools/Streamable HTTP/stdio + Streaming, HITL, onApproval) are examples/mcp in our GitHub repository.

2. Streamable HTTP MCP servers
When your Agent talks directly to a Streamable HTTP MCP server—local or remote—instantiate MCPServerStreamableHttp with the server url, name, and any optional settings:

Run with Streamable HTTP MCP servers
import { Agent, run, MCPServerStreamableHttp } from '@openai/agents';

async function main() {
  const mcpServer = new MCPServerStreamableHttp({
    url: 'https://gitmcp.io/openai/codex',
    name: 'GitMCP Documentation Server',
  });
  const agent = new Agent({
    name: 'GitMCP Assistant',
    instructions: 'Use the tools to respond to user requests.',
    mcpServers: [mcpServer],
  });

  try {
    await mcpServer.connect();
    const result = await run(agent, 'Which language is this repo written in?');
    console.log(result.finalOutput);
  } finally {
    await mcpServer.close();
  }
}

main().catch(console.error);

The constructor also accepts additional MCP TypeScript‑SDK options such as authProvider, requestInit, reconnectionOptions, and sessionId. See the MCP TypeScript SDK repository and its documents for details.

3. Stdio MCP servers
For servers that expose only standard I/O, instantiate MCPServerStdio with a fullCommand:

Run with Stdio MCP servers
import { Agent, run, MCPServerStdio } from '@openai/agents';
import * as path from 'node:path';

async function main() {
  const samplesDir = path.join(__dirname, 'sample_files');
  const mcpServer = new MCPServerStdio({
    name: 'Filesystem MCP Server, via npx',
    fullCommand: `npx -y @modelcontextprotocol/server-filesystem ${samplesDir}`,
  });
  await mcpServer.connect();
  try {
    const agent = new Agent({
      name: 'FS MCP Assistant',
      instructions:
        'Use the tools to read the filesystem and answer questions based on those files. If you are unable to find any files, you can say so instead of assuming they exist.',
      mcpServers: [mcpServer],
    });
    const result = await run(agent, 'Read the files and list them.');
    console.log(result.finalOutput);
  } finally {
    await mcpServer.close();
  }
}

main().catch(console.error);

Other things to know
For Streamable HTTP and Stdio servers, each time an Agent runs it may call list_tools() to discover available tools. Because that round‑trip can add latency—especially to remote servers—you can cache the results in memory by passing cacheToolsList: true to MCPServerStdio or MCPServerStreamableHttp.

Only enable this if you’re confident the tool list won’t change. To invalidate the cache later, call invalidateToolsCache() on the server instance.

Tool filtering
You can restrict which tools are exposed from each server by passing either a static filter via createMCPToolStaticFilter or a custom function. Here’s a combined example showing both approaches:

Tool filtering
import {
  MCPServerStdio,
  MCPServerStreamableHttp,
  createMCPToolStaticFilter,
  MCPToolFilterContext,
} from '@openai/agents';

interface ToolFilterContext {
  allowAll: boolean;
}

const server = new MCPServerStdio({
  fullCommand: 'my-server',
  toolFilter: createMCPToolStaticFilter({
    allowed: ['safe_tool'],
    blocked: ['danger_tool'],
  }),
});

const dynamicServer = new MCPServerStreamableHttp({
  url: 'http://localhost:3000',
  toolFilter: async ({ runContext }: MCPToolFilterContext, tool) =>
    (runContext.context as ToolFilterContext).allowAll || tool.name !== 'admin',
});

Further reading
Model Context Protocol – official specification.
examples/mcp – runnable demos referenced above.