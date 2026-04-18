# n8n Workflow Project

## Project Purpose

This repository is a workspace for building and managing n8n workflows using Claude. There is no application code here — the work product is n8n workflow definitions deployed directly to a live n8n instance.

## Available Tools

### n8n MCP Server (czlonkowski/n8n-mcp)
Two categories of tools are available:

**Documentation & Discovery (no API key needed):**
- `tools_documentation` — look up docs for any MCP tool; start here when unsure
- `search_nodes` — full-text search across 1,396+ nodes (filter by core/verified/community)
- `get_node` — detailed node info: properties, docs, versions, real examples
- `validate_node` — validate a node config (minimal = fast, full = comprehensive)
- `validate_workflow` — validate a complete workflow including AI Agent compatibility
- `search_templates` — search 2,709 templates by keyword, node type, task, or metadata
- `get_template` — retrieve full workflow JSON from a template

**n8n Instance Management (requires N8N_API_URL + N8N_API_KEY):**
- `n8n_create_workflow`, `n8n_get_workflow`, `n8n_update_full_workflow`, `n8n_update_partial_workflow`, `n8n_delete_workflow`, `n8n_list_workflows`
- `n8n_validate_workflow`, `n8n_autofix_workflow`, `n8n_workflow_versions`
- `n8n_deploy_template` — deploy a template directly to the instance
- `n8n_test_workflow` — trigger a test execution
- `n8n_executions` — list, get, or delete executions
- `n8n_manage_credentials` — manage credentials
- `n8n_health_check` — verify API connectivity
- `n8n_audit_instance` — security audit (50+ pattern detection)

Always prefer deploying workflows directly via MCP tools rather than generating JSON for the user to paste manually.

### n8n Skills (czlonkowski/n8n-skills)
Seven skills that activate automatically based on context — no need to invoke them manually:

| Skill | When it applies |
|-------|----------------|
| **n8n MCP Tools Expert** | Searching nodes, validating, managing workflows (highest priority) |
| **n8n Workflow Patterns** | Designing automation, choosing architecture |
| **n8n Expression Syntax** | Writing `{{ }}` expressions, accessing `$json`, `$node`, etc. |
| **n8n Node Configuration** | Configuring nodes, understanding property dependencies |
| **n8n Validation Expert** | Interpreting and fixing validation errors |
| **n8n Code JavaScript** | Writing Code nodes in JS |
| **n8n Code Python** | Writing Code nodes in Python (prefer JS — handles 95% of cases) |

## Workflow Building Guidelines

### Before Building
- Clarify the **trigger type** (webhook, schedule, manual, app event, etc.)
- Understand the **expected data shape** flowing through the workflow
- Confirm the **destination/output** (database write, API call, notification, etc.)
- Ask about **error handling** requirements upfront

### Node Standards
- Use `search_nodes` and `get_node` to find the right node before building — prefer built-in n8n nodes over HTTP Request when a native integration exists
- Check `search_templates` for similar existing workflows before building from scratch
- Use **descriptive node names** — never leave defaults like "HTTP Request1" or "Set2"
- Add **Sticky Notes** to explain non-obvious logic, branching decisions, or gotchas
- **Explicitly configure all parameters** — default parameter values are the #1 source of runtime failures
- For IF node routing, use `branch: 'true'` or `branch: 'false'` explicitly

### Credentials & Security
- Always use **named credential references** — never hardcode API keys, tokens, or passwords
- If a required credential doesn't exist yet, tell the user what to create before proceeding
- Never edit production workflows directly with AI — create a copy and test in development first

### Validation
- Run `validate_node` on complex node configs before deploying
- Run `validate_workflow` on the full workflow before activating
- Use `n8n_autofix_workflow` to resolve common errors automatically

### Error Handling
- Add explicit error handling for workflows that call external APIs or process user data
- Use the **Error Trigger** node for workflow-level error notifications
- For critical paths, use IF nodes to check for error fields and route accordingly

### Testing
- After deploying, use `n8n_test_workflow` to trigger a test execution
- For webhook-triggered workflows, provide the webhook URL and an example payload
- For scheduled workflows, suggest running manually once before activating

## Interaction Pattern

1. **User describes** a workflow need in natural language
2. **Claude asks** clarifying questions if trigger, data shape, or destination is unclear
3. **Claude searches** for relevant nodes and templates using MCP discovery tools
4. **Claude builds** the workflow using n8n instance management tools
5. **Claude validates** with `validate_workflow` or `n8n_validate_workflow` before activating
6. **Claude explains** what was built: the trigger, data flow, key nodes, and how to test it
7. **Claude helps debug** if test runs reveal issues
