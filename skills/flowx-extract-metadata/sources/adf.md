# Extract — Azure Data Factory

Source guide for `--source adf`. See the parent `SKILL.md` for how the phase runs and the MCP note.

## Step 1 — Ask the user how to export

Ask in plain language, and keep it short. Suggested wording:

> To migrate your Data Factory I first need a copy of its pipeline definitions — the JSON behind
> each pipeline, dataset, linked service and trigger. Nothing in Azure is changed. There are three
> ways to get it; which would you prefer?
>
> 1. **Use the factory's Git repository** — best if the factory is connected to Azure DevOps or GitHub.
> 2. **Download it from ADF Studio** — quickest if you can open the factory in your browser.
> 3. **Pull it through the Azure API** — no downloads or Git needed; you sign in with your Azure
>    account, which needs read access to the factory.
>
> Not sure? Tell me whether the factory is connected to Git and whether you can sign in to Azure,
> and I'll suggest one.

If the user isn't sure, ask at most these, in order, and recommend the first that fits:

1. *"When you open the factory in ADF Studio, is there a Git branch name at the top, and can you read
   that repository?"* → yes → **option 1**.
2. *"Can you open the factory in ADF Studio as a Data Factory Contributor (or ask someone who can)?"*
   → yes → **option 2**.
3. *"Do you have Reader access to the factory in the Azure portal?"* → yes → **option 3**.

Options 1 and 2 are the simplest: they need nothing beyond access the user usually already has.
Git is first because it needs no Azure role and gives versioned files, but only when the factory is
Git-connected — often only the development factory is. A production factory deployed by CI/CD
usually isn't; use option 2 or 3 against it if its values matter. Option 3 signs in as the user
(`az login` locally, or a device code in Databricks). Only if none of these work, or the user asks
for a sign-in with no interaction, use **"When the user's own sign-in doesn't work"** in Step 4 —
don't bring up service principals or service credentials before that.

When the user picks one, explain what it needs before starting, using the table below.

| | 1. Git repository | 2. ADF Studio download | 3. Azure API |
|---|---|---|---|
| **Permissions** | Read access to the repository. No Azure role. | **Data Factory Contributor** on the factory is enough (Microsoft doesn't document a smaller export-only role). | **Reader** on the factory, for the user. (There is no built-in "Data Factory Reader" role; Reader on the resource group also works but exposes more than needed.) |
| **You'll need** | The repo, its branches and the factory's *root folder* (ADF Studio > Manage > Source control > Git configuration). In Databricks, a Git credential for that provider. | A browser with ADF Studio | Tenant ID, subscription ID, resource group and factory name (ADF Studio > Manage > Properties); network access to `management.azure.com`. Device-code sign-in may be blocked by company policy (see Step 4). |
| **Gets you** | The latest commit on the collaboration branch, a release tag, or the published template — the user confirms which | The factory as it is now in ADF Studio | The factory exactly as it is now |

### Options 1 and 2 are done by the user

For the Git repository and the ADF Studio download, **you guide and the user acts**. Explain the
steps, answer questions, then wait for the user to come back with a path. Don't clone repositories,
create or switch Git folders, check out branches or tags, add Git credentials, open ADF Studio or
download files yourself — even if your tools would let you. Your part starts again at Step 5, which
only reads the path they give you.

## Step 2 — Export the pipelines

### Option 1: Use a Git repository

The usual setup is **one factory per repository**: the factory is connected to one repo, everyone
works in feature branches that merge into a single *collaboration branch*, and publishing writes to
one *publish branch*. Other branches are work in progress, not other factories.

Ask which version they want to migrate, and explain the difference in one line each:

- **Latest on the collaboration branch** (usually `main`) — the newest merged work. It can include
  changes that haven't been published or released yet.
- **A release tag** — if the team tags what it deploys (for example `release-1.4` or `v2026.10.1`),
  this is a known, reproducible release. Recommend it when the migration should match what runs in
  production.
- **What's published** — the publish branch (`adf_publish` unless changed in Git configuration),
  regenerated each time someone publishes in ADF Studio.

The collaboration branch and tags hold one JSON file per resource under the factory's root folder
(`pipeline/`, `dataset/`, `linkedService/`, `trigger/`, `factory/`). The publish branch holds a folder
named after the factory with `ARMTemplateForFactory.json` and `ARMTemplateParametersForFactory.json`.
Don't use a feature branch unless the user asks for it.

Always offer all three, even when you don't know whether the repo has tags. Suggested wording:

> Which version of the factory do you want to migrate?
> 1. **The latest commit on `<collaboration branch>`** — newest merged work, may not be released yet.
> 2. **A release tag** — what your team released (for example `release-1.4`; the repo's **Tags** or
>    **Releases** page lists them).
> 3. **The published version** (`adf_publish`) — what was last published from ADF Studio.

Then give the user the steps for where they work. Suggested instructions:

- **On their machine:**
  1. If they want a tag and aren't sure which: `git ls-remote --tags --sort=-v:refname <repo-url> | head`
  2. Copy that version: `git clone --depth 1 --branch <branch-or-tag> <repo-url> <folder>`
  3. Check what they got: `git -C <folder> log -1 --format='%h %ci %s'`
- **In Databricks (a Git folder):**
  1. Add a Git credential if they don't have one: **Settings > Linked accounts > Add Git credential**.
     For Azure DevOps choose **Azure DevOps (MSFT Entra)** (no token needed). For GitHub choose
     **Link Git account** (the Databricks GitHub App) or a personal access token with read access to
     the repo; if the GitHub App can't be installed on the repo's owner (common with
     enterprise-managed accounts and org policies), use a token.
  2. Create the Git folder: **Workspace > Create > Git folder**, with the repo URL.
  3. Pick the version. It opens on the default branch, normally the collaboration branch. For the
     published version, choose the publish branch in the branch selector at the top of the Git
     folder. For a tag, run `databricks repos update <repo-id> --tag <tag>` (the Git folder's ID is
     in its URL, or from `databricks repos list`); a tag that doesn't exist fails with "Remote ref
     does not exist". Warn them **not** to type a tag into the branch selector: that creates a new
     branch with the tag's name instead of checking out the tag.

**Ask the user to confirm the exact version** before you continue: the commit id, date and message
from step 3 on their machine, or the branch or tag and commit shown in the Git folder. Then ask for
the folder path and go to Step 5. The check finds the root folder, or the template in the publish
branch.

### Option 2: Download from ADF Studio

Give the user these steps:

1. Open the factory in **ADF Studio** → **Manage** (toolbox icon) → under **Source control**, **ARM
   template** → **Export ARM template**. The browser downloads `arm_template.zip`.
2. Unzip it. It holds `ARMTemplateForFactory.json`, `ARMTemplateParametersForFactory.json`,
   `linkedTemplates/` and a `factory/` folder.
3. Put it where flowx can read it. Keep `ARMTemplateForFactory.json` and
   `ARMTemplateParametersForFactory.json` together — without the parameters file, values such as
   trigger parameters stay unresolved. In Databricks, uploading just those two files to a
   `/Workspace` folder or a Unity Catalog volume is enough (Catalog > the volume > **Upload to this
   volume**); the rest of the zip isn't needed.
4. Come back with the path of the folder (or of `ARMTemplateForFactory.json`).

The Azure portal's generic **Export template** does **not** work for Data Factory; it must be ADF Studio.

When they're back, go to Step 5 with that path. The check points discover at
`ARMTemplateForFactory.json` — never the folder itself, which would load no pipelines.

### Option 3: Use the Azure REST API

Collect the four coordinates (tenant, subscription, resource group, factory name). The user signs in
with **their own Azure account**, so they need **Reader** on the factory and nothing else is set up:

- **Locally (Claude Code or another local agent):** `az login`, which opens a browser.
- **In Databricks (Genie Code or a notebook):** device-code sign-in. The notebook shows a code; the
  user enters it at `https://microsoft.com/devicelogin` and signs in as usual.

Before starting, add this short caveat:

> One thing to know: some organisations block the device-code sign-in (the one-time code) through
> their sign-in policies. If it's refused — for example with a message about Conditional Access or
> `AADSTS53003` — nothing is lost: we can switch to the Git repository or the ADF Studio download, or
> use an identity your Azure admins set up for this.

Then ask who should run it, and recommend based on where you are:

> I can run the extraction for you now, or give you a notebook to run in your own workspace.
> Which do you prefer?

| | When to recommend | Sign-in |
|---|---|---|
| **3a. I run it** | Claude Code or a local agent with `az` available | The user's `az login` |
| **3b. One MCP tool call** | — | **Not available yet.** Tell the user the flowx MCP tool can't run the extraction yet and offer 3a or 3c. |
| **3c. You run a notebook** | Genie Code; the user wants to run it in their own workspace; the agent can't reach Azure; a customer runs it for you | Device code (the notebook's default) |

#### Option 3a — The assistant runs the calls

1. Install flowx's `azure` extra into the plugin venv (see `SKILL.md`).
2. Ask the user to run `az login --tenant <tenant-id>` (add `--use-device-code` on a remote machine
   without a browser). In Claude Code, suggest they type `! az login --tenant <tenant-id>` so it runs
   in this session.
3. Run:

   ```bash
   "$PY" -m flowx.adapter extract --source adf \
     --tenant-id <tenant-id> --subscription-id <subscription-id> \
     --resource-group <resource-group> --factory-name <factory> \
     --output-dir <output_root>/adf_export/<factory>
   ```

   Add `--overwrite` only after confirming the user wants to replace an earlier export in that
   folder. The command lists each resource type once (paginated), retries throttled requests,
   writes the native layout plus `_manifest.json`, and checks the result the way discover reads
   it. Exit code 0 means the export is ready for discover — still show any warnings, which name
   optional resource types (data flows, integration runtimes, credentials) that couldn't be read;
   1 means it failed or isn't usable (the output says why); 2 means a sign-in, permission or setup
   problem, such as missing packages.

#### Option 3c — The user runs a notebook

1. Generate the notebook, pre-filled with what you know (never secrets):

   ```bash
   "$PY" -m flowx.adapter extract --source adf --emit-notebook <local_path>/extract_<factory>.py \
     --tenant-id <tenant-id> --subscription-id <subscription-id> \
     --resource-group <resource-group> --factory-name <factory> \
     [--output-dir /Workspace/Users/<user>/flowx_adf_export/<factory>]
   ```

2. Put it in the user's workspace:
   - From a local CLI: `databricks workspace import /Workspace/Users/<user>/extract_<factory> --file <local_path>/extract_<factory>.py --language PYTHON --format SOURCE --profile <profile>`
   - In Genie Code: upload it with the SDK so it opens as a notebook:
     `WorkspaceClient().workspace.upload("/Workspace/Users/<user>/extract_<factory>", open(path, "rb"), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)`
3. Tell the user what to do: open it, attach any compute (serverless works) and **Run all**. The
   widgets are already filled in and the `auth_mode` widget is `device_code`, so the
   **"4. Sign in and extract"** cell prints a code to enter at the Microsoft device-login page. Ask
   them to come back with the export path it prints.
4. When they're back, go to Step 5 with that path. If you run locally and the export is in their
   workspace, download it first: `databricks workspace export-dir <workspace_path> <local_dir> --profile <profile>`.

### When the user's sign-in doesn't work

Use this only if device code or `az login` is blocked, the user has no Reader access and can't get
it, or the user asks for a sign-in with no interaction (for example to run the extraction on a
schedule). Don't offer these up front: they need an admin's help.

First, if the user hasn't tried them, suggest the Git repository (option 1) or the ADF Studio
download (option 2): neither needs an Azure sign-in from flowx. Otherwise, two identities run with no
code to type. Both need someone with **Owner** or **User Access Administrator** on the factory to give
the identity **Reader** on the factory itself (not the whole resource group, which would expose its
other resources):

**A service principal from a secret scope** — if the team already has one, or an Entra admin can
create one (app registration and a client secret, which expires and must be rotated). Store it in a
secret scope; never ask the user to paste the secret into chat:

```bash
databricks secrets create-scope flowx-extract
databricks secrets put-secret flowx-extract azure-client-id     --string-value "<appId>"
databricks secrets put-secret flowx-extract azure-client-secret --string-value "<secret>"
```

- Notebook (3c): set `auth_mode` to `service_principal` and fill in `secret_scope` (and
  `client_id_key` / `client_secret_key` if the keys aren't named `azure-client-id` /
  `azure-client-secret`).
- Locally (3a): export `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` before running
  the command; it uses them instead of `az login`.

**A Unity Catalog service credential** (Databricks only) — uses an Azure Databricks access
connector's managed identity, so there is no code and no secret. Setup, once:

1. **Azure:** give an access connector's managed identity **Reader** on the factory. Prefer a
   connector dedicated to this over the one behind the metastore's storage credential, so the
   access isn't tied to an identity the platform team treats as storage-only. Creating a connector
   needs Contributor on a resource group.
2. **Unity Catalog (`CREATE SERVICE CREDENTIAL` on the metastore, usually a metastore admin):**
   **Catalog > External data > Credentials > Create credential > Service credential**, with the
   access connector's resource ID.
3. **Grant use:** `GRANT ACCESS ON SERVICE CREDENTIAL <name> TO <user or group>`; check it is bound
   to the workspace if workspace bindings are used.

Then:

- Notebook (3c, the simplest): add `--service-credential <name>` when generating it. The notebook
  then signs in with the credential by default, so **Run all** — or a scheduled job — needs no
  interaction.
- Genie Code running it itself (3a): run the extraction **in a notebook cell, in-process** — a
  service credential isn't available to a separate `python -m` process. It needs `azure-identity`,
  `requests`, `pyyaml` and `sqlglot` installed with `%pip`, and Python 3.12 or later (serverless
  environment version 3 or later):

  ```python
  import sys
  sys.path.insert(0, "<plugin_dir>/src")
  from flowx.sources.adf.extract.runner import main
  main(["--subscription-id", "<subscription-id>", "--resource-group", "<resource-group>",
        "--factory-name", "<factory>", "--output-dir", "<output_root>/adf_export/<factory>",
        "--auth", "service-credential", "--service-credential", "<credential-name>"])
  ```

## Step 3 — Check the result (all options)

```bash
"$PY" -m flowx.adapter extract --source adf --validate-path <path> --json
```

It reports `kind` (`native`, `arm_template`, `arm_export_folder`), the exact `discover_path`, the
counts discover will see, and warnings. Always use `discover_path`, not the path the user gave you.
Act on the warnings:

- **`arm_export_folder`** — expected for option 2 and the publish branch; use the template file it names.
- **`multiple_factories`** — uncommon: more than one factory shares this folder (for example several
  factories connected to the same repo with different root folders). Ask which one and check the path
  it lists for that factory.
- **No pipelines found / not usable** — wrong folder or branch; go back to the option's steps.
- **Incomplete extraction / count mismatch** — say which resource types are missing and why
  (usually a missing role); offer to re-run after it's fixed.

## Step 4 — Hand off to discover

Show a short summary — factory name, pipelines, datasets, linked services, triggers — and the
`discover_path`. Then continue with `flowx-discover` (or the next step of `flowx-migrate`), passing
`discover_path` as `--source-path`. On the MCP path, pass a `/Workspace/...` export as
`adf_workspace_path` and a `/Volumes/...` export as `adf_volume_path`.
