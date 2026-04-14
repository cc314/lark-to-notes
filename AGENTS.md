# Vault Maintenance Contract

This vault is maintained as a wiki for both human reading and agent operation.

## Canonical Locations

- Put new source material in `raw/` first.
- Use `daily/YYYY-MM-DD.md` for daily tasks, follow-ups, and short-lived context.
- Use `meetings/YYYY-MM-DD - subject.md` for actual meetings and only for discussion threads that deserve a standalone record.
- Use `people/<Name>/index.md` for stakeholder pages.
- Use `people/index.md` as the curated directory for navigating a larger people graph.
- Use `area/current tasks/index.md` as the durable open-work list for active work, project items, and follow-ups promoted out of daily notes.
- Use `projects/<Project>/index.md` for durable project pages.
- Use `area/<Topic>/index.md` for durable topic pages.

## Note Creation Rules

- Prefer updating an existing note over creating a near-duplicate.
- Keep one canonical note per entity or event and link to it from related notes.
- Keep org structure on people notes by default using fields such as `departments`, `reports_to`, and `reports`; do not create a separate org/entity hierarchy unless the org unit needs durable non-person context.
- Do not create a project note unless the source clearly refers to a real project.
- Do not create a meeting note for a simple chat follow-up that fits cleanly in the daily note.
- For assets such as screenshots, PDFs, sheets, or documents, add a nearby `.md` context note in `raw/`.
- Preserve user-authored content unless the task explicitly requires restructuring it.

## Metadata Rules

- All curated notes should have `type`, `created`, `updated`, and `tags`.
- Daily and meeting notes should also carry `date`, `people`, `projects`, and `areas`.
- Raw notes should also carry `source`, `author`, and `published`.
- People notes should also carry `aliases` and `role`.
- Project notes should also carry `status` and `stakeholders`.
- Use Obsidian wikilinks inside both body content and frontmatter list properties when linking related notes.

## Maintenance Workflow

1. Capture or summarize new information in `raw/`, `daily/`, or `meetings/`.
2. Distill actionable items from messages and source material into the relevant daily note first.
3. Promote still-open work, project items, and follow-ups into `area/current tasks/index.md`.
4. Roll durable context into the relevant `people/`, `projects/`, and `area/` pages when it is more than a one-off daily task.
5. Add backlinks across the source note and the durable notes.
6. Update `updated` on substantive edits.
7. Keep `people/index.md` current as the main directory for person notes once the people graph grows beyond quick scanning in the root index.
8. Keep `index.md` current enough to serve as the vault entry point, but prefer a selective set of high-value links rather than dumping every person note there.
