# Vault Maintenance Contract

This vault is maintained as a wiki for both human reading and agent operation.

## Canonical Locations

- Put new source material in `raw/` first.
- Use `daily/YYYY-MM-DD.md` for daily tasks, follow-ups, and short-lived context.
- Use `meetings/YYYY-MM-DD - subject.md` for meetings and important stakeholder chat threads.
- Use `people/<Name>/index.md` for stakeholder pages.
- Use `projects/<Project>/index.md` for durable project pages.
- Use `area/<Topic>/index.md` for durable topic pages.

## Note Creation Rules

- Prefer updating an existing note over creating a near-duplicate.
- Keep one canonical note per entity or event and link to it from related notes.
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
2. Roll durable context into the relevant `people/`, `projects/`, and `area/` pages.
3. Add backlinks across the source note and the durable notes.
4. Update `updated` on substantive edits.
5. Keep `index.md` current enough to serve as the vault entry point.