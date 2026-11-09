# Animation: an Iceberg commit is one atomic swap of the table's metadata pointer in the catalog

**Claim.** An Iceberg commit is one atomic swap of the table's metadata pointer in the catalog, which is why time travel, rollback and write-audit-publish are cheap. Literal version on screen: the atomic step is the catalog (Unity Catalog's Iceberg REST endpoint) swapping `metadata-location` to a new `metadata.json`; `current-snapshot-id` is a field inside that file, not a pointer of its own. For the course textbook (Chapter 5, tables and catalog).

**Numbers.** `data.json`. Every behaviour shown was verified on the course stack (Spark 4.3.0, Iceberg built from apache/iceberg#18749, Unity Catalog 0.7.0) on 2026-11-06. On screen the WAP beat says only "main is unchanged" and "+1 row" (the check had main at 5 rows, branch at 6; absolute counts are not shown because the earlier snapshots are illustrative). Metadata versions, snapshot labels, file counts, the branch name `audit` and all timing are schematic, labelled on screen.

**Backend:** hf (boxes, arrows and text; no morphs). **Themes:** paper, ide. **Formats:** portrait, square, landscape. **Camera cut:** yes.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, catalog, v1.json, S1 with its manifest and data files, all on screen at frame 0 | the catalog holds one pointer: metadata-location | The catalog holds one pointer: the table's current metadata file. | the tree and the one pointer have to read before anything moves |
| `writes` | 4.5 | new data files, a manifest, manifest list S2 and v2.json appear; the catalog arrow stays on v1 | new files are written; readers still see v1 | An INSERT adds data files, a manifest, a manifest list and metadata v2. | shows the work done before the commit, invisible to readers |
| `swap` | 10.0 | the catalog's metadata-location flips v1 to v2; arrow, current card and current snapshot move together | the catalog swaps metadata-location: v1 → v2 | Then the catalog swaps metadata-location to v2 in one step. That's the commit. | the claim itself: the commit is this one swap |
| `delete` | 15.5 | manifest list S3 and v3.json written, then one swap | DELETE commits S3 the same way | A DELETE commits snapshot three the same way. | builds the history rollback needs |
| `rollback` | 19.0 | only v4.json is written (current-snapshot-id S2); swap; S3 and its files stay | no data rewritten: v4's current-snapshot-id is S2 again | Rollback rewrites no data. Metadata v4 makes snapshot two current again. | rollback is cheap because it is metadata only |
| `wap` | 24.0 | S4 on branch audit (green) with its files; v5.json names main S2 and audit S4; swap; main's arrow stays on S2 | the write lands on branch audit; main is unchanged | With write-audit-publish on, the next write lands on a branch. Main doesn't move. | WAP is a branch ref in the same metadata, not a copy |
| `audit` | 30.0 | S4 pulses | auditor reads branch audit: +1 row | An auditor reads the branch by name. | the audit step reads the unpublished data |
| `publish` | 33.5 | v6.json (current S4) written; swap; main's current snapshot moves to S4 | fast_forward moves main to S4: another swap (stays) | Fast-forward moves main to the branch's snapshot. One more pointer swap. | publishing is the same cheap swap |

## What has to be readable

The catalog's `metadata-location → vN.metadata.json` line at every swap; the `current-snapshot-id` inside each card; the SQL line of each beat (longest: the `fast_forward` and `rollback_to_snapshot` calls); "main is unchanged" and "+1 row"; the footnote.
