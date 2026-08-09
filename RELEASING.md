# Releasing Spring Cloud AWS

This document describes the release process for Spring Cloud AWS (awspring/spring-cloud-aws),
reverse-engineered from the release history from 3.0.0 through 4.1.0. There is no
maven-release-plugin, no CI-driven version bumping, and no automated release-notes generation —
the process is a small set of manual PRs plus a tag-triggered GitHub Actions workflow.

## TL;DR — release sequence

For a minor/major release from `main` (patch releases follow the same sequence on a `X.Y.x` maintenance branch):

1. **Align dependencies** — one or more "Lift/Upgrade" PRs bumping AWS SDK, Spring Boot/Cloud/Framework, and friends.
2. **Prepare release PR** — set the version in *every* module pom from `X.Y.Z-SNAPSHOT` to `X.Y.Z`.
3. **Check `release.yml` tag patterns** — the workflow only triggers on hardcoded `vX.Y.*` patterns; new minors must be added.
4. **Push tag `vX.Y.Z`** — triggers the automated Release workflow: full test build, deploy to Maven Central, docs upload to S3.
5. **Publish the GitHub Release** — hand-written notes (docs link, highlights, per-module changes); close the milestone.
6. **Back to snapshot PR** — set poms to the next `-SNAPSHOT` version.
7. **Update README** — compatibility matrix and version mentions (separate PR).
8. **(New minor line only)** cut the `X.Y.x` maintenance branch, add it to CI/docs workflows, bump `main` to the next minor snapshot.

---

## How versions and dependencies are managed

### Project version

The project version is **hardcoded in ~50 `pom.xml` files** (root, every module, samples, starters, docs).
Version-bump PRs touch all of them with a one-line change each (see
[#1648](https://github.com/awspring/spring-cloud-aws/pull/1648) for a typical diff).
`./mvnw versions:set -DnewVersion=... -DprocessAllModules` produces this shape of change.

### Dependency versions

- Managed as properties in [`spring-cloud-aws-dependencies/pom.xml`](spring-cloud-aws-dependencies/pom.xml)
  (`awssdk-v2.version`, `spring-cloud-commons.version`, `spring-cloud-stream.version`, `spring-modulith.version`,
  KCL/KPL/DAX, S3 encryption client, jakarta-mail, etc.) and the root pom (Spring Cloud Build parent version,
  which pins the Spring Boot/Framework generation).
- **Dependabot only covers GitHub Actions** ([.github/dependabot.yml](.github/dependabot.yml)) — all library
  upgrades are manual PRs.
- **When**: dependency alignment happens as dedicated PRs in the days/weeks *before* the release
  ("Lift aws SDK version" [#1647](https://github.com/awspring/spring-cloud-aws/pull/1647),
  "Lift spring versions" [#1649](https://github.com/awspring/spring-cloud-aws/pull/1649)), not inside the
  prepare-release PR. The prepare-release PR is version-only.
- Major/milestone releases get a bigger "release upgrades" PR that also adapts code to the new Spring generation
  ("Spring Cloud AWS 4.0.0-M1 release upgrades" [#1521](https://github.com/awspring/spring-cloud-aws/pull/1521)).
- Releases track the Spring Boot / Spring Cloud release train — compatibility requests are often the trigger for
  a release cycle ([#1400](https://github.com/awspring/spring-cloud-aws/issues/1400) → 3.4.0,
  [#1412](https://github.com/awspring/spring-cloud-aws/issues/1412) / [#1534](https://github.com/awspring/spring-cloud-aws/issues/1534) → 4.0.0).

---

## The steps in detail

### Step 1 — Align dependencies

Bump AWS SDK, Spring Cloud/Boot, and other managed dependencies to the versions the release should ship with.
Test-only deps (LocalStack, Testcontainers, WireMock) are often bumped in the same sweep.

Examples:
- 4.1.0: [#1647](https://github.com/awspring/spring-cloud-aws/pull/1647) (AWS SDK, from issue [#1646](https://github.com/awspring/spring-cloud-aws/issues/1646)), [#1649](https://github.com/awspring/spring-cloud-aws/pull/1649) (Spring versions)
- 4.0.0-RC1: [#1547](https://github.com/awspring/spring-cloud-aws/pull/1547), [#1551](https://github.com/awspring/spring-cloud-aws/pull/1551)
- 3.3.0: [#1317](https://github.com/awspring/spring-cloud-aws/pull/1317), [#1320](https://github.com/awspring/spring-cloud-aws/pull/1320)
- 3.2.0: [#1206](https://github.com/awspring/spring-cloud-aws/pull/1206), [#1207](https://github.com/awspring/spring-cloud-aws/pull/1207)

### Step 2 — Prepare release PR (version bump to GA)

A PR titled "Prepare release X.Y.Z" / "Set version to X.Y.Z" changing only the `<version>` element in every pom:
`X.Y.Z-SNAPSHOT` → `X.Y.Z`. Merged to `main` (or the maintenance branch for patches).

Examples: [#1648](https://github.com/awspring/spring-cloud-aws/pull/1648) (4.1.0),
[#1563](https://github.com/awspring/spring-cloud-aws/pull/1563) (4.0.0),
[#1324](https://github.com/awspring/spring-cloud-aws/pull/1324) (3.3.0),
[#1403](https://github.com/awspring/spring-cloud-aws/pull/1403) (3.4.0),
[#1589](https://github.com/awspring/spring-cloud-aws/pull/1589) (4.0.1, on `4.0.x`).

### Step 3 — Make sure `release.yml` will trigger

[.github/workflows/release.yml](.github/workflows/release.yml) triggers on an **explicit allowlist of tag
patterns** (`v3.4.*`, `v4.0.*`, `v4.1.*`, ...). Every new minor line requires adding its pattern — this has been
forgotten and patched at the last minute more than once:

- [#1655](https://github.com/awspring/spring-cloud-aws/pull/1655) — added `v4.1.*` the day before the 4.1.0 tag
  (the v4.1.0 tag actually points at this commit)
- [`2a21815f`](https://github.com/awspring/spring-cloud-aws/commit/2a21815f0c5742b912baa3a02fc09df151663c65) — added `v4.0.*` for 4.0.0-M1
- [#424](https://github.com/awspring/spring-cloud-aws/pull/424) — "Fix release action" (3.0.0-M1 era)
- [#177](https://github.com/awspring/spring-cloud-aws/pull/177) — fixed the docs-to-S3 upload step (2.3.x era)

### Step 4 — Tag and let the Release workflow run

Push tag `vX.Y.Z` pointing at (or after) the prepare-release commit. The workflow then:

1. `./mvnw -V verify javadoc:aggregate -Pspring,docs-classic` — full test run + aggregated javadoc + reference docs.
2. `./mvnw -V -Prelease,spring deploy -DskipTests` — signs (GPG) and publishes to **Maven Central** via the
   Central Portal (`central-publishing-maven-plugin`, migrated from OSSRH/Sonatype in
   [#1515](https://github.com/awspring/spring-cloud-aws/pull/1515) on main and
   [#1513](https://github.com/awspring/spring-cloud-aws/pull/1513) on `3.4.x`).
3. Syncs `docs/target/generated-docs/` and `target/site/` to the `awspring-docs` S3 bucket
   (→ docs.awspring.io) and invalidates CloudFront.

Secrets involved: `CENTRAL_TOKEN_USERNAME/PASSWORD`, `MAVEN_GPG_PRIVATE_KEY`, `MAVEN_GPG_PASSPHRASE`,
`S3_AWS_ACCESS_KEY/SECRET_KEY`.

Note the tag does not have to point at the exact prepare-release commit — it is placed on the branch head at
release time (v4.1.0 → the release.yml fix commit, v4.0.2 → a feature commit merged after the prepare PR).

### Step 5 — GitHub Release notes + milestone

Releases are tracked with **milestones** (one per version, closed at release). The GitHub Release is
created manually with hand-written notes following a consistent format: reference-docs link, a Highlights
section, changes grouped per module (SQS, SNS, S3, ...) with PR links, and new contributors.
See [4.1.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.1.0) for the current template,
and the [milestone list](https://github.com/awspring/spring-cloud-aws/milestones?state=closed) for scoping.

### Step 6 — Back to snapshot

A PR restoring `-SNAPSHOT` in all poms. Convention has been the *next patch* snapshot
(4.0.0 → `4.0.1-SNAPSHOT` in [#1569](https://github.com/awspring/spring-cloud-aws/pull/1569)); the bump to the
next *minor* snapshot happens later as a separate decision
(`4.1.0-SNAPSHOT` in [#1578](https://github.com/awspring/spring-cloud-aws/pull/1578),
`3.4.0-SNAPSHOT` in [#1352](https://github.com/awspring/spring-cloud-aws/pull/1352)).

Examples: [#1658](https://github.com/awspring/spring-cloud-aws/pull/1658) (after 4.1.0),
[#1552](https://github.com/awspring/spring-cloud-aws/pull/1552) (after 4.0.0-RC1),
[#1325](https://github.com/awspring/spring-cloud-aws/pull/1325) (after 3.3.0).

⚠️ Watch the target version — this has gone wrong twice:
- After 4.1.0, [#1658](https://github.com/awspring/spring-cloud-aws/pull/1658) set `4.1.0-SNAPSHOT` —
  the *same* version that was just released — instead of `4.1.1-SNAPSHOT`.
- After 3.3.0-RC1, [#1304](https://github.com/awspring/spring-cloud-aws/pull/1304) set `3.3.1-SNAPSHOT`
  (skipping the not-yet-released 3.3.0) and was corrected to `3.3.0-SNAPSHOT` by
  [#1306](https://github.com/awspring/spring-cloud-aws/pull/1306).

### Step 7 — Update README

The [README compatibility matrix](README.md#compatibility-with-spring-project-versions) (Spring Cloud AWS ↔
Spring Cloud ↔ Spring Boot ↔ Spring Framework ↔ AWS SDK) and the version mentions are updated in a separate PR
after release: [#1565](https://github.com/awspring/spring-cloud-aws/pull/1565) (4.0.0),
[#1397](https://github.com/awspring/spring-cloud-aws/pull/1397) (3.3.1),
[#1326](https://github.com/awspring/spring-cloud-aws/pull/1326) (3.3.0),
[#1214](https://github.com/awspring/spring-cloud-aws/pull/1214) (3.2.0).

### Step 8 — Maintenance branches and patch releases

When `main` moves to the next minor/major, the released line gets a `X.Y.x` branch
(`3.0.x` ... `3.4.x`, `4.0.x`). Patch releases repeat the same sequence there:

1. **Add the branch to CI/docs workflows** — `build.yml` and `upload-docs.yml` list branches explicitly:
   [#1592](https://github.com/awspring/spring-cloud-aws/pull/1592) ("Add branch to ci/cd", `4.0.x`),
   [`d308b349`](https://github.com/awspring/spring-cloud-aws/commit/d308b3495) ("Run Github actions on 3.4.x" — the v3.4.0 tag points here).
2. **Cherry-pick fixes** from main — commits carry double PR references, original + backport
   (e.g. "Fix SQS MessageInterceptor exceptions handling (#1600) ([#1607](https://github.com/awspring/spring-cloud-aws/pull/1607))" on `4.0.x`).
3. **Update dependencies on the branch**: [#1590](https://github.com/awspring/spring-cloud-aws/pull/1590) /
   [#1591](https://github.com/awspring/spring-cloud-aws/pull/1591) (4.0.1),
   [#1504](https://github.com/awspring/spring-cloud-aws/pull/1504) (3.4.1).
4. **Prepare X.Y.Z release PR** on the branch, tag, GitHub Release. The branch may stay at the released
   version until the next patch prep (no back-to-snapshot between 4.0.1 and 4.0.2), or go back to snapshot
   immediately (3.3.x → `3.3.2-SNAPSHOT` right after 3.3.1).

---

## Known failure modes

| Failure | What happened | Fix |
|---|---|---|
| Tag doesn't trigger release | `release.yml` tag allowlist missing the new `vX.Y.*` pattern | [#1655](https://github.com/awspring/spring-cloud-aws/pull/1655), [`2a21815f`](https://github.com/awspring/spring-cloud-aws/commit/2a21815f0c5742b912baa3a02fc09df151663c65), [#424](https://github.com/awspring/spring-cloud-aws/pull/424) |
| Module missing from Maven Central | Module not listed in the parent pom's `<modules>`, so deploy skips it. Happened **twice** with the IMDS starter: 3.3.0 ([#1396](https://github.com/awspring/spring-cloud-aws/pull/1396) fixed it for 3.3.1) and again 3.4.0 (issue [#1484](https://github.com/awspring/spring-cloud-aws/issues/1484), fixed by [#1514](https://github.com/awspring/spring-cloud-aws/pull/1514) → forced the 3.4.1 release) | Verify new modules are in root `<modules>` before tagging |
| Wrong post-release snapshot version | See Step 6 warning ([#1304](https://github.com/awspring/spring-cloud-aws/pull/1304)→[#1306](https://github.com/awspring/spring-cloud-aws/pull/1306), [#1658](https://github.com/awspring/spring-cloud-aws/pull/1658)) | Next patch snapshot of the just-released version |
| Docs upload broken | S3 sync step in release workflow | [#177](https://github.com/awspring/spring-cloud-aws/pull/177), [#1013](https://github.com/awspring/spring-cloud-aws/pull/1013), [`efbc0555`](https://github.com/awspring/spring-cloud-aws/commit/efbc0555d) (docs-classic profile) |
| Publishing infra migration | OSSRH → Central Portal required `central-publishing-maven-plugin` on every live branch | [#1515](https://github.com/awspring/spring-cloud-aws/pull/1515) (main), [#1513](https://github.com/awspring/spring-cloud-aws/pull/1513) (3.4.x) |
| Release visible to users before announcement | Users asking "has X been released?" | Issues [#797](https://github.com/awspring/spring-cloud-aws/issues/797), [#1484](https://github.com/awspring/spring-cloud-aws/issues/1484) |

---

## Release history — steps per version

PR links are the release-process PRs only (version bumps, dependency alignment, workflow fixes, readme),
not feature PRs.

### 4.1.0 — 2026-07-22 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1647](https://github.com/awspring/spring-cloud-aws/pull/1647) Lift AWS SDK (issue [#1646](https://github.com/awspring/spring-cloud-aws/issues/1646)), [#1649](https://github.com/awspring/spring-cloud-aws/pull/1649) Lift spring versions |
| Prepare release | [#1648](https://github.com/awspring/spring-cloud-aws/pull/1648) Prepare release 4.1.0 |
| Release workflow fix | [#1655](https://github.com/awspring/spring-cloud-aws/pull/1655) Fix release yml (add `v4.1.*`) — tag v4.1.0 points at this commit |
| GitHub Release | [v4.1.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.1.0) |
| Back to snapshot | [#1658](https://github.com/awspring/spring-cloud-aws/pull/1658) — set `4.1.0-SNAPSHOT` (⚠️ same as released version) |

### 4.0.2 — 2026-04-30 (from `4.0.x`)

| Step | Reference |
|---|---|
| Cherry-picks | e.g. [#1607](https://github.com/awspring/spring-cloud-aws/pull/1607) (backport of #1600) |
| Prepare release | [#1613](https://github.com/awspring/spring-cloud-aws/pull/1613) Prepare 4.0.2 release |
| GitHub Release | [v4.0.2](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.0.2) — tag points at a post-prepare feature commit ([#1614](https://github.com/awspring/spring-cloud-aws/pull/1614)) |

### 4.0.1 — 2026-04-25 (from `4.0.x`)

| Step | Reference |
|---|---|
| Branch CI setup | [#1592](https://github.com/awspring/spring-cloud-aws/pull/1592) Add branch to ci/cd |
| Dependency alignment | [#1590](https://github.com/awspring/spring-cloud-aws/pull/1590) Spring deps, [#1591](https://github.com/awspring/spring-cloud-aws/pull/1591) AWS deps |
| Prepare release | [#1589](https://github.com/awspring/spring-cloud-aws/pull/1589) Prepare 4.0.1 release |
| GitHub Release | [v4.0.1](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.0.1) |

### 4.0.0 — 2026-01-28 (from `main`)

| Step | Reference |
|---|---|
| Drivers | Issues [#1412](https://github.com/awspring/spring-cloud-aws/issues/1412) (Boot 4 / SC 5), [#1534](https://github.com/awspring/spring-cloud-aws/issues/1534) (Spring Cloud 2025.1.0) |
| Prepare release | [#1563](https://github.com/awspring/spring-cloud-aws/pull/1563) Set version to 4.0.0 |
| README | [#1565](https://github.com/awspring/spring-cloud-aws/pull/1565) |
| GitHub Release | [v4.0.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.0.0) |
| Back to snapshot | [#1569](https://github.com/awspring/spring-cloud-aws/pull/1569) → `4.0.1-SNAPSHOT`; later [#1578](https://github.com/awspring/spring-cloud-aws/pull/1578) → `4.1.0-SNAPSHOT` |

### 4.0.0-RC1 — 2026-01-14 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1547](https://github.com/awspring/spring-cloud-aws/pull/1547) AWS SDK, [#1551](https://github.com/awspring/spring-cloud-aws/pull/1551) Spring Modulith 2.0.0 |
| Prepare release | [#1545](https://github.com/awspring/spring-cloud-aws/pull/1545) Prepare for release of 4.0.0-RC1 |
| GitHub Release | [v4.0.0-RC1](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.0.0-RC1) |
| Back to snapshot | [#1552](https://github.com/awspring/spring-cloud-aws/pull/1552) |

### 4.0.0-M1 — 2025-11-25 (from `main`)

| Step | Reference |
|---|---|
| Base version switch | [#1413](https://github.com/awspring/spring-cloud-aws/pull/1413) Boot 4.0.0/SC 5.0.0, [#1414](https://github.com/awspring/spring-cloud-aws/pull/1414) Set version to 4.0.0-SNAPSHOT |
| Dependency alignment | [#1521](https://github.com/awspring/spring-cloud-aws/pull/1521) release upgrades, [#1525](https://github.com/awspring/spring-cloud-aws/pull/1525) AWS SDK 2.39.0 + DAX |
| Publishing migration | [#1515](https://github.com/awspring/spring-cloud-aws/pull/1515) central-publishing-maven-plugin |
| Prepare release | [#1529](https://github.com/awspring/spring-cloud-aws/pull/1529) Set version to 4.0.0 M1 |
| Release workflow fix | [`2a21815f`](https://github.com/awspring/spring-cloud-aws/commit/2a21815f0c5742b912baa3a02fc09df151663c65) adjust release action (add `v4.0.*`) |
| GitHub Release | [v4.0.0-M1](https://github.com/awspring/spring-cloud-aws/releases/tag/v4.0.0-M1) |
| Back to snapshot | [#1527](https://github.com/awspring/spring-cloud-aws/pull/1527) Back to 4.0.0-SNAPSHOT |

### 3.4.2 — 2025-12-01 (from `3.4.x`)

| Step | Reference |
|---|---|
| Cherry-picks | e.g. [#1507](https://github.com/awspring/spring-cloud-aws/pull/1507)–[#1510](https://github.com/awspring/spring-cloud-aws/pull/1510), [#1528](https://github.com/awspring/spring-cloud-aws/pull/1528) |
| Publishing migration | [#1513](https://github.com/awspring/spring-cloud-aws/pull/1513) central-publishing-maven-plugin on 3.4.x |
| Prepare release | [#1533](https://github.com/awspring/spring-cloud-aws/pull/1533) Lift version for 3.4.2 release |
| GitHub Release | [v3.4.2](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.4.2) |

### 3.4.1 — 2025-11-10 (from `3.4.x`)

| Step | Reference |
|---|---|
| Driver | Issue [#1484](https://github.com/awspring/spring-cloud-aws/issues/1484) — IMDS starter missing from Maven Central in 3.4.0 |
| Cherry-picks + deps | [#1501](https://github.com/awspring/spring-cloud-aws/pull/1501), [#1504](https://github.com/awspring/spring-cloud-aws/pull/1504) Update dependencies |
| Fix | [#1514](https://github.com/awspring/spring-cloud-aws/pull/1514) Include IMDS module in main pom (tag v3.4.1 points here) |
| Prepare release | [#1505](https://github.com/awspring/spring-cloud-aws/pull/1505) Set version to 3.4.1 |
| GitHub Release | [v3.4.1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.4.1) |

### 3.4.0 — 2025-06-10 (from `main`)

| Step | Reference |
|---|---|
| Driver | Issue [#1400](https://github.com/awspring/spring-cloud-aws/issues/1400) — Boot 3.5.0 / SC 2025.0.0 compatibility |
| Base version switch | [#1352](https://github.com/awspring/spring-cloud-aws/pull/1352) Set version to 3.4.0-SNAPSHOT |
| Dependency alignment | [#1402](https://github.com/awspring/spring-cloud-aws/pull/1402) AWS SDK 2.31.54 + LocalStack 4.4.0, [#1321](https://github.com/awspring/spring-cloud-aws/pull/1321) WireMock, [#1323](https://github.com/awspring/spring-cloud-aws/pull/1323) jakarta-mail |
| Prepare release | [#1403](https://github.com/awspring/spring-cloud-aws/pull/1403) Release 3.4.0 |
| Branch cut | [`d308b349`](https://github.com/awspring/spring-cloud-aws/commit/d308b3495) Run Github actions on 3.4.x (tag v3.4.0 points here) |
| GitHub Release | [v3.4.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.4.0) |
| Back to snapshot / README | direct commits [`f6f44191`](https://github.com/awspring/spring-cloud-aws/commit/f6f441910) (3.4.1-SNAPSHOT), [`9e0cfaed`](https://github.com/awspring/spring-cloud-aws/commit/9e0cfaedf) (readme) |

### 3.3.1 — 2025-05-23 (from `3.3.x`)

| Step | Reference |
|---|---|
| Cherry-picks + deps | [#1390](https://github.com/awspring/spring-cloud-aws/pull/1390), [#1393](https://github.com/awspring/spring-cloud-aws/pull/1393) Update Spring dependencies, [#1394](https://github.com/awspring/spring-cloud-aws/pull/1394) |
| Fix | [#1396](https://github.com/awspring/spring-cloud-aws/pull/1396) Include IMDS starter in parent pom (first occurrence of the missing-module failure) |
| Prepare release | [#1395](https://github.com/awspring/spring-cloud-aws/pull/1395) Set version to 3.3.1 |
| GitHub Release | [v3.3.1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.3.1) |
| Back to snapshot | [`3b34d552`](https://github.com/awspring/spring-cloud-aws/commit/3b34d552f) Set version to 3.3.2-SNAPSHOT (on 3.3.x) |
| README | [#1397](https://github.com/awspring/spring-cloud-aws/pull/1397) (on main) |

### 3.3.0 — 2025-01-19 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1317](https://github.com/awspring/spring-cloud-aws/pull/1317) Spring Modulith 1.3, [#1320](https://github.com/awspring/spring-cloud-aws/pull/1320) AWS SDK 2.29.52 |
| Prepare release | [#1324](https://github.com/awspring/spring-cloud-aws/pull/1324) Prepare 3.3.0 release (tag points here) |
| GitHub Release | [v3.3.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.3.0) |
| Back to snapshot | [#1325](https://github.com/awspring/spring-cloud-aws/pull/1325) → 3.3.1-SNAPSHOT |
| README | [#1326](https://github.com/awspring/spring-cloud-aws/pull/1326) |

### 3.3.0-RC1 — 2024-12-19 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1290](https://github.com/awspring/spring-cloud-aws/pull/1290) Spring Cloud 4.2.0, [#1303](https://github.com/awspring/spring-cloud-aws/pull/1303) AWS SDK 2.29.35 + S3 Encryption Client 3.3.0 |
| Prepare release | [#1300](https://github.com/awspring/spring-cloud-aws/pull/1300) Set version to 3.3.0-RC1 |
| GitHub Release | [v3.3.0-RC1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.3.0-RC1) |
| Back to snapshot | [#1304](https://github.com/awspring/spring-cloud-aws/pull/1304) → 3.3.1-SNAPSHOT (⚠️ wrong), corrected by [#1306](https://github.com/awspring/spring-cloud-aws/pull/1306) → 3.3.0-SNAPSHOT |
| README | [#1305](https://github.com/awspring/spring-cloud-aws/pull/1305) |

### 3.3.0-M1 — 2024-11-07 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1266](https://github.com/awspring/spring-cloud-aws/pull/1266) AWS SDK, [#1267](https://github.com/awspring/spring-cloud-aws/pull/1267) Spring Cloud 4.2.0-M2 |
| Prepare release | [#1268](https://github.com/awspring/spring-cloud-aws/pull/1268) Prepare Release |
| Docs prep | [#1269](https://github.com/awspring/spring-cloud-aws/pull/1269) Prepare docs (tag points here) |
| GitHub Release | [v3.3.0-M1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.3.0-M1) |
| Back to snapshot + README | [#1270](https://github.com/awspring/spring-cloud-aws/pull/1270) |

### 3.2.1 — 2024-10-28 (from `3.2.x`)

| Step | Reference |
|---|---|
| Prepare release | [#1261](https://github.com/awspring/spring-cloud-aws/pull/1261) Set version to 3.2.1 (tag points here) |
| GitHub Release | [v3.2.1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.2.1) |

### 3.0.5 — 2024-09-22 (from `3.0.x`)

| Step | Reference |
|---|---|
| Prepare release | [#1228](https://github.com/awspring/spring-cloud-aws/pull/1228) Set version to 3.0.5 |
| GitHub Release | [v3.0.5](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.0.5) |
| Note | [#1227](https://github.com/awspring/spring-cloud-aws/pull/1227) "Prepare 3.1.2 release" was closed unmerged — 3.1.2 was never shipped (a `3.1.2` branch still exists upstream) |

### 3.2.0 — 2024-09-17 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1206](https://github.com/awspring/spring-cloud-aws/pull/1206) Spring Cloud 4.1.4, [#1207](https://github.com/awspring/spring-cloud-aws/pull/1207) AWS SDK 2.25.70 |
| Prepare release | [#1208](https://github.com/awspring/spring-cloud-aws/pull/1208) Set version to 3.2.0 |
| Docs | [#1210](https://github.com/awspring/spring-cloud-aws/pull/1210) Build docs (tag points here) |
| GitHub Release | [v3.2.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.2.0) |
| Back to snapshot | [#1212](https://github.com/awspring/spring-cloud-aws/pull/1212) → 3.3.0-SNAPSHOT |
| README | [#1214](https://github.com/awspring/spring-cloud-aws/pull/1214) |
| Post-release deps | [#1215](https://github.com/awspring/spring-cloud-aws/pull/1215), [#1216](https://github.com/awspring/spring-cloud-aws/pull/1216) |

### 3.2.0-M1 — 2024-04-02 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1085](https://github.com/awspring/spring-cloud-aws/pull/1085), [#1087](https://github.com/awspring/spring-cloud-aws/pull/1087) LocalStack 3.2.0, [#1115](https://github.com/awspring/spring-cloud-aws/pull/1115) AWS SDK 2.25.21 |
| Prepare release | [#1121](https://github.com/awspring/spring-cloud-aws/pull/1121) Set version to 3.2.0-M1 |
| GitHub Release | [v3.2.0-M1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.2.0-M1) |
| Back to snapshot | [#1122](https://github.com/awspring/spring-cloud-aws/pull/1122) Back to snapshots |

### 3.1.1 — 2024-03-18 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#1072](https://github.com/awspring/spring-cloud-aws/pull/1072) AWS SDK 2.25.10 |
| Docs publishing fix | [#1013](https://github.com/awspring/spring-cloud-aws/pull/1013) Fix publishing docs |
| Prepare release | [#1092](https://github.com/awspring/spring-cloud-aws/pull/1092) Set version to 3.1.1 |
| GitHub Release | [v3.1.1](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.1.1) |
| Back to snapshot | [#1098](https://github.com/awspring/spring-cloud-aws/pull/1098) → 3.1.2-SNAPSHOT, then [#1100](https://github.com/awspring/spring-cloud-aws/pull/1100) → 3.2.0-SNAPSHOT |
| README | [#1099](https://github.com/awspring/spring-cloud-aws/pull/1099) |

### 3.1.0 — 2023-12-10 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#956](https://github.com/awspring/spring-cloud-aws/pull/956) SC 4.1.0-RC1, [#981](https://github.com/awspring/spring-cloud-aws/pull/981) SC 4.1.0, [#983](https://github.com/awspring/spring-cloud-aws/pull/983) Testcontainers |
| Base version switch | [#959](https://github.com/awspring/spring-cloud-aws/pull/959) Set version to 3.1.0-SNAPSHOT |
| Prepare release | [#993](https://github.com/awspring/spring-cloud-aws/pull/993) Set version to 3.1.0 |
| Release workflow fix | [`efbc0555`](https://github.com/awspring/spring-cloud-aws/commit/efbc0555d) Use docs-classic in release action (tag points here) |
| GitHub Release | [v3.1.0](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.1.0) |
| Back to snapshot | [#994](https://github.com/awspring/spring-cloud-aws/pull/994) → 3.1.1-SNAPSHOT |

### 3.0.4 — 2023-12-10 (from `3.0.x`, same day as 3.1.0)

| Step | Reference |
|---|---|
| Prepare release | [#992](https://github.com/awspring/spring-cloud-aws/pull/992) Set version to 3.0.4 |
| GitHub Release | [v3.0.4](https://github.com/awspring/spring-cloud-aws/releases/tag/v3.0.4) |
| Back to snapshot | [`635a4051`](https://github.com/awspring/spring-cloud-aws/commit/635a40515) → 3.0.5-SNAPSHOT |

### 3.0.3 — 2023-11 (from `main`, before the 3.1.0-SNAPSHOT switch)

| Step | Reference |
|---|---|
| Dependency alignment | [#916](https://github.com/awspring/spring-cloud-aws/pull/916) spring-cloud-commons 4.0.4, [#940](https://github.com/awspring/spring-cloud-aws/pull/940) AWS SDK 2.21.12, [#941](https://github.com/awspring/spring-cloud-aws/pull/941) Testcontainers, [#949](https://github.com/awspring/spring-cloud-aws/pull/949) LocalStack |
| Prepare release | [#943](https://github.com/awspring/spring-cloud-aws/pull/943) Set version 3.0.3 |
| Back to snapshot | [#946](https://github.com/awspring/spring-cloud-aws/pull/946) → 3.0.4-SNAPSHOT |

### 3.0.2 — 2023-08 (from `main`)

| Step | Reference |
|---|---|
| Prepare release | [#866](https://github.com/awspring/spring-cloud-aws/pull/866) Set version to 3.0.2 |
| Back to snapshot | [#918](https://github.com/awspring/spring-cloud-aws/pull/918) → 3.0.3-SNAPSHOT |

### 3.0.1 — 2023-05 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#805](https://github.com/awspring/spring-cloud-aws/pull/805) AWS SDK 2.20.63, [#806](https://github.com/awspring/spring-cloud-aws/pull/806) AWS CRT |
| Prepare release | [#810](https://github.com/awspring/spring-cloud-aws/pull/810) Set version to 3.0.1 |
| Back to snapshot | [#812](https://github.com/awspring/spring-cloud-aws/pull/812) Back to snapshots |
| README | [#811](https://github.com/awspring/spring-cloud-aws/pull/811) |

### 3.0.0 — 2023-04 (from `main`)

| Step | Reference |
|---|---|
| Dependency alignment | [#771](https://github.com/awspring/spring-cloud-aws/pull/771) AWS SDK 2.20.49, [#755](https://github.com/awspring/spring-cloud-aws/pull/755) Testcontainers 1.18.0 |
| Prepare release | [#785](https://github.com/awspring/spring-cloud-aws/pull/785) Set version to 3.0.0 |
| Back to snapshot | [#786](https://github.com/awspring/spring-cloud-aws/pull/786) Back to snapshots |
| README | [#789](https://github.com/awspring/spring-cloud-aws/pull/789) |
| Post-release | Issue [#797](https://github.com/awspring/spring-cloud-aws/issues/797) "version 3.0.0 has not been published yet" (Central sync/announcement lag) |

### 3.0.0 pre-releases (condensed)

| Version | Prepare | Back to snapshot | Notes |
|---|---|---|---|
| 3.0.0-RC2 | [#735](https://github.com/awspring/spring-cloud-aws/pull/735) | [#737](https://github.com/awspring/spring-cloud-aws/pull/737) | deps sweep [#639](https://github.com/awspring/spring-cloud-aws/pull/639), [#607](https://github.com/awspring/spring-cloud-aws/pull/607)–[#612](https://github.com/awspring/spring-cloud-aws/pull/612) |
| 3.0.0-RC1 | [#645](https://github.com/awspring/spring-cloud-aws/pull/645) | [#651](https://github.com/awspring/spring-cloud-aws/pull/651) | |
| 3.0.0-M3 | [#548](https://github.com/awspring/spring-cloud-aws/pull/548) | — | AWS SDK bump [#547](https://github.com/awspring/spring-cloud-aws/pull/547) |
| 3.0.0-M2 | [#500](https://github.com/awspring/spring-cloud-aws/pull/500) | [#501](https://github.com/awspring/spring-cloud-aws/pull/501) | deps [#497](https://github.com/awspring/spring-cloud-aws/pull/497), [#498](https://github.com/awspring/spring-cloud-aws/pull/498), [#510](https://github.com/awspring/spring-cloud-aws/pull/510), [#511](https://github.com/awspring/spring-cloud-aws/pull/511) |
| 3.0.0-M1 | [#421](https://github.com/awspring/spring-cloud-aws/pull/421) | — | release action fix [#424](https://github.com/awspring/spring-cloud-aws/pull/424); main → 3.0.0-SNAPSHOT [#609](https://github.com/awspring/spring-cloud-aws/pull/609) |
