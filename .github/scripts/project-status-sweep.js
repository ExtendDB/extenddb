'use strict';
/**
 * Project status sweep for the ExtendDB Roadmap board (org Project 3).
 *
 * Run on a schedule from the default branch by
 * .github/workflows/project-status-sweep.yml. It reads every open issue and
 * pull request on the board and sets Status from their real state:
 *
 *   Todo         issue with no assignee and no open linked PR
 *   In Progress  issue assigned or with an open linked PR; any PR not yet
 *                reviewed, a draft PR, or one with changes requested
 *   Review       PR reviewed by someone other than its author (not a bot)
 *
 * It never touches:
 *   - closed or merged items (the board's built-in workflows set Done)
 *   - draft (board-only) items and archived items
 *   - a card whose Status is not one of the three above (e.g. Done, Blocked)
 *   - a card a person moved by hand, until its issue or PR changes again
 *
 * Fork safety: an issue only follows a linked PR whose author is an
 * owner/member/collaborator, or that a maintainer linked from the sidebar.
 * A "Closes #N" in an outside PR's body cannot move anyone else's card.
 */

const ORG = 'ExtendDB';
const PROJECT_NUMBER = 3;
const STATUS_FIELD = 'Status';
// Statuses the sweep manages, lowest to highest.
const MANAGED = ['Todo', 'In Progress', 'Review'];
const TRUSTED = new Set(['OWNER', 'MEMBER', 'COLLABORATOR']);
const REVIEW_STATES = new Set(['APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED']);

const rank = (status) => MANAGED.indexOf(status);

const QUERY = `
query($org: String!, $num: Int!, $after: String) {
  rateLimit { cost remaining }
  organization(login: $org) {
    projectV2(number: $num) {
      id
      field(name: "${STATUS_FIELD}") {
        ... on ProjectV2SingleSelectField { id options { id name } }
      }
      items(first: 50, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isArchived
          status: fieldValueByName(name: "${STATUS_FIELD}") {
            ... on ProjectV2ItemFieldSingleSelectValue {
              name
              updatedAt
              creator { login __typename }
            }
          }
          content {
            __typename
            ... on PullRequest { ...pr }
            ... on Issue {
              number
              state
              updatedAt
              assignees { totalCount }
              linked: closedByPullRequestsReferences(first: 10, includeClosedPrs: false) {
                nodes { ...pr }
              }
              manual: closedByPullRequestsReferences(first: 10, includeClosedPrs: false, userLinkedOnly: true) {
                nodes { id }
              }
            }
          }
        }
      }
    }
  }
}
fragment pr on PullRequest {
  id
  number
  state
  isDraft
  updatedAt
  authorAssociation
  author { login }
  reviews(last: 30) {
    nodes { state submittedAt author { login __typename } }
  }
}`;

const UPDATE = `
mutation($p: ID!, $i: ID!, $f: ID!, $o: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $p, itemId: $i, fieldId: $f, value: { singleSelectOptionId: $o }
  }) { projectV2Item { id } }
}`;

/** Status a pull request's card should have. */
function prStatus(pr) {
  if (pr.isDraft) return 'In Progress';
  const author = pr.author?.login;
  const reviews = [...(pr.reviews?.nodes ?? [])]
    .filter((r) => r.submittedAt) // pending reviews have no submittedAt
    .sort((a, b) => a.submittedAt.localeCompare(b.submittedAt));

  let reviewed = false;
  const decision = new Map(); // reviewer -> their latest non-comment verdict
  for (const r of reviews) {
    // Only people count: skip bots (code scanning, Copilot) and the author's own replies.
    if (r.author?.__typename !== 'User' || r.author.login === author) continue;
    if (!REVIEW_STATES.has(r.state)) continue;
    reviewed = true;
    if (r.state !== 'COMMENTED') decision.set(r.author.login, r.state);
  }
  if ([...decision.values()].includes('CHANGES_REQUESTED')) return 'In Progress';
  return reviewed ? 'Review' : 'In Progress';
}

/** Status an issue's card should have. */
function issueStatus(issue) {
  const manual = new Set((issue.manual?.nodes ?? []).map((p) => p.id));
  let best = issue.assignees.totalCount > 0 ? 'In Progress' : 'Todo';
  for (const pr of issue.linked?.nodes ?? []) {
    if (pr.state !== 'OPEN') continue;
    if (!TRUSTED.has(pr.authorAssociation) && !manual.has(pr.id)) continue;
    // Any trusted open PR means work has started, at least In Progress.
    const s = prStatus(pr);
    if (rank(s) > rank(best)) best = s;
  }
  return best;
}

/** Decide what to do with one board item: { target } or { skip: reason }. */
function decide(item) {
  if (item.isArchived) return { skip: 'archived' };
  const c = item.content;
  if (!c || (c.__typename !== 'PullRequest' && c.__typename !== 'Issue')) return { skip: 'draft item' };
  if (c.state !== 'OPEN') return { skip: 'closed' };

  const current = item.status?.name ?? null;
  if (current !== null && rank(current) === -1) return { skip: `status "${current}" not managed` };

  // A person set this status by hand: keep it until the issue or PR changes again.
  const setter = item.status?.creator;
  if (setter?.__typename === 'User' && c.updatedAt <= item.status.updatedAt) {
    return { skip: 'set by hand' };
  }

  const target = c.__typename === 'PullRequest' ? prStatus(c) : issueStatus(c);
  if (target === current) return { skip: 'up to date' };
  return { target };
}

/**
 * Sweep the board. `github` is the octokit from actions/github-script.
 * With dryRun, log the changes without making them.
 */
async function sweep({ github, core, dryRun = false }) {
  let project = null;
  let after = null;
  let cost = 0;
  const skipped = {};
  const changes = [];

  do {
    const data = await github.graphql(QUERY, { org: ORG, num: PROJECT_NUMBER, after });
    cost += data.rateLimit?.cost ?? 0;
    const p = data.organization.projectV2;
    project ??= p;
    for (const item of p.items.nodes) {
      const d = decide(item);
      if (d.skip) {
        skipped[d.skip] = (skipped[d.skip] ?? 0) + 1;
      } else {
        const c = item.content;
        changes.push({
          itemId: item.id,
          label: `${c.__typename === 'PullRequest' ? 'PR' : 'issue'} #${c.number}`,
          from: item.status?.name ?? '(none)',
          to: d.target,
        });
      }
    }
    after = p.items.pageInfo.hasNextPage ? p.items.pageInfo.endCursor : null;
  } while (after);

  if (!project.field) {
    core.warning(`No "${STATUS_FIELD}" single-select field on the board; nothing changed.`);
    return { changes: [], skipped, failed: 0 };
  }

  let failed = 0;
  for (const ch of changes) {
    const option = project.field.options.find((o) => o.name === ch.to);
    if (!option) {
      core.warning(`${ch.label}: board has no "${ch.to}" status, left as ${ch.from}.`);
      ch.to = null;
      continue;
    }
    if (dryRun) {
      core.info(`[dry run] ${ch.label}: ${ch.from} -> ${ch.to}`);
      continue;
    }
    try {
      await github.graphql(UPDATE, { p: project.id, i: ch.itemId, f: project.field.id, o: option.id });
      core.info(`${ch.label}: ${ch.from} -> ${ch.to}`);
    } catch (err) {
      failed += 1;
      core.warning(`${ch.label}: update failed: ${err.message}`);
    }
  }

  const moved = changes.filter((c) => c.to).length;
  core.info(`${dryRun ? 'Would move' : 'Moved'} ${moved} card(s); query cost ${cost}.`);
  core.info(`Left alone: ${JSON.stringify(skipped)}`);
  if (failed) core.setFailed(`${failed} card update(s) failed.`);
  return { changes, skipped, failed };
}

module.exports = sweep;
module.exports.prStatus = prStatus;
module.exports.issueStatus = issueStatus;
module.exports.decide = decide;
module.exports.QUERY = QUERY;
