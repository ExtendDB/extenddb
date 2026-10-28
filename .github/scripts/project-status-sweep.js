'use strict';
/**
 * Project status sweep for the ExtendDB Roadmap board (org Project 3).
 *
 * Run on a schedule from the default branch by
 * .github/workflows/project-status-sweep.yml. It reads every open issue and
 * pull request on the board and sets Status from their real state:
 *
 *   Todo         issue with no assignee and no trusted open linked PR
 *   In Progress  issue assigned or with a trusted open linked PR; any PR that
 *                is a draft, not yet reviewed, or has changes requested
 *   Review       PR reviewed by a person other than its author (bots ignored);
 *                an issue whose trusted linked PR is in Review
 *
 * A PR whose only human review was dismissed still counts as reviewed: a
 * person did look at it.
 *
 * It never touches:
 *   - closed or merged items (the board's built-in workflows set Done)
 *   - draft (board-only), inaccessible and archived items
 *   - a card whose Status is not one of the three above (e.g. Done, Blocked)
 *   - a card a person moved by hand, until the issue, the PR, or the issue's
 *     trusted linked PR changes again. "By hand" is read from the item's
 *     timeline (the last status-change event on this board was by a User).
 *
 * Trust: an issue only follows a linked PR whose author is an
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
const LINKED_LIMIT = 10;

const rank = (status) => MANAGED.indexOf(status);
const latest = (...times) => times.filter(Boolean).sort().at(-1);

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
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          content {
            __typename
            ... on PullRequest {
              ...pr
              moves: timelineItems(last: 10, itemTypes: [PROJECT_V2_ITEM_STATUS_CHANGED_EVENT]) { ...prMoves }
            }
            ... on Issue {
              number
              state
              updatedAt
              assignees { totalCount }
              moves: timelineItems(last: 10, itemTypes: [PROJECT_V2_ITEM_STATUS_CHANGED_EVENT]) { ...issueMoves }
              linked: closedByPullRequestsReferences(first: ${LINKED_LIMIT}, includeClosedPrs: false) {
                totalCount
                nodes { ...pr }
              }
              manual: closedByPullRequestsReferences(first: ${LINKED_LIMIT}, includeClosedPrs: false, userLinkedOnly: true) {
                totalCount
                nodes { id }
              }
            }
          }
        }
      }
    }
  }
}
fragment prMoves on PullRequestTimelineItemsConnection {
  nodes { ... on ProjectV2ItemStatusChangedEvent { createdAt actor { __typename } project { number } } }
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
  verdicts: latestOpinionatedReviews(first: 50) {
    nodes { state author { login __typename } }
  }
}
# Issues and PRs have differently named timeline connection types.
fragment issueMoves on IssueTimelineItemsConnection {
  nodes { ... on ProjectV2ItemStatusChangedEvent { createdAt actor { __typename } project { number } } }
}`;

const UPDATE = `
mutation($p: ID!, $i: ID!, $f: ID!, $o: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $p, itemId: $i, fieldId: $f, value: { singleSelectOptionId: $o }
  }) { projectV2Item { id } }
}`;

/** Is this review by a person who is not the PR's author? */
const byOtherHuman = (review, author) =>
  review.author?.__typename === 'User' && review.author.login !== author;

/** Status a pull request's card should have. */
function prStatus(pr) {
  if (pr.isDraft) return 'In Progress';
  const author = pr.author?.login;

  // One current verdict per reviewer (dismissed ones are already dropped).
  const verdicts = (pr.verdicts?.nodes ?? []).filter((r) => byOtherHuman(r, author));
  if (verdicts.some((r) => r.state === 'CHANGES_REQUESTED')) return 'In Progress';
  if (verdicts.length > 0) return 'Review';

  // No verdict yet: any submitted human review (comment, dismissed) still counts.
  const reviewed = (pr.reviews?.nodes ?? []).some(
    (r) => r.submittedAt && REVIEW_STATES.has(r.state) && byOtherHuman(r, author),
  );
  return reviewed ? 'Review' : 'In Progress';
}

/** Open linked PRs an issue may follow. */
function trustedOpenPrs(issue) {
  const manual = new Set((issue.manual?.nodes ?? []).map((p) => p.id));
  return (issue.linked?.nodes ?? []).filter(
    (pr) => pr.state === 'OPEN' && (TRUSTED.has(pr.authorAssociation) || manual.has(pr.id)),
  );
}

/** Status an issue's card should have. */
function issueStatus(issue) {
  let best = issue.assignees.totalCount > 0 ? 'In Progress' : 'Todo';
  for (const pr of trustedOpenPrs(issue)) {
    // Any trusted open PR means work has started, at least In Progress.
    const s = prStatus(pr);
    if (rank(s) > rank(best)) best = s;
  }
  return best;
}

/** Last status change on this board, from the issue's or PR's timeline. */
function lastMove(content) {
  return (content.moves?.nodes ?? []).filter((e) => e.project?.number === PROJECT_NUMBER).at(-1);
}

/** Decide what to do with one board item: { target } or { skip: reason }. */
function decide(item) {
  if (item.isArchived) return { skip: 'archived' };
  const c = item.content;
  if (!c || (c.__typename !== 'PullRequest' && c.__typename !== 'Issue')) {
    return { skip: 'draft or inaccessible item' };
  }
  if (c.state !== 'OPEN') return { skip: 'closed' };

  const current = item.status?.name ?? null;
  if (current !== null && rank(current) === -1) return { skip: `status "${current}" not managed` };

  // A person moved this card: keep it until the work changes again.
  const move = lastMove(c);
  if (move?.actor?.__typename === 'User') {
    const changedAt = c.__typename === 'Issue'
      ? latest(c.updatedAt, ...trustedOpenPrs(c).map((p) => p.updatedAt))
      : c.updatedAt;
    if (changedAt <= move.createdAt) return { skip: 'set by hand' };
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
      const c = item.content;
      for (const key of ['linked', 'manual']) {
        const conn = c?.[key];
        if (conn && conn.totalCount > conn.nodes.length) {
          core.warning(`issue #${c.number}: only the first ${conn.nodes.length} of ${conn.totalCount} linked PRs were read.`);
        }
      }
      const d = decide(item);
      if (d.skip) {
        skipped[d.skip] = (skipped[d.skip] ?? 0) + 1;
      } else {
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
