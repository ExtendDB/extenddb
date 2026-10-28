'use strict';
/**
 * Project status sweep for the ExtendDB Roadmap board (org Project 3).
 *
 * Run from the default branch by .github/workflows/project-status-sweep.yml.
 * It reads every open issue and pull request on the board and sets Status
 * from their real state:
 *
 *   Todo         issue with no assignee and no trusted open linked PR
 *   In Progress  issue assigned or with a trusted open linked PR; any PR that
 *                is a draft, not yet reviewed, or has an outstanding
 *                changes-requested verdict
 *   Review       PR reviewed by a person other than its author (bots ignored),
 *                including one whose changes-requested reviewer has been asked
 *                to review again; an issue whose trusted linked PR is in Review
 *
 * A PR whose only human review was dismissed still counts as reviewed: a
 * person did look at it.
 *
 * It never touches:
 *   - closed or merged items (the board's built-in workflows set Done)
 *   - draft (board-only), inaccessible and archived items
 *   - a card whose Status is not one of the three above (e.g. Done, Blocked)
 *   - a card whose "Auto status" field is "Off" (a permanent hold)
 *   - a card a person moved by hand, until the issue, the PR, or the issue's
 *     trusted linked PR changes again. "By hand" is read from the item's
 *     timeline (the last status-change event on this board was by a User).
 *
 * Trust: an issue only follows a linked PR whose author is an
 * owner/member/collaborator, or that a maintainer linked from the sidebar.
 * A "Closes #N" in an outside PR's body cannot move anyone else's card.
 *
 * Privacy: this runs in a public repo, so its Actions log is public while the
 * board is not. The log gets one line of totals only: no issue or PR numbers,
 * no statuses, no draft counts, no board settings. Every detail (each move,
 * each problem, what was skipped) goes to one board-only draft card titled
 * "Board sweep: ...", which only org members can see.
 *
 * Health: that card is rewritten on every run with the outcome and time, so a
 * stopped or failing sweep is visible on the board. Dry runs write only that
 * card. A run fails (red, and GitHub emails the workflow's owner) when card
 * updates fail or the card itself cannot be written; other problems are
 * "degraded": the run stays green and the card title says NEEDS ATTENTION.
 */

const ORG = 'ExtendDB';
const PROJECT_NUMBER = 3;
const STATUS_FIELD = 'Status';
const HOLD_FIELD = 'Auto status';
const HOLD_OFF = 'Off';
const HEARTBEAT_PREFIX = 'Board sweep:';
// Statuses the sweep manages, lowest to highest.
const MANAGED = ['Todo', 'In Progress', 'Review'];
const TRUSTED = new Set(['OWNER', 'MEMBER', 'COLLABORATOR']);
const REVIEW_STATES = new Set(['APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED']);
// Built-in board workflows that also set Status and would fight the sweep.
const CONFLICTING_WORKFLOWS = [
  'Item added to project',
  'Item reopened',
  'Pull request linked to issue',
  'Code changes requested',
  'Code review approved',
];
const LINKED_LIMIT = 5; // more are fetched one issue at a time
const MIN_RATE_REMAINING = 500;
const RETRIES = 2;

const rank = (status) => MANAGED.indexOf(status);
const latest = (...times) => times.filter(Boolean).sort().at(-1);
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const MOVES = (type) => `
fragment ${type}Moves on ${type === 'pr' ? 'PullRequest' : 'Issue'}TimelineItemsConnection {
  nodes { ... on ProjectV2ItemStatusChangedEvent { createdAt actor { __typename } project { number } } }
}`;

const PR_FRAGMENT = `
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
  reviewRequests(first: 10) {
    nodes { requestedReviewer { __typename ... on User { login } } }
  }
}`;

const LINKED_FIELDS = (limit) => `
  linked: closedByPullRequestsReferences(first: ${limit}, includeClosedPrs: false) {
    totalCount
    nodes { ...pr }
  }
  manual: closedByPullRequestsReferences(first: ${limit}, includeClosedPrs: false, userLinkedOnly: true) {
    totalCount
    nodes { id }
  }`;

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
          hold: fieldValueByName(name: "${HOLD_FIELD}") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          content {
            __typename
            ... on DraftIssue { id title }
            ... on PullRequest {
              ...pr
              moves: timelineItems(last: 10, itemTypes: [PROJECT_V2_ITEM_STATUS_CHANGED_EVENT]) { ...prMoves }
            }
            ... on Issue {
              id
              number
              state
              updatedAt
              assignees { totalCount }
              moves: timelineItems(last: 10, itemTypes: [PROJECT_V2_ITEM_STATUS_CHANGED_EVENT]) { ...issueMoves }
              ${LINKED_FIELDS(LINKED_LIMIT)}
            }
          }
        }
      }
    }
  }
}
${PR_FRAGMENT}
${MOVES('pr')}
${MOVES('issue')}`;

// Full linked-PR lists for an issue whose first page was truncated.
const LINKED_QUERY = `
query($id: ID!) {
  node(id: $id) {
    ... on Issue { ${LINKED_FIELDS(100)} }
  }
}
${PR_FRAGMENT}`;

const WORKFLOWS_QUERY = `
query($org: String!, $num: Int!) {
  organization(login: $org) {
    projectV2(number: $num) { workflows(first: 30) { nodes { name enabled } } }
  }
}`;

const UPDATE = `
mutation($p: ID!, $i: ID!, $f: ID!, $o: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $p, itemId: $i, fieldId: $f, value: { singleSelectOptionId: $o }
  }) { projectV2Item { id } }
}`;

const ADD_DRAFT = `
mutation($p: ID!, $t: String!, $b: String!) {
  addProjectV2DraftIssue(input: { projectId: $p, title: $t, body: $b }) { projectItem { id } }
}`;

const UPDATE_DRAFT = `
mutation($d: ID!, $t: String!, $b: String!) {
  updateProjectV2DraftIssue(input: { draftIssueId: $d, title: $t, body: $b }) { draftIssue { id } }
}`;

/** Is this review by a person who is not the PR's author? */
const byOtherHuman = (review, author) =>
  review.author?.__typename === 'User' && review.author.login !== author;

/** Status a pull request's card should have. */
function prStatus(pr) {
  if (pr.isDraft) return 'In Progress';
  const author = pr.author?.login;

  // Reviewers asked to look again: their old verdict is no longer the last word.
  const requested = new Set(
    (pr.reviewRequests?.nodes ?? []).map((r) => r.requestedReviewer?.login).filter(Boolean),
  );

  // One current verdict per reviewer (dismissed ones are already dropped).
  const verdicts = (pr.verdicts?.nodes ?? []).filter((r) => byOtherHuman(r, author));
  const blocking = verdicts.filter(
    (r) => r.state === 'CHANGES_REQUESTED' && !requested.has(r.author.login),
  );
  if (blocking.length > 0) return 'In Progress';
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
  if (!c) return { skip: 'inaccessible' };
  if (c.__typename !== 'PullRequest' && c.__typename !== 'Issue') return { skip: 'draft item' };
  if (c.state !== 'OPEN') return { skip: 'closed' };
  if (item.hold?.name === HOLD_OFF) return { skip: 'auto status off' };

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

/** Run a GraphQL call, retrying a few times with backoff. */
async function withRetry(fn, sleep) {
  let lastErr;
  for (let attempt = 0; attempt <= RETRIES; attempt += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt < RETRIES) await sleep(2000 * 2 ** attempt);
    }
  }
  throw lastErr;
}

/** Fill in linked-PR lists that the paged query cut short. */
async function completeLinked(github, issue, sleep) {
  const truncated = ['linked', 'manual'].some((k) => issue[k] && issue[k].totalCount > issue[k].nodes.length);
  if (!truncated) return;
  const { node } = await withRetry(() => github.graphql(LINKED_QUERY, { id: issue.id }), sleep);
  issue.linked = node.linked;
  issue.manual = node.manual;
  for (const k of ['linked', 'manual']) {
    if (issue[k].totalCount > issue[k].nodes.length) {
      throw new Error(`more than ${issue[k].nodes.length} linked PRs`);
    }
  }
}

/** Board workflows that are on and would also set Status. */
async function conflictingWorkflows(github) {
  const data = await github.graphql(WORKFLOWS_QUERY, { org: ORG, num: PROJECT_NUMBER });
  return data.organization.projectV2.workflows.nodes
    .filter((w) => w.enabled && CONFLICTING_WORKFLOWS.includes(w.name))
    .map((w) => w.name);
}

/**
 * Rewrite (or create) the board's health card. Never throws. Errors are not
 * logged in detail: the log is public.
 */
async function writeHeartbeat({ github, project, heartbeat, title, body, sleep }) {
  try {
    if (heartbeat) {
      await withRetry(() => github.graphql(UPDATE_DRAFT, { d: heartbeat.id, t: title, b: body }), sleep);
    } else {
      await withRetry(() => github.graphql(ADD_DRAFT, { p: project.id, t: title, b: body }), sleep);
    }
    return true;
  } catch {
    return false;
  }
}

const MAX_CARD_LINES = 300;

/** Private health-card body: every detail of the run. */
function cardBody({ outcome, dryRun, moves, problems, skipped, cost, runUrl }) {
  const lines = moves.slice(0, MAX_CARD_LINES);
  return [
    `Outcome: **${outcome}**${dryRun ? ' (dry run: no Status was changed)' : ''}`,
    ...(runUrl ? [`Run: ${runUrl}`] : []),
    '',
    ...(problems.length ? ['**Problems**', ...problems.map((p) => `- ${p}`), ''] : []),
    `**${dryRun ? 'Would move' : 'Moved'}** (${moves.length})`,
    ...(lines.length ? lines.map((m) => `- ${m}`) : ['- none']),
    ...(moves.length > lines.length ? [`- ...and ${moves.length - lines.length} more`] : []),
    '',
    `**Left alone**: ${Object.entries(skipped).map(([k, v]) => `${k} ${v}`).join(', ') || 'none'}`,
    `Query cost: ${cost}`,
  ].join('\n');
}

/**
 * Sweep the board. `github` is the octokit from actions/github-script.
 * With dryRun, log the changes without making them.
 * Returns { outcome: 'ok' | 'degraded' | 'failed', problems, changes, skipped, failed }.
 */
async function sweep({ github, core, dryRun = false, runUrl = '', now = new Date(), sleep = defaultSleep }) {
  const problems = [];
  const skipped = {};
  const changes = [];
  let project = null;
  let heartbeat = null;
  let after = null;
  let cost = 0;

  do {
    const data = await withRetry(
      () => github.graphql(QUERY, { org: ORG, num: PROJECT_NUMBER, after }),
      sleep,
    );
    cost += data.rateLimit?.cost ?? 0;
    const p = data.organization.projectV2;
    project ??= p;

    for (const item of p.items.nodes) {
      const c = item.content;
      if (c?.__typename === 'DraftIssue' && c.title?.startsWith(HEARTBEAT_PREFIX)) {
        heartbeat ??= c;
        continue;
      }
      if (c?.__typename === 'Issue') {
        try {
          await completeLinked(github, c, sleep);
        } catch (err) {
          problems.push(`issue #${c.number}: could not read all linked PRs (${err.message}); left as is.`);
          skipped['incomplete data'] = (skipped['incomplete data'] ?? 0) + 1;
          continue;
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
    const remaining = data.rateLimit?.remaining;
    if (after && remaining !== undefined && remaining < MIN_RATE_REMAINING) {
      problems.push(`API rate limit low (${remaining} left); stopped reading the board early.`);
      after = null;
    }
  } while (after);

  if (skipped.inaccessible) {
    problems.push(
      `${skipped.inaccessible} card(s) are from repos the sweep's GitHub App cannot read; install the App there.`,
    );
  }

  try {
    const conflicts = await conflictingWorkflows(github);
    if (conflicts.length) {
      problems.push(`Board workflows that also set Status are on: ${conflicts.join(', ')}. Turn them off.`);
    }
  } catch (err) {
    // Not a problem worth flagging: the token may simply lack that read.
  }

  let failed = 0;
  const moves = [];
  if (!project.field) {
    problems.push(`No "${STATUS_FIELD}" single-select field on the board; nothing changed.`);
    changes.length = 0;
  }
  for (const ch of changes) {
    const option = project.field.options.find((o) => o.name === ch.to);
    if (!option) {
      problems.push(`${ch.label}: board has no "${ch.to}" status option; left as ${ch.from}.`);
      ch.to = null;
      continue;
    }
    if (!dryRun) {
      try {
        await withRetry(
          () => github.graphql(UPDATE, { p: project.id, i: ch.itemId, f: project.field.id, o: option.id }),
          sleep,
        );
      } catch (err) {
        failed += 1;
        problems.push(`${ch.label}: update to ${ch.to} failed after ${RETRIES + 1} tries (${err.message}).`);
        ch.to = null;
        continue;
      }
    }
    moves.push(`${ch.label}: ${ch.from} -> ${ch.to}`);
  }

  // The health card is the only alert channel, so failing to write it fails the run.
  let outcome = failed ? 'failed' : problems.length ? 'degraded' : 'ok';
  const stamp = now.toISOString().slice(0, 16).replace('T', ' ');
  const label = dryRun ? 'DRY RUN' : outcome === 'ok' ? 'OK' : 'NEEDS ATTENTION';
  const cardWritten = project
    ? await writeHeartbeat({
      github,
      project,
      heartbeat,
      title: `${HEARTBEAT_PREFIX} ${label}, last run ${stamp} UTC`,
      body: cardBody({ outcome, dryRun, moves, problems, skipped, cost, runUrl }),
      sleep,
    })
    : false;
  if (!cardWritten) outcome = 'failed';

  // Public log: totals only. Details are on the private card.
  const issues = problems.length + (cardWritten ? 0 : 1);
  core.info(
    `Sweep ${outcome}${dryRun ? ' (dry run)' : ''}: ${moves.length} card(s) ${dryRun ? 'would move' : 'moved'}, `
    + `${issues} problem(s), query cost ${cost}. Details are on the board's "${HEARTBEAT_PREFIX}" card.`,
  );
  if (outcome === 'degraded') core.warning(`${problems.length} problem(s); see the board's "${HEARTBEAT_PREFIX}" card.`);
  if (outcome === 'failed') {
    core.setFailed(cardWritten
      ? `${failed} card update(s) failed; see the board's "${HEARTBEAT_PREFIX}" card.`
      : `Could not write the board's "${HEARTBEAT_PREFIX}" card; check the GitHub App's install and permissions.`);
  }
  return { outcome, problems, changes, moves, skipped, failed, cardWritten };
}

module.exports = sweep;
Object.assign(module.exports, {
  prStatus,
  issueStatus,
  decide,
  QUERY,
  LINKED_QUERY,
  WORKFLOWS_QUERY,
  HEARTBEAT_PREFIX,
  HOLD_FIELD,
  HOLD_OFF,
});
