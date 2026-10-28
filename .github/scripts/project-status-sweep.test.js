'use strict';
// Tests for project-status-sweep.js. Run: node --test .github/scripts/*.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const sweep = require('./project-status-sweep.js');

const { prStatus, issueStatus, decide, HEARTBEAT_PREFIX, HOLD_OFF } = sweep;

// ---- fixtures ---------------------------------------------------------------

const user = (login) => ({ login, __typename: 'User' });
const bot = (login) => ({ login, __typename: 'Bot' });
const review = (author, state, submittedAt = '2026-10-27T09:00:00Z') => ({ author, state, submittedAt });
const verdict = (author, state) => ({ author, state });
const request = (login) => ({ requestedReviewer: { __typename: 'User', login } });
const move = (actorType, createdAt, project = 3) => ({ createdAt, actor: { __typename: actorType }, project: { number: project } });

const pr = (over = {}) => ({
  __typename: 'PullRequest',
  id: 'PR_1',
  number: 9,
  state: 'OPEN',
  isDraft: false,
  updatedAt: '2026-10-27T10:00:00Z',
  authorAssociation: 'MEMBER',
  author: { login: 'alice' },
  reviews: { nodes: [] },
  verdicts: { nodes: [] },
  reviewRequests: { nodes: [] },
  moves: { nodes: [] },
  ...over,
});

const issue = (over = {}) => ({
  __typename: 'Issue',
  id: 'I_1',
  number: 1,
  state: 'OPEN',
  updatedAt: '2026-10-27T10:00:00Z',
  assignees: { totalCount: 0 },
  moves: { nodes: [] },
  linked: { totalCount: 0, nodes: [] },
  manual: { totalCount: 0, nodes: [] },
  ...over,
});

const item = (content, status, over = {}) => ({
  id: `ITEM_${content?.number ?? 'x'}`,
  isArchived: false,
  content,
  status: status ? { name: status } : null,
  hold: null,
  ...over,
});

const linked = (...prs) => ({ totalCount: prs.length, nodes: prs });

// A fake octokit: serves board pages, records writes, can be told to fail.
function fakeGithub({ pages, workflows = [], failUpdates = 0, linkedById = {}, failLinked = false, failWorkflows = false, failCard = false }) {
  const writes = [];
  let page = 0;
  let updateFailures = failUpdates;
  return {
    writes,
    graphql: async (query, vars) => {
      if (query.includes('updateProjectV2ItemFieldValue')) {
        if (updateFailures > 0) { updateFailures -= 1; throw new Error('boom'); }
        writes.push({ kind: 'status', item: vars.i, option: vars.o });
        return {};
      }
      if (query.includes('DraftIssue(input')) {
        if (failCard) throw new Error('card write refused');
        const kind = query.includes('addProjectV2DraftIssue') ? 'heartbeat-add' : 'heartbeat-update';
        writes.push({ kind, id: vars.d, title: vars.t, body: vars.b });
        return {};
      }
      if (query.includes('workflows(')) {
        if (failWorkflows) throw new Error('forbidden');
        return { organization: { projectV2: { workflows: { nodes: workflows } } } };
      }
      if (query.includes('node(id:')) {
        if (failLinked) throw new Error('timeout');
        return { node: linkedById[vars.id] };
      }
      return pages[page++];
    },
  };
}

const OPTIONS = ['Todo', 'In Progress', 'Review', 'Done'].map((name) => ({ id: `OPT_${name}`, name }));

const boardPage = (nodes, { hasNextPage = false, remaining = 4000, field = { id: 'F', options: OPTIONS } } = {}) => ({
  rateLimit: { cost: 9, remaining },
  organization: {
    projectV2: {
      id: 'PROJ',
      field,
      items: { pageInfo: { hasNextPage, endCursor: hasNextPage ? 'next' : null }, nodes },
    },
  },
});

function fakeCore() {
  const out = { info: [], warning: [], failed: [], outputs: {} };
  return {
    out,
    info: (m) => out.info.push(m),
    warning: (m) => out.warning.push(m),
    setFailed: (m) => out.failed.push(m),
    setOutput: (k, v) => { out.outputs[k] = v; },
  };
}

const run = (gh, opts = {}) => {
  const core = fakeCore();
  return sweep({ github: gh, core, sleep: async () => {}, now: new Date('2026-10-28T03:20:00Z'), ...opts })
    .then((result) => ({ result, core: core.out }));
};

// ---- PR rules -----------------------------------------------------------------

test('PR with no reviews is In Progress', () => {
  assert.equal(prStatus(pr()), 'In Progress');
});

test('draft PR stays In Progress even when approved', () => {
  assert.equal(prStatus(pr({ isDraft: true, verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } })), 'In Progress');
});

test('a human comment review moves a PR to Review', () => {
  assert.equal(prStatus(pr({ reviews: { nodes: [review(user('bob'), 'COMMENTED')] } })), 'Review');
});

test("the author's own replies do not count as review", () => {
  assert.equal(prStatus(pr({ reviews: { nodes: [review(user('alice'), 'COMMENTED')] } })), 'In Progress');
});

test('bot and code-scanning reviews do not count', () => {
  const p = pr({
    reviews: { nodes: [review(bot('github-advanced-security'), 'COMMENTED')] },
    verdicts: { nodes: [verdict(bot('some-bot'), 'CHANGES_REQUESTED')] },
  });
  assert.equal(prStatus(p), 'In Progress');
});

test('pending (unsubmitted) reviews do not count', () => {
  assert.equal(prStatus(pr({ reviews: { nodes: [review(user('bob'), 'PENDING', null)] } })), 'In Progress');
});

test('an approval moves a PR to Review', () => {
  assert.equal(prStatus(pr({ verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } })), 'Review');
});

test('changes requested sends a PR back to In Progress', () => {
  const p = pr({
    verdicts: { nodes: [verdict(user('bob'), 'CHANGES_REQUESTED')] },
    reviews: { nodes: [review(user('bob'), 'CHANGES_REQUESTED'), review(user('alice'), 'COMMENTED')] },
  });
  assert.equal(prStatus(p), 'In Progress');
});

test('one approval does not outweigh another reviewer requesting changes', () => {
  const p = pr({ verdicts: { nodes: [verdict(user('bob'), 'APPROVED'), verdict(user('cat'), 'CHANGES_REQUESTED')] } });
  assert.equal(prStatus(p), 'In Progress');
});

test('re-requesting review from the reviewer who asked for changes moves it to Review', () => {
  const p = pr({
    verdicts: { nodes: [verdict(user('bob'), 'CHANGES_REQUESTED')] },
    reviewRequests: { nodes: [request('bob')] },
  });
  assert.equal(prStatus(p), 'Review');
});

test('re-requesting only some blocking reviewers keeps In Progress', () => {
  const p = pr({
    verdicts: { nodes: [verdict(user('bob'), 'CHANGES_REQUESTED'), verdict(user('cat'), 'CHANGES_REQUESTED')] },
    reviewRequests: { nodes: [request('bob')] },
  });
  assert.equal(prStatus(p), 'In Progress');
});

test('a stale changes-requested verdict is not lost behind many later comments', () => {
  const p = pr({
    verdicts: { nodes: [verdict(user('bob'), 'CHANGES_REQUESTED')] },
    reviews: { nodes: Array.from({ length: 30 }, () => review(user('bob'), 'COMMENTED')) },
  });
  assert.equal(prStatus(p), 'In Progress');
});

test('a PR whose only review was dismissed counts as reviewed', () => {
  assert.equal(prStatus(pr({ reviews: { nodes: [review(user('bob'), 'DISMISSED')] } })), 'Review');
});

// ---- Issue rules --------------------------------------------------------------

test('unassigned issue with no PR is Todo', () => {
  assert.equal(issueStatus(issue()), 'Todo');
});

test('assigned issue is In Progress', () => {
  assert.equal(issueStatus(issue({ assignees: { totalCount: 1 } })), 'In Progress');
});

test('issue follows its trusted linked PR into Review', () => {
  const i = issue({ linked: linked(pr({ verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } })) });
  assert.equal(issueStatus(i), 'Review');
});

test("an outside contributor's Closes #N does not move the issue", () => {
  const outside = pr({ authorAssociation: 'FIRST_TIME_CONTRIBUTOR', verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } });
  assert.equal(issueStatus(issue({ linked: linked(outside) })), 'Todo');
});

test('an outside PR a maintainer linked from the sidebar is followed', () => {
  const outside = pr({ authorAssociation: 'CONTRIBUTOR' });
  const i = issue({ linked: linked(outside), manual: { totalCount: 1, nodes: [{ id: outside.id }] } });
  assert.equal(issueStatus(i), 'In Progress');
});

test('a merged or closed linked PR is ignored', () => {
  assert.equal(issueStatus(issue({ linked: linked(pr({ state: 'MERGED' })) })), 'Todo');
});

// ---- What the sweep leaves alone -----------------------------------------------

test('skips archived, draft, inaccessible and closed items', () => {
  assert.deepEqual(decide(item(pr(), 'Todo', { isArchived: true })), { skip: 'archived' });
  assert.deepEqual(decide(item({ __typename: 'DraftIssue', id: 'D', title: 'plan' }, 'Todo')), { skip: 'draft item' });
  assert.deepEqual(decide(item(null, 'Todo')), { skip: 'inaccessible' });
  assert.deepEqual(decide(item(pr({ state: 'MERGED' }), 'Review')), { skip: 'closed' });
});

test('never touches Done or any status it does not manage', () => {
  assert.deepEqual(decide(item(pr(), 'Done')), { skip: 'status "Done" not managed' });
  assert.deepEqual(decide(item(pr(), 'Blocked')), { skip: 'status "Blocked" not managed' });
});

test('never touches a card with Auto status = Off', () => {
  assert.deepEqual(decide(item(pr(), 'Todo', { hold: { name: HOLD_OFF } })), { skip: 'auto status off' });
});

test('moves a PR card out of Todo', () => {
  assert.deepEqual(decide(item(pr(), 'Todo')), { target: 'In Progress' });
});

test('keeps a hand move until the PR changes', () => {
  const p = pr({ moves: { nodes: [move('Bot', '2026-10-27T09:00:00Z'), move('User', '2026-10-27T11:00:00Z')] } });
  assert.deepEqual(decide(item(p, 'Review')), { skip: 'set by hand' });
});

test('releases a hand move once the PR changes again', () => {
  const p = pr({ updatedAt: '2026-10-27T12:00:00Z', moves: { nodes: [move('User', '2026-10-27T11:00:00Z')] } });
  assert.deepEqual(decide(item(p, 'Review')), { target: 'In Progress' });
});

test('a hand move on another board does not count', () => {
  const p = pr({ moves: { nodes: [move('User', '2026-10-27T11:00:00Z', 2)] } });
  assert.deepEqual(decide(item(p, 'Review')), { target: 'In Progress' });
});

test('a later bot move (the sweep itself) is not a hand move', () => {
  const p = pr({ moves: { nodes: [move('User', '2026-10-27T08:00:00Z'), move('Bot', '2026-10-27T11:00:00Z')] } });
  assert.deepEqual(decide(item(p, 'Review')), { target: 'In Progress' });
});

test("an issue's hand move is released when its trusted linked PR changes", () => {
  const i = issue({
    moves: { nodes: [move('User', '2026-10-27T11:00:00Z')] },
    linked: linked(pr({ updatedAt: '2026-10-27T12:00:00Z', verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } })),
  });
  assert.deepEqual(decide(item(i, 'Todo')), { target: 'Review' });
});

test("an untrusted PR's activity does not release an issue's hand move", () => {
  const i = issue({
    moves: { nodes: [move('User', '2026-10-27T11:00:00Z')] },
    linked: linked(pr({ authorAssociation: 'NONE', updatedAt: '2026-10-27T12:00:00Z' })),
  });
  assert.deepEqual(decide(item(i, 'Review')), { skip: 'set by hand' });
});

test('a card already in the right status is left alone', () => {
  assert.deepEqual(decide(item(issue(), 'Todo')), { skip: 'up to date' });
});

// ---- Whole sweep: failure modes ---------------------------------------------------

test('a clean run moves cards, writes the health card and reports ok', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(pr({ number: 5 }), 'Todo')])] });
  const { result, core } = await run(gh);
  assert.equal(result.outcome, 'ok');
  assert.deepEqual(gh.writes[0], { kind: 'status', item: 'ITEM_5', option: 'OPT_In Progress' });
  assert.equal(gh.writes[1].kind, 'heartbeat-add');
  assert.match(gh.writes[1].title, /^Board sweep: OK, last run 2026-10-28 03:20 UTC$/);
  assert.equal(core.failed.length, 0);
});

test('reads every page', async () => {
  const gh = fakeGithub({
    pages: [
      boardPage([item(pr({ number: 5 }), 'Todo')], { hasNextPage: true }),
      boardPage([item(pr({ number: 6 }), 'Todo')]),
    ],
  });
  const { result } = await run(gh);
  assert.deepEqual(result.changes.map((c) => c.label), ['PR #5', 'PR #6']);
});

test('updates the existing health card instead of adding another', async () => {
  const hb = item({ __typename: 'DraftIssue', id: 'DRAFT_HB', title: `${HEARTBEAT_PREFIX} OK, last run earlier` }, null);
  const gh = fakeGithub({ pages: [boardPage([hb])] });
  await run(gh);
  assert.deepEqual(gh.writes.map((w) => w.kind), ['heartbeat-update']);
  assert.equal(gh.writes[0].id, 'DRAFT_HB');
});

test('a dry run changes no Status, and lists its plan on the health card only', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(pr({ number: 5 }), 'Todo')])] });
  const { result } = await run(gh, { dryRun: true });
  assert.deepEqual(gh.writes.map((w) => w.kind), ['heartbeat-add']);
  assert.match(gh.writes[0].title, /^Board sweep: DRY RUN, last run /);
  assert.match(gh.writes[0].body, /PR #5: Todo -> In Progress/);
  assert.equal(result.moves.length, 1);
});

// ---- Privacy: the Actions log is public, the board is not ------------------------

test('the public log names no issue, PR, status or draft; the private card has the details', async () => {
  const field = { id: 'F', options: OPTIONS.filter((o) => o.name !== 'Review') };
  const nodes = [
    item(pr({ number: 501, verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } }), 'Todo'),
    item(issue({ number: 602, assignees: { totalCount: 1 } }), 'Todo'),
    item(pr({ number: 703 }), 'Todo'),
    item({ __typename: 'DraftIssue', id: 'D1', title: 'Secret launch plan' }, 'Todo'),
    item(null, 'Todo'),
  ];
  const gh = fakeGithub({
    pages: [boardPage(nodes, { field })],
    workflows: [{ name: 'Pull request linked to issue', enabled: true }],
    failUpdates: 3,
  });
  const { core } = await run(gh);
  const publicLog = [...core.info, ...core.warning, ...core.failed].join('\n');
  for (const leak of ['#501', '#602', '#703', 'Todo', 'In Progress', 'Review', 'draft', 'Secret', 'linked to issue', 'inaccessible', 'boom']) {
    assert.ok(!publicLog.includes(leak), `public log leaked "${leak}": ${publicLog}`);
  }
  const card = gh.writes.find((w) => w.kind === 'heartbeat-add');
  for (const detail of ['PR #501', 'issue #602', 'PR #703: Todo -> In Progress', 'Pull request linked to issue', 'draft item 1', 'boom']) {
    assert.ok(card.body.includes(detail), `card is missing "${detail}"`);
  }
});

test('a health card that cannot be written fails the run without details', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(pr({ number: 5 }), 'Todo')])], failCard: true });
  const { result, core } = await run(gh);
  assert.equal(result.outcome, 'failed');
  assert.equal(result.cardWritten, false);
  assert.equal(core.failed.length, 1);
  assert.ok(!core.failed[0].includes('#5') && !core.failed[0].includes('card write refused'));
});

test('a failing update is retried, then succeeds', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(pr({ number: 5 }), 'Todo')])], failUpdates: 2 });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'ok');
  assert.equal(gh.writes.filter((w) => w.kind === 'status').length, 1);
});

test('an update that keeps failing marks the run failed, other cards still move', async () => {
  const gh = fakeGithub({
    pages: [boardPage([item(pr({ number: 5 }), 'Todo'), item(pr({ number: 6 }), 'Todo')])],
    failUpdates: 3,
  });
  const { result, core } = await run(gh);
  assert.equal(result.outcome, 'failed');
  assert.equal(result.failed, 1);
  assert.equal(gh.writes.filter((w) => w.kind === 'status').length, 1);
  assert.equal(core.failed.length, 1);
  assert.match(gh.writes.at(-1).title, /NEEDS ATTENTION/);
});

test('a renamed status option is reported, not fatal', async () => {
  const field = { id: 'F', options: OPTIONS.filter((o) => o.name !== 'Review') };
  const p = pr({ number: 5, verdicts: { nodes: [verdict(user('bob'), 'APPROVED')] } });
  const gh = fakeGithub({ pages: [boardPage([item(p, 'Todo'), item(issue({ number: 6, assignees: { totalCount: 1 } }), 'Todo')], { field })] });
  const { result, core } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.ok(result.problems.some((m) => m.includes('no "Review" status option')));
  assert.equal(gh.writes.filter((w) => w.kind === 'status').length, 1);
  assert.equal(core.failed.length, 0);
});

test('a missing Status field changes nothing and is reported', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(pr({ number: 5 }), 'Todo')], { field: null })] });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.equal(gh.writes.filter((w) => w.kind === 'status').length, 0);
});

test('conflicting built-in board workflows are reported', async () => {
  const gh = fakeGithub({
    pages: [boardPage([])],
    workflows: [
      { name: 'Pull request linked to issue', enabled: true },
      { name: 'Item closed', enabled: true },
      { name: 'Code review approved', enabled: false },
    ],
  });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.ok(result.problems.some((m) => m.includes('Pull request linked to issue') && !m.includes('Item closed')));
});

test('an unreadable workflow list is not a problem', async () => {
  const gh = fakeGithub({ pages: [boardPage([])], failWorkflows: true });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'ok');
});

test('cards from repos the App cannot read are reported', async () => {
  const gh = fakeGithub({ pages: [boardPage([item(null, 'Todo')])] });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.ok(result.problems.some((m) => m.includes('install the App')));
});

test('a truncated linked-PR list is fetched in full', async () => {
  const many = Array.from({ length: 7 }, (_, n) => pr({ id: `PR_${n}`, number: 100 + n, state: n === 6 ? 'OPEN' : 'MERGED' }));
  const first = issue({ number: 6, linked: { totalCount: 7, nodes: many.slice(0, 5) } });
  const gh = fakeGithub({
    pages: [boardPage([item(first, 'Todo')])],
    linkedById: { I_1: { linked: { totalCount: 7, nodes: many }, manual: { totalCount: 0, nodes: [] } } },
  });
  const { result } = await run(gh);
  assert.deepEqual(result.changes.map((c) => [c.label, c.to]), [['issue #6', 'In Progress']]);
});

test('an issue whose linked PRs cannot be read is left as is', async () => {
  const first = issue({ number: 6, linked: { totalCount: 9, nodes: [pr()] } });
  const gh = fakeGithub({ pages: [boardPage([item(first, 'Todo')])], failLinked: true });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.equal(result.changes.length, 0);
  assert.equal(result.skipped['incomplete data'], 1);
});

test('stops reading early when the API rate limit runs low', async () => {
  const gh = fakeGithub({
    pages: [
      boardPage([item(pr({ number: 5 }), 'Todo')], { hasNextPage: true, remaining: 100 }),
      boardPage([item(pr({ number: 6 }), 'Todo')]),
    ],
  });
  const { result } = await run(gh);
  assert.equal(result.outcome, 'degraded');
  assert.deepEqual(result.changes.map((c) => c.label), ['PR #5']);
});
