// workflow-template.js: paste into the Workflow tool's `script` and pass as args
//   {tracks:[{key,wt,br,base,cont?,start?,risk?,models?}], rules?, agents?, seBin?}
// Stages: build -> review -> fix. Any stage may end with wait={name} (a long run detached with bgrun.sh): the
// script runs a tiny iso-waiter (Bash only, cheap model) until the job is DONE, then a CONTINUATION agent with a fresh
// context takes over from NOTES.md. A stage may also end with checkpoint=true after ~50 tool calls: same continuation.
// Each track's scratch dir <agents>/<key> must hold TASK.md (under ~6 KB, an index at the top) before the run.
// Models (mixed): review always runs on the session's model. risk:'low' (docs, harness, CLI, tooling, tests only, code off
// the critical path) puts build and fix on Sonnet; anything else -- code where a mistake loses or corrupts data -- stays
// on the session's model. models:{build,review,fix} overrides per stage.
export const meta = {
  name: 'lean-tracks-economy',
  description: 'Build, adversarially review and fix tracks; long runs are waited on by a tiny waiter and every long agent is continued in a fresh context (subagent-economy skill)',
  phases: [{ title: 'Build' }, { title: 'Wait' }, { title: 'Review' }, { title: 'Fix' }],
}
// Paths: this machine's defaults. A = the per-project dir of track scratch dirs (TASK/NOTES/REPORT/REVIEW.md);
// keep it outside ~/.claude, where subagents may be refused writes.
// Both paths come from args (the tsdb-tracks skill says how to find them); no machine paths here.
if (!args.seBin || !args.agents) throw new Error('pass args.seBin (subagent-economy scripts dir) and args.agents (track scratch root)')
const BIN = args.seBin
const A = args.agents
const SHORT = {
  type: 'object',
  properties: {
    done: { type: 'boolean', description: 'scope finished and its checks green (builder) / ready to gate (fixer) / review written (reviewer)' },
    commits: { type: 'array', items: { type: 'string' } },
    summary: { type: 'string', description: 'at most ~12 lines; detail goes in files' },
    open: { type: 'array', items: { type: 'string' } },
    wait: { type: 'object', properties: { name: { type: 'string' }, note: { type: 'string' } }, required: ['name'], description: 'a bgrun.sh job to wait for; you END your stage now' },
    checkpoint: { type: 'boolean', description: 'you wrote NOTES.md and are handing over to a fresh agent' },
  },
  required: ['done', 'summary'],
}
const ECON = `Economy rules (skill subagent-economy, read it if unsure): never wait more than ~4 minutes inside your context -- start long runs with ${BIN}/bgrun.sh NAME CMD (or ${BIN}/rbgrun.sh on the fleet), write in ${A}/<key>/NOTES.md exactly what you were doing and how to judge the result, and END your stage returning wait={name}. Read narrowly (grep -n, sed -n of <=120 lines, git diff --stat, tail -n 40 of logs, each handover file once). Count your tool calls: at 50 (or as soon as your context feels long), stop, write NOTES.md and return checkpoint=true -- measured: re-reading the context is ~94% of all tokens, and one builder that never checkpointed ran 230 calls to a 452k context and cost as much as three whole tracks.`
const MODEL = (kind, t) => (t.models && t.models[kind]) || (kind !== 'review' && t.risk === 'low' ? 'sonnet' : undefined)
const mopt = (kind, t) => MODEL(kind, t) ? { model: MODEL(kind, t) } : {}
const ROLE = {
  build: 'ROLE builder: you write code and tests; detach every long suite, integration, coverage and sanitizer run.',
  review: 'ROLE adversarial reviewer: do not edit the branch; detach mutation batches (one bgrun per ~10 mutations, one result line each); review diff --stat first, then only hunks you will comment on.',
  fix: 'ROLE fixer: read REVIEW.md once; re-run only the suites a finding touched, then the full set ONCE at the end, detached.',
}
const head = t => `${args.rules ? `Read ${args.rules} first and follow it. ` : ''}Worktree ${t.wt}, branch ${t.br}, base ${t.base}; scratch dir ${A}/${t.key}. Track key: ${t.key}.`
const PHASE = { build: 'Build', review: 'Review', fix: 'Fix' }
const prompt = {
  build: t => `${head(t)}\n${ROLE.build}\n${ECON}\nTASK: ${A}/${t.key}/TASK.md (and NOTES.md if it exists: you are continuing earlier work, the branch holds it). When finished write ${A}/${t.key}/REPORT.md (built, every check and result, measurements, what is left).`,
  review: t => `${head(t)}\n${ROLE.review}\n${ECON}\nRead TASK.md and REPORT.md in ${A}/${t.key}, review 'git -C ${t.wt} diff ${t.base}..${t.br}'. Find what is WRONG (correctness, concurrency, lifetimes, wire/disk/API compatibility, tests without teeth -- apply mutations, a fix test that passes on the unfixed build, thin coverage, missing sanitizer/static-analysis runs, unmeasured perf claims, plan text vs code, security of untrusted input, anything missing). Evidence for every finding. Write ${A}/${t.key}/REVIEW.md.`,
  fix: t => `${head(t)}\n${ROLE.fix}\n${ECON}\nRead TASK.md, REPORT.md, REVIEW.md in ${A}/${t.key}. Verify each finding yourself; fix the real ones with tests that fail before the fix; squash into clean step commits; append '## After review' to REPORT.md. done=true only if everything is green and ready to gate.`,
}
const CONT = key => `\nYou are a CONTINUATION: an earlier agent stopped (a long run it detached, a checkpoint, or a usage limit). Read ${A}/${key}/NOTES.md and the branch log first; the detached job's output is where NOTES.md says.`
// The waiter: agent type iso-waiter (Bash only, cheap model; ~/.claude/agents/iso-waiter.md, visible to sessions started
// after it was written); a session that cannot see it falls back to the cheap model with the same strict prompt.
const WAITER_PROMPT = n => `You only wait for a detached job. Run ${BIN}/await.sh ${n} 540 ; if it exits 10 (still running) run it again, as many times as it takes. Do not read files, do not investigate, run nothing else. When it exits 0 or 11 return exactly its first output line and the output file path it printed. NAME = ${n}`
async function waiter(name, key) {
  const o = { label: `wait:${key}`, phase: 'Wait' }
  try { return await agent(`NAME = ${name}`, { ...o, agentType: 'iso-waiter' }) }
  catch (e) { return await agent(WAITER_PROMPT(name), { ...o, model: 'haiku' }) }
}
// one stage with its wait/checkpoint loop; returns the last agent result
async function stage(kind, t, first) {
  let r = await agent(first, { label: `${kind}:${t.key}`, phase: PHASE[kind], schema: SHORT, ...mopt(kind, t) })
  for (let hop = 0; r && !r.done && (r.wait || r.checkpoint) && hop < 40; hop++) {
    let w = null
    if (r.wait) w = await waiter(r.wait.name, t.key)
    const note = w ? `\nThe detached job ${r.wait.name} finished; the waiter reported: ${JSON.stringify(w)}. ${r.wait.note || ''}` : ''
    r = await agent(prompt[kind](t) + CONT(t.key) + note, { label: `${kind}:${t.key}#${hop + 2}`, phase: PHASE[kind], schema: SHORT, ...mopt(kind, t) })
  }
  return r
}
// start:'review' or 'fix' resumes a track whose earlier stages are done; cont:true continues an interrupted build.
return await pipeline(
  args.tracks,
  t => (t.start === 'fix' || t.start === 'review') ? { resumed: true } : stage('build', t, prompt.build(t) + (t.cont ? CONT(t.key) : '')),
  (b, t) => !b ? null : t.start === 'fix' ? { b, r: { resumed: true } } : stage('review', t, prompt.review(t)).then(r => ({ b, r })),
  (br, t) => br && br.r ? stage('fix', t, prompt.fix(t) + (t.start === 'fix' ? CONT(t.key) : '')).then(f => ({ track: t.key, build: br.b, review: br.r, fix: f })) : br,
)
