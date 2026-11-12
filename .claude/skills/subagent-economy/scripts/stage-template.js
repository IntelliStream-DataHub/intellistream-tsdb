// SPDX-License-Identifier: AGPL-3.0-only
// stage-template.js: stage a gate batch (cherry-pick tracks onto main, number bugs, run the stage checks), then have a
// second agent check the merge. Paste into the Workflow tool's `script` with args:
//   {label:'B20', wt:'/abs/relq20', branch:'rel-queue20', base:'<sha of LOCAL main to cut from>',
//    tracks:[{key:'d8', branch:'fix-witness', commits:'a b c', from:'<their base>'}],
//    checks:'<what to run on the tip>', conflicts?:'<project rules for resolving conflicts>',
//    bugs?:'<how placeholders are numbered and where the ledger is>', mainTree?:'/abs/main/checkout',
//    rules?:'/abs/RULES.md', agents?:'<dir of track scratch dirs>', notes?:'<anything the stager must know>',
//    stagerModel?, checkModel?}
// Both agents run on Sonnet (staging is mechanical; the gate and the tracks' reviews carry the judgement); a batch whose
// tracks CONFLICT in code where a mistake loses data passes checkModel:'opus' (stagerModel too if the resolution is more
// than mechanical). The stager never pushes and never queues the gate; the orchestrator does, after asking.
export const meta = {
  name: 'stage-batch',
  description: 'Stage tracks onto main as one gate batch (Sonnet), then independently check the merge (Sonnet)',
  phases: [{ title: 'Stage' }, { title: 'Wait' }, { title: 'Check' }],
}
if (!args.seBin || !args.agents) throw new Error('pass args.seBin (this skill\'s scripts dir) and args.agents (track scratch root)')
const BIN = args.seBin
const A = args.agents, S = `${A}/stage${args.label}`
const RULES = args.rules ? `Read ${args.rules} first and follow it. ` : ''
const SHORT = {
  type: 'object',
  properties: {
    done: { type: 'boolean', description: 'batch staged and every check green' },
    tip: { type: 'string' },
    summary: { type: 'string', description: 'the full report when done (markdown); otherwise <= 12 lines' },
    wait: { type: 'object', properties: { name: { type: 'string' }, note: { type: 'string' } }, required: ['name'] },
    checkpoint: { type: 'boolean' },
  },
  required: ['done', 'summary'],
}
const ECON = `Economy (skill subagent-economy): never wait more than ~4 minutes inside your context -- start long runs with ${BIN}/bgrun.sh NAME CMD (local) or ${BIN}/rbgrun.sh (fleet), write in ${S}/NOTES.md exactly what you were doing and how to judge each result, and END your stage returning wait={name}. Read narrowly. At ~50 tool calls write NOTES.md and return checkpoint=true.`
const list = args.tracks.map((t, i) => ` ${i + 1}. track ${t.key}: branch ${t.branch}, commits ${t.commits} (cut from ${t.from}). Report ${A}/${t.key}/REPORT.md.`).join('\n')
const TASK = `${RULES}You are the STAGER for gate batch ${args.label}. Scratch dir ${S} (create it).
GOAL: a branch ${args.branch} in a NEW worktree ${args.wt}, cut from LOCAL main ${args.base} (it may be ahead of origin/main by commits that ride with this gate), holding, in this order:
${list}
Cherry-pick them; resolve conflicts keeping both sides. ${args.conflicts || ''} Check that nothing the tracks each added under a number or name (flags, event/error codes, bits, migration or plan file numbers) collides.
${args.bugs ? `BUG NUMBERS: ${args.bugs} Change placeholders IN THESE COMMITS ONLY. ` : ''}NEVER start a commit-message line with '#' (git strips it as a comment: write 'Bug #NNN: ...').
CHECKS on the tip: ${args.checks || "the project's full check target -- read it and run EVERY static line before its first suite (a stager once skipped one and the gate went red on it); release + debug builds with 0 warnings; the unit suite; each track's own checks as its REPORT names them"}. Prefer the fleet (rbgrun.sh) when one is configured. A failure: run the same on main to decide whether it is pre-existing; record the evidence.
${args.notes || ''}
DO NOT push, do not queue the gate, ${args.mainTree ? `do not touch main's working tree (${args.mainTree}), ` : ''}never git reset --hard main, never ssh to the fleet yourself.
When done, return done=true, tip=<sha>, summary = the full report in markdown: commits table (new sha, track, source sha, what), every conflict and its resolution, bug numbers assigned, every check with result and log path, what the integrator must know. Put the report in summary, not in a file.`
const CONT = `\nYou are a CONTINUATION: an earlier stager stopped. Read ${S}/NOTES.md and 'git -C ${args.wt} log --oneline ${args.base}..' first.`
const WAITER_PROMPT = n => `You only wait for a detached job. Run ${BIN}/await.sh ${n} 540 ; if it exits 10 (still running) run it again, as many times as it takes. Do not read files, do not investigate, run nothing else. When it exits 0 or 11 return exactly its first output line and the output file path it printed. NAME = ${n}`
async function waiter(name) {
  const o = { label: `wait:${args.label}`, phase: 'Wait' }
  try { return await agent(`NAME = ${name}`, { ...o, agentType: 'iso-waiter' }) }
  catch (e) { return await agent(WAITER_PROMPT(name), { ...o, model: 'haiku' }) }
}
phase('Stage')
let r = await agent(`${TASK}\n${ECON}`, { label: `stage:${args.label}`, phase: 'Stage', schema: SHORT, model: args.stagerModel || 'sonnet' })
for (let hop = 0; r && !r.done && (r.wait || r.checkpoint) && hop < 30; hop++) {
  let w = null
  if (r.wait) w = await waiter(r.wait.name)
  const note = w ? `\nThe detached job ${r.wait.name} finished; the waiter reported: ${JSON.stringify(w)}. ${r.wait.note || ''}` : ''
  r = await agent(`${TASK}\n${ECON}${CONT}${note}`, { label: `stage:${args.label}#${hop + 2}`, phase: 'Stage', schema: SHORT, model: args.stagerModel || 'sonnet' })
}
if (!r || !r.done) return { staged: false, stager: r }
phase('Check')
const VERDICT = {
  type: 'object',
  properties: { ok: { type: 'boolean' }, problems: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } },
  required: ['ok', 'problems'],
}
const srcs = args.tracks.map(t => `${t.key} (${t.branch}: ${t.commits}, cut from ${t.from})`).join('; ')
const check = await agent(`${RULES}You independently CHECK a staged gate batch; do not edit anything, do not run long suites. Worktree ${args.wt}, branch ${args.branch}, tip ${r.tip}, base ${args.base}. It should hold: ${srcs}.
The stager's report:\n${r.summary}\n
Check with evidence (git commands, grep) and try to REFUTE the stager's claims: (1) nothing lost: per file, each source branch's changes are in ${args.base}..${args.branch}, differing only by conflict resolution or bug-number substitution, and nothing of ${args.base} was dropped; (2) conflict resolutions correct (numbers and names distinct, masks and lists complete); (3) no placeholder left, numbers unique and continuing, any ledger consistent, no commit body lost a line that began with '#'; (4) every check the report lists exists as a log saying what the report says. Return ok=true only if nothing is wrong.`, { label: `check:${args.label}`, phase: 'Check', schema: VERDICT, model: args.checkModel || 'sonnet' })
return { staged: true, tip: r.tip, report: r.summary, check }
