// SPDX-License-Identifier: AGPL-3.0-only
// verify-patterns.js: the cheap adversarial-verify shape for fan-out workflows (audits, reviews), measured on the
// clock audit of 2026-10-18 (189 agents, 29 M relative cost; about half of it avoidable). Paste what you need into a
// Workflow script. Three rules:
//   1. Only findings that could matter go to verifiers (severity minor and above), never "safe but interesting" sites.
//   2. Findings in one file are verified by ONE agent (it reads the file once), as a batch with one verdict each.
//   3. Two skeptics per batch; a third only for the findings they disagree on.
// Every new agent costs ~50k relative just to start (its ~41k start context is written once), so fewer, fuller agents win.

const VERDICTS = {
  type: 'object',
  properties: {
    verdicts: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          id: { type: 'string' },
          refuted: { type: 'boolean' },
          severity_if_real: { type: 'string', enum: ['none', 'cosmetic', 'minor', 'major', 'blocker'] },
          reasoning: { type: 'string' },
        },
        required: ['id', 'refuted', 'reasoning'],
      },
    },
  },
  required: ['verdicts'],
}

const SEVERITY = ['none', 'cosmetic', 'minor', 'major', 'blocker']   // in rank order: sort by this, not by name
const MATTERS = f => SEVERITY.indexOf(f.severity) >= SEVERITY.indexOf('minor')

// findings: [{id, file, line, claim, severity, evidence}]; lenses: two (or three) distinct skeptic instructions.
// Returns every finding with votes, confirmed and severity_verified (the median of the confirming verdicts):
// confirmed true/false by majority; null when two skeptics split and there is no third lens (judge those yourself);
// findings below 'minor' come back unverified (unverified: true, confirmed: null).
async function verifyByFile(findings, lenses, ctx, phaseName = 'Verify') {
  const byFile = {}
  for (const f of findings.filter(MATTERS)) (byFile[f.file] = byFile[f.file] || []).push(f)
  const ask = (batch, lens, tag) => agent(`${ctx}
Adversarially verify these findings, all in ${batch[0].file}. Read the file (and its callers) ONCE, then judge each.
YOUR LENS: ${lens}
Default to refuted=true when you cannot confirm a finding as described; if it is real but mis-rated, give severity_if_real.
FINDINGS: ${JSON.stringify(batch.map(f => ({ id: f.id, line: f.line, severity: f.severity, claim: f.claim, evidence: f.evidence })))}`,
    { label: `verify:${batch[0].file.split('/').pop()}:${tag}`, phase: phaseName, schema: VERDICTS })
  const out = await pipeline(Object.values(byFile),
    batch => parallel([0, 1].map(i => () => ask(batch, lenses[i], `s${i + 1}`))).then(vs => ({ batch, vs })),
    async ({ batch, vs }) => {
      const v = id => vs.filter(Boolean).map(x => (x.verdicts || []).find(y => y.id === id)).filter(Boolean)
      const split = batch.filter(f => { const a = v(f.id); return a.length === 2 && a[0].refuted !== a[1].refuted })
      let third = null
      if (split.length && lenses[2]) third = await ask(split, lenses[2], 's3')
      return batch.map(f => {
        const all = v(f.id).concat(third ? (third.verdicts || []).filter(y => y.id === f.id) : [])
        const yes = all.filter(x => !x.refuted)
        const sev = yes.map(x => x.severity_if_real).filter(Boolean).sort((a, b) => SEVERITY.indexOf(a) - SEVERITY.indexOf(b))
        const tie = all.length > 0 && yes.length * 2 === all.length
        return { ...f, votes: `${yes.length}/${all.length}`, confirmed: tie ? null : yes.length * 2 > all.length,
                 severity_verified: sev.length ? sev[Math.floor(sev.length / 2)] : f.severity }
      })
    })
  const dropped = findings.filter(f => !MATTERS(f)).map(f => ({ ...f, unverified: true, confirmed: null }))
  if (dropped.length) log(`${dropped.length} findings below 'minor' were not sent to verifiers (returned as unverified)`)
  return out.filter(Boolean).flat().concat(dropped)
}
