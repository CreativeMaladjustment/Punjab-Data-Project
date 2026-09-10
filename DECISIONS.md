# Decision Log — Punjab Data Project

Append-only. Each entry: context → decision → consequences. Reversals get a new
entry referencing the old one, never an edit.

---

## D-001 (2026-07, pilot) — Three-layer data model
**Context:** The catalog is both source and object of study; "cleaning" it destroys
evidence about the observing apparatus.
**Decision:** Page image → verbatim imperial record → normalized layer. Verbatim
fields are never edited to fix the catalog; normalization is a separate, inspectable
act (aliases.json + deterministic rules in postprocess.py).
**Consequences:** Every analysis must choose its layer explicitly; misprints and
registrar editorializing remain queryable.

## D-002 (2026-07, pilot) — Provenance and uncertainty policy
**Decision:** Every entry carries pdf_page + printed_page. Uncertain readings go to
`flags` (feeding the adjudication queue), never silently guessed. Davis's sheets are
cross-validation, not gold.
**Consequences:** Queues accumulate (Q2 ~40, Q3 55, Q4 38 items) and require periodic
adjudication sessions against page images.

## D-003 (2026-07, Q2/Q3) — Serial continuity rules
**Context:** Serials looked chaotic until the June→Sept boundary was examined.
**Decision:** Treat serials as continuing across quarters within a year AND across
script subsections of a language (e.g., Q4 PUNJABI—POETRY resumes at 242 after Q3
PUNJABI (PERSIAN CHARACTER)—POETRY ended at 241; URDU (HINDI CHARACTER)—DRAMA s19
continues URDU—DRAMA s18). Registration numbers are one annual sequence.
**Consequences:** Serial monotonicity is a validity check; broken sequences are
evidence of misreads (used to correct p043 Q3) or genuine catalog anomalies (flagged,
e.g. Q4 p34 serial 4 between 19 and 20).

## D-004 (2026-07, Q3) — Ambiguous-glyph resolution with internal evidence
**Context:** Pushto reg 921/924; blur between 3/8 etc.
**Decision:** An ambiguous glyph may be resolved using documented internal evidence
(reg uniqueness within year, serial neighbors), recorded in the flag. This is not a
correction of the record but a reading of it.
**Consequences:** Flags carry the full reasoning chain (e.g., Q4 p15 s298 reg
1331-vs-1381 duplicate analysis).

## D-005 (2026-07, Q3/Q4; amended 2026-07-09) — Prior-hand marginalia are data
**Decision:** Pencil X-marks, running numerals, and handwritten verso indexes are
captured in `marks` and in marginalia_*.md files, but all interpretation (collection
tracking hypothesis) is held as open questions, not findings.
**Amendment (2026-07-09):** These marks are **NOT Davis's** — confirmed with him: they
predate his acquisition of the volumes and are in an unidentified earlier hand. The
data capture is unchanged (we still track every mark), but every attribution was
de-Davis'd across the extractions, marginalia files, explorer UI ("annotated" filter),
and memo. The apparatus is now framed as a prior owner's/reader's second-order
annotation layer — a provenance question, which if anything strengthens the
observation-operator angle.
**Consequences:** Memo (S4) asks Davis about the *source* of the hand, not its meaning.

## D-006 (2026-07, Q3) — Davis-sheet diff made optional
**Context:** No transcription sheets exist for Q3/Q4.
**Decision:** validate.py runs internal checks always, Davis-diff only when a sheet
exists in the manifest.

## D-007 (2026-07-08) — Vertical slice before scaling extraction
**Context:** Year 1910 complete (1,026 entries). Choice: keep extracting (1911+) vs.
build an analysis prototype. Year-level aggregation immediately exposed normalization
debt (lang naming variants, city case/accent duplicates) invisible at page level.
**Decision:** Build the 1910 vertical slice first: S1 normalization, S2 printer–
publisher network, S3 script-market comparison, S4 memo to Davis (see PLAN.md §4).
Extraction resumes afterward, with the API-batch path evaluated once the schema is
validated by real analytical use.
**Consequences:** 1911Q1 deferred; schema fixes land before 40+ more quarters are
extracted; Davis gets a legible artifact.

## D-008 (2026-07-08) — Language normalization + periodical flag
**Context:** Extraction sessions drifted on compound-language naming ("Bilingual
(Arabic Urdu)" in Q2/Q3 vs "Bilingual (Arabic and Urdu)" in Q4) and on periodicals
(Q2/Q3 recorded lang as "Periodicals Urdu"; Q4 recorded lang "Urdu" with
PERIODICALS section).
**Decision:** (a) Add `lang` table to aliases.json; canonical compound form is
"Bilingual (X and Y)" / "Trilingual (X, Y and Z)" preserving the catalog's language
order. (b) `norm_lang` folds "Periodicals X" → "X"; periodical status becomes a
derived `periodical` column set from the section prefix (PERIODICALS…). (c) Cities
get a deterministic fold (strip accents, collapse whitespace, drop trailing "city")
applied before alias lookup. (d) punjab.db `entries` table is dropped and rebuilt
from all three quarters (schema gains norm_lang, periodical).
**Consequences:** Analyses use norm_lang/norm_pcity; verbatim lang/pcity untouched;
future extraction sessions should still record what the section header says — drift
is now absorbed by the norm layer instead of polluting counts.

## D-009 (2026-07-08) — Slice outputs are file-first, Gephi-compatible
**Decision:** S2/S3 outputs are plain CSVs + markdown summaries under
`analysis/slice_1910/out/`, buildable by re-runnable scripts (no notebook state, no
binary formats). Network CSVs use Gephi-importable node/edge conventions.
**Consequences:** Davis and Thomas can open everything without tooling; scripts are
the documentation of method.

## D-010 (2026-07-08) — Printer entity folds (and deliberate non-folds)
**Context:** The catalog itself alternates between short and full firm names.
**Decision:** Folded as one entity (all same city across all 1910 data):
Rafah-i-'Am (Steam) Press variants → "Rafah-i-Am Steam Press"; "Hindustan Press" →
"Hindustan Steam Press"; "Arya Press" → "Arya Steam Press" (both Lahore only);
"Dipak Rajput Press" → "Dipak Rajput Printing Works"; Rose Bazar spelling variants;
Hamidiya variants. **Not folded:** "Rajput Printing Works" is kept distinct from
"Dipak Rajput Printing Works" — the catalog prints both forms within single pages,
so they are plausibly two firms; revisit with address evidence.
**Consequences:** If Arya Press turns out to be a distinct Amritsar firm in later
years, the alias must become year/city-scoped (would need a rule upgrade).

## D-011 (2026-07-08) — Publisher entity resolution is slice-local, for now
**Context:** First network build exposed publisher variants (quote styles for the
CMG Press; The/Messrs./M. prefixes; Son/Sons; Pokar/Pokhar) that distort top-N
tables. A full publisher entity table is real historical work (hundreds of names,
honorific conventions, firm successions) and exceeds slice scope.
**Decision:** build_network.py applies (a) descriptor trimming (", Book-sellers"
etc.), (b) a small documented PUB_ALIASES dict for observed high-frequency variants,
(c) self-publication detection (author/compiler/translator/editor patterns) reported
as a statistic rather than forced into the entity network. norm_publisher in the DB
remains lightly normalized (aliases.json) — the slice does not rebuild it.
**Consequences:** Publisher counts in slice outputs may differ slightly from raw DB
GROUP BYs; the PUB_ALIASES dict is the seed for the eventual aliases.json publisher
table (post-slice task, PLAN.md §5).

## D-012 (2026-07-08) — Explorer is a single self-contained HTML file
**Context:** Thomas asked for an interactive way to visualize and explore the 1910
findings, shareable with Davis.
**Decision:** `analysis/slice_1910/build_site.py` generates
`out/explore_1910.html`: one file, all data embedded as JSON, vanilla JS (no CDN,
no server, no build chain) — opens by double-click, emailable, archive-stable.
Tabs: overview dashboard, filterable full-record table (verbatim + flags + marginalia
marks per entry), canvas force-layout printer–publisher network, script-market
comparison, curated exhibits (cross-linked to the records), method & caveats.
Rebuild after any DB change with `python build_site.py`.
**Consequences:** File is ~0.7 MB and grows with the corpus; when the dataset spans
multiple years it will need pagination or a real static-site split (revisit then).
Verified functionally in a browser (all tabs, filters, canvases, crosslinks; no
console errors).
**2026-07-09 update:** first force layout pinned nodes in a ring at the canvas
border (repulsion ≫ centering + hard clamping). Rewritten: alpha-cooled simulation
in unbounded space, collision resolution, smoothed auto-fit-to-view instead of
clamping. Network tab gained a type switcher, all graphs computed client-side from
the embedded entries (build_network.py refactored to expose publisher_entity for
reuse): Printer↔Publisher, Printer↔Language (press multilingualism), Printer↔Printer
via shared publishers (cooperation backbone), Author↔Printer. Verified: 0% of drawn
content in border strip; all four types build; slider re-thresholds live.
**2026-07-15 update:** build_site.py generalized to every year in punjab.db (now
1910–1912, 4,502 entries) and writes **out/explore_1910_1912.html** (2.5 MB) — the
published single-year explore_1910.html is no longer overwritten. Changes: quarters
keyed by full ID (1910Q1…1912Q4) everywhere incl. scan-viewer paths and --package;
overview gains a chronological register-over-time strip; printer/publisher tiles and
the script-market table are computed live from the corpus instead of the 1910-slice
CSVs; the relief-fund chart stays as a labelled 1910 panel; four new exhibits
(volume-spanning prior-hand scripture index; Imad-ud-din self-registration 1912Q2;
30,000-copy Sachi Yadgar handbill 1911Q1; the flagged broken-type "New Fashion"
title 1912Q4); method text covers all three annual reg sequences and the API
extraction (D-015). Verified in browser: all tabs, 12-quarter filter, detail panel,
network canvas, market table (41 langs), scan viewer loading 1911 pages. At ~5 more
years the 2.5 MB single file will need the D-012-anticipated pagination/split.

## D-013 (2026-07-08) — Source-page linking: rendered PNGs first, PDF deep-link second
**Context:** Thomas asked whether the explorer can link records directly to the
source page. The bound volume PDF is huge and machine-specific; but the pipeline
already renders every page as `p<printed:03d>_pdf<pdf>.png` with a constant
printed→pdf offset per quarter, and every entry carries both page numbers.
**Decision:** Two mechanisms in the explorer's detail panel: (a) a built-in scan
viewer that loads the page PNG — trying the packaged layout `out/pages/<quarter>/`
first, then the repo-relative pipeline path — with arrow-key paging across the
quarter and click-to-zoom; (b) an "open PDF at p.N" link using `file:///…#page=N`
(volume paths read from the quarter manifests at build time; works in
Chromium/Firefox built-in viewers when the HTML is opened from disk). A
`--package` build flag copies all page PNGs (~145 MB) into out/ so the folder can
be zipped for Davis with scans included. No PDF.js or embedded PDF: the PNGs are
already the pipeline's provenance layer, and a 200 MB+ embedded viewer serves no
one.
**Consequences:** The emailed single file degrades gracefully (viewer explains how
to get scans; PDF link still works on any machine holding the volume at the same
path). The explorer is now a full verification loop: claim → record → flag →
source pixel.

## D-014 (2026-07-09) — 1910Q1 extracted; year 1910 closed as the slice's unit
**Context:** Audit caught that the "1910 complete" claim in the docs was actually
only Apr–Dec (Q2/Q3/Q4 = 1,026 entries); Q1 (Jan–Mar, quarter ending 31 March 1910)
was never extracted. A full year is the source's natural closed unit (D-007), so Q1
was in scope. Q1 sits at the **front of the 1910-1912 volume** (SV_412_44_1910-1912.pdf),
not the earlier-assumed 1908-1910 volume: doc index 2–51 = printed pp. 2–51, **offset 0**
(the manifest's `pdf_page` is PyMuPDF's 0-indexed `doc[]` index, so render.py's `doc[i]`
and the ad-hoc `doc[pdfpg-1]` recon differ by one — reconciled against render.py + the
Q2 manifest). A prior-hand multi-part index verso (an earlier annotator's, not Davis's
— see D-005) is at doc[52].
**Decision:** Extracted all 50 content pages in-session (382 entries), plus the verso
as an empty record with its prior-hand index documented separately in
`data/1910Q1/marginalia_p052.md`. Validation queue = 16 items, all **catalog
artifacts, not transcription errors**: 2 genuine reg collisions (249 at p28-s12/p42-s15;
306 at p38-s8/p49-s6), a printed **reg 570** far out of the 1–383 range (p18 s67), a
30,000-copy outlier (a real mass-issue army musketry form, p2 s1), a printed 1→3 serial
jump (p22 Sindhi Fiction), and cross-section serial resets where the topic-grouping
lumps books + periodicals (Urdu-Law, Urdu-Misc). Per the standing rule, all are
**flagged, not silently corrected**; the three plausible-misread suspects (30000, 570,
326-dup) were each re-checked against the page image and confirmed faithful to the
original before being left in the queue.
**Consequences:** Year 1910 is now genuinely complete — 1,408 entries, 1,960,018
copies, closing the annual registration sequence (regs ~1–1410 with a few
collisions/gaps). Q1 is the 2nd-largest quarter by entries (382, after Q3's 423).
The vertical slice (D-007) now runs on a real full year; the slice scripts
(build_network / script_market / build_site / davis_memo) must be re-run to fold Q1 in.
Next extraction target is 1911 (same volume, 1911Q1 at PDF 195).

## D-015 (2026-07-15) — API extraction model: Opus 4.8, chosen by bake-off
**Context:** Scaling beyond 1910 requires the Batch API backend (extract_api.py). Three
models were run over the full 1910Q2 golden quarter (42 pages, 321 entries) and diffed
against the validated in-session extractions (analysis/bakeoff_1910Q2.py; scratch
outputs in pipeline/data/1910Q2/_baked/, gitignored).
**Decision:** claude-opus-4-8, Batch API, prompt-cached schema, thinking disabled.
Evidence: Opus reg recall 1.000 / serial 0.987 / weighted 0.916 at $1.95 per quarter
(batch); Haiku 4.5 was 5.6x cheaper but missed or misread 1 in 4 registration numbers
(reg recall 0.755) and dropped 69 of 321 entries — reg/serial/copies are the validation
instruments, so the cheap model is a false economy. Sonnet 5's batch was still queued
at decision time; Opus left little headroom regardless. Output-token compression
(shortened JSON keys) was considered and rejected: ~1/3 cost saving (~$27/decade) does
not justify a remapping layer inside the fidelity-critical path.
**Consequences:** Decade-scale cost ~$2/quarter (~$80 for the 1910s). Cost is ~83%
output tokens; the golden 1910Q2 extractions remain the regression oracle for any
future model/prompt change. Exact-match caveat: even Opus scores low on title/price
against gold due to diacritic/punctuation variance — bake-off numbers are strict
exact-match, relative comparison is what was decided on.

## D-016 (2026-08-04) — Render DPI is a per-purpose choice; 140 destroys the Perso-Arabic diacritics
**Context:** Native-script title capture has been a standing non-goal (PLAN §6). Before
choosing any recogniser, the question is whether the orthography is physically present
in our images at all. The scans are 2526x4163 (~306 DPI); `render.py` renders at 140.
**Measurement (E0, analysis/ocr_lab/):** for naskh the binding feature is not x-height
but the i'jam — the dots that alone separate b/t/th/p/n/y. Measured over 19 Perso-Arabic
titles they are **9.8 mil** across with **10.6 mil** clearance to the letter body. That
is 1.4 px and 1.6 px at 140 DPI — *below the 2 px sampling floor*, so dot identity is not
merely degraded but unrepresentable; 3.0 px and 3.5 px at 306 DPI, i.e. recoverable with
little margin. A blind read of 140 DPI crops produced the predicted error mode exactly:
bodies right, dots wrong in 4 of 8 cases (Zindagi->Zadgi, Regimental->Rehmandal,
Kawwa->Akwa). The 306 render is not empty magnification — its RMS difference from an
upsampled 140 render is 38% of the crop's own contrast.
**Decision:** DPI becomes a per-purpose parameter, not a global constant. Whole-page VLM
extraction stays at 140 (token cost scales with pixels and the roman text survives
comfortably); **any native-script work renders crops at the scans' native 306 DPI**.
**Consequences:** `pipeline/crops.py` defaults to 306. Re-imaging the volumes at 600 DPI
is now a costable decision rather than a wish — it would take the i'jam from 3.0 px to
5.9 px. No existing extraction is invalidated: the romanized fields the corpus is built
on were never at risk.

## D-017 (2026-08-04) — Image-to-record joins must not use database row order
**Context:** The first crop set paired every crop with the wrong register record.
**Cause:** the database returns a page's entries grouped by section rather than in page
order. Printed page 31 of 1910Q2 reads serials 73-79 (Miscellaneous) and then 1-3
(Philosophy); the database returns 1, 2, 3, 73-79. Zipping detected entries against that
order fails silently — every field is plausible, merely attached to the wrong image. It
surfaced only because a montage showed *Gainda* where the index claimed *Vedanta
Philosophy*.
**Decision:** page order comes from the per-page extraction JSONs
(`pipeline/data/<quarter>/extractions/pNNN.json`), which preserve it. Any join between
images and records validates its alignment against an independent count from the
register and drops pages that disagree, rather than assuming.
**Consequences:** 32 of 42 pages of 1910Q2 align exactly and are used; the 10 failures
are listed in the crop-set manifest. The same rule applies to the marginalia workstream
and to any future native-script pass.

## D-018 (2026-08-04) — Constraint checking is triage, not certification; and re-open the model choice
**Context:** E3 asked whether the register's own grammar can flag extraction errors
without a gold standard — the prerequisite for certifying the ~40 quarters of the
1867-1942 run that will never have a validated transcription. `pipeline/constraints.py`
implements the checks; it was scored against the 1910Q2 bake-off, where every candidate
entry has a known error label, and then run over all twelve ingested quarters.
**Findings:** (1) The serial advances by exactly +1 on all 237 within-section transitions
in gold — the strongest constraint the document offers. (2) At the **entry** level a hard
violation gives **~7x lift** (precision 0.56 vs 0.08 base) at 33% recall while flagging
~5% of entries: a good way to order an adjudication queue. (3) At the **quarter** level
it ordered the three bake-off models correctly but with heavily compressed scale (an 8x
error gap appears as a 1.8x violation gap), and n=3 is not evidence on its own. (4) The
errors are not where the grammar looks: Opus makes **zero reg errors in 321 entries**
while making 20 printer-name errors. What carried the result was the **lexicon** check —
a name occurring once within two edits of a name occurring 26 times is the frequent one,
misread. It took entry lift from 2.8x to 6.7x. For this corpus, redundancy of entities
beats redundancy of sequence. (5) Constraints are **complementary to the extractor's own
flags**: corpus-wide, 172 entries carry a hard violation and 789 carry a flag, but only
71 carry both — ~100 entries surface that nothing else would have shown.
**Decision:** constraint output **ranks the adjudication queue** (on the union with the
extractor's flags) and serves as a **regression alarm**; it does not certify a quarter.
OCR_RESEARCH_AGENDA §8.2 corrected accordingly. Quality estimation at scale still needs
a gold standard.
**Consequences:**
(a) **1911-1912 need a look before 1913 is ingested.** The four in-session 1910 quarters
run at 0.7-1.6 hard violations per 100 entries; the eight API-batch quarters run at
2.7-9.0 — 3 to 8 times higher, concentrated in serial_step, missing_serial and
printer_near_duplicate. Read as an alarm, not as a measured error rate.
(b) The checker's hard violations on the *golden* 1910Q2 extraction are all genuine
variants (Mufid-i-Am/Mufid-i-'Am, Waziri-Hind/Wazir-i-Hind, Badar/Badr, Mac Key/MacKey,
Rafah-i-'Am/Rafah-i-Am) — normalization debt for aliases.json.
(c) **D-015 should be revisited.** Sonnet 5's batch arrived after that decision; scored
on the register's validation fields it makes 18 key-field errors to Opus's 27 (the gap is
almost entirely printer names, 8 vs 20) at roughly a third of the price. Opus keeps a
one-error edge on reg, the field D-015 weighted most heavily — not an automatic reversal,
but re-run bakeoff_1910Q2.py before 1913-1915, where the choice compounds.
(d) Cross-run field comparison must fold diacritics, thousands separators, trailing
points and case, and must exclude price/publisher/title/edition/date, whose differences
are transcription convention rather than error — scoring them reported a 28% error rate
for a model whose real key-field rate is 8.4%.
(e) **Schema gap:** nothing in schema.md marks an entry as a title continuation (the
long-dash anaphor), so an empty title cannot be distinguished from an omission without
inference. Worth a field at the next schema revision.

## D-019 (2026-08-04) — The flag queue is a source-anomaly register, not an error backlog
**Context:** PLAN §5 treats ~986 flagged items as an adjudication backlog, and every
HITL design in OCR_RESEARCH_AGENDA assumes flags are worth a historian's time. E5
measured the channel two ways: on the 1910Q2 bake-off, where gold supplies error labels
(E5a, no human needed), and by adjudicating a stratified sample of flags on the *golden*
extraction against native-resolution crops (E5b).
**Findings:** (1) On the bake-off, a flag predicts a key-field error with precision
0.13 and recall 0.30-0.56 (lift 1.6-2.3x) — high recall, low precision, the mirror image
of constraint violations (precision ~0.5, recall 0.33). (2) The two channels are nearly
disjoint (Jaccard 0.05-0.11): their **intersection** has precision 0.67-1.00, their
**union** recall 0.48-0.67. (3) Field-level flags are sharp where they fire — serial 16-20x
lift, copies 36x, pcity 160x — but a flag on **reg has precision 0.00** for both good
models: they flag registration numbers they then read correctly. (4) On the golden
extraction, **0 of 17 adjudicated flags were transcription errors** (95% upper bound
~16%). 53% were catalog artifacts (reg collisions, 'intique' set in the source, 1318
Hijri against a 1910 date, a printer with no city, a missing edition ordinal, no price
line), 35% genuinely degraded source, 12% merely unusual-but-legible values.
**Decision:** the flag queue is **split at intake** into "check my reading" (~35%,
a human re-reads pixels) and "the source is odd" (~65%, a finding to preserve). An
artifact verdict is a **property of the document**, recorded permanently beside the
record — not a task to be closed. Adjudication is prioritised as: flag AND violation
first, then violation only, then flag only. Reg-only flags are deprioritised.
**Consequences:** (a) The queue is smaller than it looks — projected over the corpus,
~350 items want human eyes, ~640 are findings. (b) This strengthens the case for the
claims model (agenda §7.1) before 1913 is ingested: artifacts need somewhere permanent
to live. (c) Flags are well calibrated to their own semantics ("this is hard or odd")
and were being read as something else ("this is wrong"); use constraint violations when
the question is specifically where an extraction is wrong. (d) **Verbatim-layer breach
to sweep for:** 1910Q2 p20 reg 504 — the page prints 'Stara-i-Hiud Press', the flag says
so, but the stored value was silently normalised to 'Stara-i-Hind'. Check for other
cases where a flag documents a correction the value already absorbed. (e) Any
adjudication interface must render the **whole row including column 6**; the first
worksheet cropped columns 2-5 and silently dropped every copyright flag, the fourth most
flagged field in the corpus.

## D-020 (2026-09-02) — The verbatim sweep is closed; `copies` needs a normalised twin
**Context:** D-019(d) recorded one case where the stored value silently absorbed a
correction and asked for a sweep. `analysis/integrity/verbatim_sweep.py` performs it,
using the extractor's own flags as an independent witness of what each page prints:
where a flag quotes a page reading, compare it against the value stored for that field.
Full results in `analysis/integrity/INTEGRITY_SWEEP.md`.
**Findings:** (1) 132 flag-quotes report a page reading; 60 diverge from the store. 49 of
those are in fields the schema declares normalised (`copies` digits-only, `date` ISO-ish,
`edition` ordinal, `method`/`format` controlled) — the flag records broken type in the
source and the store holds the specified normalisation. **Not breaches; the schema
working.** (2) Of the 11 candidates in declared-verbatim fields, 5 are genuine: the
Stara-i-Hiud case from D-019(d), now confirmed the **only** silent spelling correction in
the corpus, plus **4 prices where a printed `"Price,"` label was replaced by a fabricated
`"Rs."`**. That is a correctness defect as well as a fidelity one — an anna is 1/16 of a
rupee, so `"Rs. 4 annas"` is not a well-formed price. Contained: the incoherent form
occurs exactly 4 times in 4,374 priced entries, and they are precisely these four.
(3) One extraction error found running the other way: 1911Q3 p12 s177 reg 917 stores
`pubcity = "Pesh, war"` where the page prints *Peshawar*. (4) **Separately and more
seriously:** `copies` is specified "digits only" but **573 of 4,502 values (12.7%) carry
a thousands separator** in a TEXT column, so `sum(cast(copies as integer))` returns
5,784,297 against a true 6,944,051 — a silent **16.7% undercount** from the query a
person writes first, with no error raised. `build_site.py` strips non-digits and is
unaffected; the discrepancy surfaced only because a draft document and the live site
disagreed.
**Decision:** (a) the verbatim guarantee is stated as "intact with five known exceptions,
each identified by quarter/page/serial/reg" — **never as "preserved exactly" unqualified**;
(b) the five are NOT corrected here (PLAN §6: correction is a separate adjudication pass)
but are no longer unknown; (c) `postprocess.py` should emit an integer `copies` column and
retain the printed form as `copies_verbatim`, so the fix does not come out of the verbatim
layer; (d) until then any published data dictionary carries the `replace()` idiom;
(e) `verbatim_sweep.py` runs after every year is ingested.
**Consequences:** (a) Three-layer discipline was applied to spelling and never to number
formatting — `copies` serves verbatim and analytic duty in one column. Audit the other
TEXT-typed numeric fields for the same shape. (b) README now carries both traps on the
front page; a user of the open data cannot reach either from the schema alone.

## D-021 (2026-09-10) — Pipeline infrastructure runs entirely on GitHub Actions + free-tier cloud services
**Context:** The PDF-to-image-to-extraction pipeline (`scripts/process_pcloud.py`,
`scripts/extract_with_llm.py`) needed a compute and coordination layer with no
infrastructure budget and no ops staffing.
**Decision:** No dedicated server. GitHub Actions runners are the entire compute layer
(both pipeline stages are `workflow_dispatch` workflows — the CI/CD system *is* the
production pipeline, not a thing it deploys); Backblaze B2 is object storage; Supabase
Postgres is the single source of truth and doubles as the coordination mechanism for
parallel workers (`FOR UPDATE SKIP LOCKED` + conditional upsert claiming, and a
precomputed static work-chunking split, depending on whether the unit of work needs live
contention-resolution); Ollama runs self-hosted on the runner itself for LLM inference
rather than a paid API. Full reasoning, alternatives considered, and tradeoffs in
`ARCHITECTURE.md`.
**Consequences:** Near-zero marginal infra cost and nothing to patch, at the cost of free
external quotas becoming the real operational ceiling. Observed directly on 2026-09-10: both
configured B2 accounts hit `AccessDenied: ... download bandwidth or transaction (Class B) cap
exceeded` (the message names two distinct quotas without saying which tripped), producing a
22,387 of 22,398 rows (99.95%) sitting in `failed` status at export time (6 `claimed` were
still unresolved, not themselves failures, and 5 had reached `success`) across ~11 hours and
three separate runs — the claiming/retry logic itself worked correctly
throughout; the external quota was the entire bottleneck. See `ARCHITECTURE.md` for the full
incident and the "when to revisit" triggers.
