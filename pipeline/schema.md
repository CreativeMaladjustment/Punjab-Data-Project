# Extraction schema — one JSON file per printed page

Each page file is a JSON array of entry objects, in page order. All values are
strings unless noted. Empty string = field absent on the page. **Transcribe what
is printed; do not correct, complete, or infer beyond the stated rules.**

| Field | Meaning / rules |
|---|---|
| `quarter` | e.g. `"1910Q2"` (quarter ending 30 June 1910) |
| `pdf_page` | 0-based index in the source PDF (int) |
| `printed_page` | page number printed on the page (int) |
| `section` | verbatim section header governing the entry, e.g. `"URDU—RELIGION.—concluded."`. Repeat on every entry; carry from previous page when the section continues |
| `lang` | language from the section header, e.g. `"Hindi"` |
| `char` | script qualifier from the header if any, e.g. `"Punjabi characters"`, `"Persian character"` |
| `topic` | topic from the section header, e.g. `"Religion"`, `"Science (Mathematical)"` |
| `serial` | column 1 serial number (int) |
| `reg` | registration number (column 5) |
| `copies` | number of copies (column 4), digits only |
| `printer_verbatim` | printer + place as printed, incl. `"Ditto"` |
| `printer` | printer name with Ditto resolved (still verbatim spelling) |
| `pcity` | printer's city, Ditto resolved |
| `author` | author as printed, incl. honorifics (`"Jiwan Singh, Bhai"`); a role string printed next to the name is kept in parentheses, e.g. `"(tr. ...)"` for a translator or `"(ed. ...)"` for an editor; empty if entry is title-first with no author |
| `title` | romanized title; if only native script + English gloss given, use the bracketed romanization |
| `title_native` | `true` if the entry prints a native-script title (bool) |
| `gloss` | the annotator's English content summary, verbatim, full |
| `pp_verbatim` | pagination exactly as printed, e.g. `"127, 80 and 4"` |
| `publisher` | publishing person/org as printed; `"Author"` if published by the author (keep their role note in parentheses) |
| `pubcity` | publisher's city if printed |
| `date` | publication date ISO-ish (`1910-04-25`, `1910-04` if day absent); append Hijri year in parentheses if printed |
| `price` | verbatim, e.g. `"Rs. 4, 5 annas"`, `"Free"` |
| `edition` | e.g. `"1st"`, `"14th"`, `"2nd, revised"` |
| `format` | size, e.g. `"8°"`, `"16°"` |
| `method` | `"litho."`, `"type"` etc. if printed |
| `educ` | `"Y"` if "(Designed for educational purposes.)" governs the entry |
| `copyright` | column 6 verbatim if present |
| `notes` | cross-references and bracketed catalog notes verbatim ("Previous edition noticed …") |
| `marks` | non-print marks on the entry: `"X"` pencil cross, stamps, etc. |
| `flags` | array of `{"field": ..., "issue": ...}` for uncertain readings (degraded digits, ambiguous letters). Use aggressively |

Conventions: numbers keep printed value (do not sum pagination — postprocess
does that); `(Designed for educational purposes.)` between entries governs the
following entry/entries under that heading; a header `X—Y.—concluded.` means
lang/topic continue from the previous page's section. Page layout varies by
era: earlier quarters print each entry as a paragraph; later quarters
increasingly print entries as rows of an actual multi-column table, with
columns roughly matching the `serial`/`reg`/`copies`/`copyright` "column N"
references above. Extract one entry per paragraph or per table row either
way — the fields mean the same thing regardless of which layout a given
page uses.
