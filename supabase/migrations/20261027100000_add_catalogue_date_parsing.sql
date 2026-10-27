-- Date normalization for canonical catalogue entries.
--
--  parse_catalogue_date(text, year_from, year_to)  pure text -> parsed date
--  catalogue_pdf_year_window(source_pdf)            "..._1881-84" -> 1881..1884
--  apply_catalogue_dates(page)                      parse + store on catalogue_entries_canonical
--  infer_catalogue_dates(page)                      fill year of partial dates, resolve
--                                                   "ditto", and estimate a window for
--                                                   undated entries from the quarter, the
--                                                   neighbouring dated entries in the same
--                                                   PDF, or the PDF's year span.
--
-- The as-extracted `date` text is never changed. Derived columns:
--   date_start/date_end  first/last day covered (a month -> its first/last day; a
--                        range -> first day of the range .. last day of the range)
--   date_iso             date_start written at its precision (YYYY-MM-DD / YYYY-MM / YYYY)
--   date_precision       day | month | year | range
--   date_era             Hijri/B.E./Fasli etc. kept as printed
--   date_status          ok | inferred | needs_year | ditto | needs_review | not_a_date | empty
--   date_basis           printed | neighbors | ditto | year_window   (where the date came from)
--   date_est_from/to     estimated window when there is no usable printed date

create type parsed_catalogue_date as (
    date_start date,
    date_end date,
    date_iso text,
    precision text,
    era text,
    status text,
    part_month int,
    part_day int,
    note text
);


create or replace function catalogue_month_num(tok text)
returns int as $$
    SELECT CASE regexp_replace(lower(btrim(tok)), '[.,]+$', '')
        WHEN 'jan' THEN 1 WHEN 'jany' THEN 1 WHEN 'janry' THEN 1 WHEN 'january' THEN 1
        WHEN 'feb' THEN 2 WHEN 'febr' THEN 2 WHEN 'feby' THEN 2 WHEN 'february' THEN 2 WHEN 'febry' THEN 2
        WHEN 'mar' THEN 3 WHEN 'march' THEN 3
        WHEN 'apr' THEN 4 WHEN 'april' THEN 4
        WHEN 'may' THEN 5
        WHEN 'jun' THEN 6 WHEN 'june' THEN 6
        WHEN 'jul' THEN 7 WHEN 'july' THEN 7
        WHEN 'aug' THEN 8 WHEN 'augt' THEN 8 WHEN 'august' THEN 8
        WHEN 'sep' THEN 9 WHEN 'sept' THEN 9 WHEN 'sepr' THEN 9 WHEN 'septr' THEN 9 WHEN 'september' THEN 9
        WHEN 'oct' THEN 10 WHEN 'octr' THEN 10 WHEN 'october' THEN 10
        WHEN 'nov' THEN 11 WHEN 'novr' THEN 11 WHEN 'november' THEN 11
        WHEN 'dec' THEN 12 WHEN 'decr' THEN 12 WHEN 'december' THEN 12
    END
$$ LANGUAGE SQL IMMUTABLE;


create or replace function catalogue_safe_date(y int, m int, d int)
returns date as $$
BEGIN
    RETURN make_date(y, m, d);
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- Two-digit year -> 18yy or 19yy, decided by the PDF's year window.
-- Without a window: 67..99 -> 18yy, 00..66 -> 19yy (caller flags it as a guess).
create or replace function catalogue_resolve_yy(yy int, lo int, hi int)
returns int as $$
DECLARE
    c18 int := 1800 + yy;
    c19 int := 1900 + yy;
    in18 boolean;
    in19 boolean;
BEGIN
    IF lo IS NULL THEN
        RETURN CASE WHEN yy >= 67 THEN c18 ELSE c19 END;
    END IF;
    in18 := c18 BETWEEN lo - 1 AND hi + 1;
    in19 := c19 BETWEEN lo - 1 AND hi + 1;
    IF in18 AND NOT in19 THEN RETURN c18; END IF;
    IF in19 AND NOT in18 THEN RETURN c19; END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


create or replace function catalogue_mk(
    p_start date, p_end date, p_prec text, p_iso_prec text,
    p_status text, p_note text DEFAULT NULL, p_era text DEFAULT NULL
) returns parsed_catalogue_date as $$
DECLARE
    r parsed_catalogue_date;
BEGIN
    r.date_start := p_start;
    r.date_end := p_end;
    r.precision := p_prec;
    r.status := p_status;
    r.note := p_note;
    r.era := p_era;
    IF p_start IS NOT NULL THEN
        r.date_iso := CASE p_iso_prec
            WHEN 'day' THEN to_char(p_start, 'YYYY-MM-DD')
            WHEN 'month' THEN to_char(p_start, 'YYYY-MM')
            ELSE to_char(p_start, 'YYYY')
        END;
    END IF;
    RETURN r;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- Parse one cleaned date string (no surrounding brackets/era). y_default, when
-- given, supplies the year for a day/month with none printed.
create or replace function catalogue_date_core(
    p_s text, p_default_year int, lo int, hi int
) returns parsed_catalogue_date as $$
DECLARE
    s text := p_s;
    m text[];
    r parsed_catalogue_date;
    r2 parsed_catalogue_date;
    y int; mo int; mo2 int; d int; y2 int;
    d1 date; d2 date;
    toks text[];
    t text;
    month_toks int[] := ARRAY[]::int[];
    other_ok boolean := true;
BEGIN
    IF p_default_year IS NOT NULL AND s !~ '\d{4}' THEN
        s := s || ' ' || p_default_year::text;
    END IF;

    -- two ISO dates: "1884-12-27, 1885-03-13" -> range
    m := regexp_match(s, '^(\d{4}-\d{1,2}-\d{1,2})\s*,\s*(\d{4}-\d{1,2}-\d{1,2})$');
    IF m IS NOT NULL THEN
        r  := catalogue_date_core(m[1], NULL, lo, hi);
        r2 := catalogue_date_core(m[2], NULL, lo, hi);
        IF r.date_start IS NOT NULL AND r2.date_end IS NOT NULL THEN
            RETURN catalogue_mk(r.date_start, r2.date_end, 'range', 'day', 'ok', 'two dates');
        END IF;
        RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'unparsed two dates');
    END IF;

    -- YYYY-MM-YYYY (repeated year; month is the real information)
    m := regexp_match(s, '^(\d{4})-(\d{1,2})-(\d{4})$');
    IF m IS NOT NULL AND m[1] = m[3] THEN
        d1 := catalogue_safe_date(m[1]::int, m[2]::int, 1);
        IF d1 IS NOT NULL THEN
            RETURN catalogue_mk(d1, (d1 + interval '1 month - 1 day')::date, 'month', 'month', 'ok', 'repeated year');
        END IF;
    END IF;

    -- ISO full date, also dotted 1891.05.28
    m := regexp_match(s, '^(\d{4})[-.](\d{1,2})[-.](\d{1,2})$');
    IF m IS NOT NULL THEN
        d1 := catalogue_safe_date(m[1]::int, m[2]::int, m[3]::int);
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid date');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok');
    END IF;

    -- ISO year-month (also "1882-11-"), or a two-digit year range "1934-35"
    m := regexp_match(s, '^(\d{4})-(\d{1,2})-?$');
    IF m IS NOT NULL THEN
        IF m[2]::int BETWEEN 1 AND 12 THEN
            d1 := make_date(m[1]::int, m[2]::int, 1);
            RETURN catalogue_mk(d1, (d1 + interval '1 month - 1 day')::date, 'month', 'month', 'ok');
        ELSIF length(m[2]) = 2 THEN
            y2 := (substr(m[1], 1, 2) || m[2])::int;
            IF y2 < m[1]::int THEN y2 := y2 + 100; END IF;
            RETURN catalogue_mk(make_date(m[1]::int, 1, 1), make_date(y2, 12, 31), 'range', 'year', 'ok', 'year range');
        END IF;
        RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid month');
    END IF;

    -- year only
    m := regexp_match(s, '^(\d{4})$');
    IF m IS NOT NULL THEN
        RETURN catalogue_mk(make_date(m[1]::int, 1, 1), make_date(m[1]::int, 12, 31), 'year', 'year', 'ok');
    END IF;

    -- numeric D-M-YYYY / D/M/YYYY
    m := regexp_match(s, '^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$');
    IF m IS NOT NULL THEN
        d1 := catalogue_safe_date(m[3]::int, m[2]::int, m[1]::int);
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid date');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok');
    END IF;

    -- numeric D-M-YY
    m := regexp_match(s, '^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2})$');
    IF m IS NOT NULL THEN
        y := catalogue_resolve_yy(m[3]::int, lo, hi);
        d1 := CASE WHEN y IS NULL THEN NULL ELSE catalogue_safe_date(y, m[2]::int, m[1]::int) END;
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'two-digit year not resolvable');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok',
                            CASE WHEN lo IS NULL THEN 'century guessed' ELSE 'century from pdf' END);
    END IF;

    -- "10th April, 1911" / "11 DEC 1933" / "10th Augt. 1894"
    m := regexp_match(s, '^(\d{1,2})(?:st|nd|rd|th)?\s*([A-Za-z]+)\.?,?\s*(\d{4})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[2]) IS NOT NULL THEN
        d1 := catalogue_safe_date(m[3]::int, catalogue_month_num(m[2]), m[1]::int);
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid day');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok');
    END IF;

    -- "April 11, 1932" / "Aug. 15, 1930"
    m := regexp_match(s, '^([A-Za-z]+)\.?\s*(\d{1,2})(?:st|nd|rd|th)?,?\s*(\d{4})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[1]) IS NOT NULL THEN
        d1 := catalogue_safe_date(m[3]::int, catalogue_month_num(m[1]), m[2]::int);
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid day');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok');
    END IF;

    -- "1908.Dec.4" / "1904. March 25" / "1912 Mar. 23" / "1928-July-15"
    m := regexp_match(s, '^(\d{4})[.\s-]+([A-Za-z]+)\.?[.\s-]*(\d{1,2})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[2]) IS NOT NULL THEN
        d1 := catalogue_safe_date(m[1]::int, catalogue_month_num(m[2]), m[3]::int);
        IF d1 IS NULL THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'invalid day');
        END IF;
        RETURN catalogue_mk(d1, d1, 'day', 'day', 'ok');
    END IF;

    -- "April, 1906" / "April 1870"
    m := regexp_match(s, '^([A-Za-z]+)\.?,?\s*(\d{4})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[1]) IS NOT NULL THEN
        d1 := make_date(m[2]::int, catalogue_month_num(m[1]), 1);
        RETURN catalogue_mk(d1, (d1 + interval '1 month - 1 day')::date, 'month', 'month', 'ok');
    END IF;

    -- month lists / ranges with a year: "April and May 1938", "June to September 1926",
    -- "July, August and September, 1940", "End of March 1902"
    m := regexp_match(s, '^([A-Za-z][A-Za-z.,&\s-]*?)[\s,]*(\d{4})$');
    IF m IS NOT NULL THEN
        toks := regexp_split_to_array(lower(m[1]), '[^a-z]+');
        FOREACH t IN ARRAY toks LOOP
            IF t = '' THEN CONTINUE; END IF;
            IF catalogue_month_num(t) IS NOT NULL THEN
                month_toks := month_toks || catalogue_month_num(t);
            ELSIF t NOT IN ('and', 'to', 'end', 'of', 'the', 'from', 'early', 'late', 'mid', 'middle', 'beginning', 'issued', 'in') THEN
                other_ok := false;
            END IF;
        END LOOP;
        IF other_ok AND array_length(month_toks, 1) >= 1 THEN
            mo := month_toks[1];
            mo2 := month_toks[array_length(month_toks, 1)];
            d1 := make_date(m[2]::int, mo, 1);
            d2 := (make_date(m[2]::int, mo2, 1) + interval '1 month - 1 day')::date;
            IF array_length(month_toks, 1) >= 2 AND d2 >= d1 THEN
                RETURN catalogue_mk(d1, d2, 'range', 'month', 'ok', 'month range');
            END IF;
            RETURN catalogue_mk(d1, (d1 + interval '1 month - 1 day')::date, 'month', 'month', 'ok');
        END IF;
    END IF;

    -- "April 45" (month + two-digit year)
    m := regexp_match(s, '^([A-Za-z]+)\.?\s*(\d{2})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[1]) IS NOT NULL AND m[2]::int > 31 THEN
        y := catalogue_resolve_yy(m[2]::int, lo, hi);
        IF y IS NOT NULL THEN
            d1 := make_date(y, catalogue_month_num(m[1]), 1);
            RETURN catalogue_mk(d1, (d1 + interval '1 month - 1 day')::date, 'month', 'month',
                                CASE WHEN lo IS NULL THEN 'needs_review' ELSE 'ok' END, 'two-digit year');
        END IF;
    END IF;

    -- day + month, no year: "12th April", "Aug. 10", "April 10"
    m := regexp_match(s, '^(\d{1,2})(?:st|nd|rd|th)?\s*([A-Za-z]+)\.?$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[2]) IS NOT NULL THEN
        r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_year', 'no year printed');
        r.part_month := catalogue_month_num(m[2]);
        r.part_day := m[1]::int;
        RETURN r;
    END IF;
    m := regexp_match(s, '^([A-Za-z]+)\.?\s*(\d{1,2})$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[1]) IS NOT NULL AND m[2]::int BETWEEN 1 AND 31 THEN
        r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_year', 'no year printed');
        r.part_month := catalogue_month_num(m[1]);
        r.part_day := m[2]::int;
        RETURN r;
    END IF;
    -- month only: "April", "February"
    m := regexp_match(s, '^([A-Za-z]+)\.?$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[1]) IS NOT NULL THEN
        r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_year', 'month only');
        r.part_month := catalogue_month_num(m[1]);
        RETURN r;
    END IF;
    -- "1st April to end of June" (range, no year): keep the first day/month
    m := regexp_match(s, '^(\d{1,2})(?:st|nd|rd|th)?\s*([A-Za-z]+)\.?\s+to\s+.*$', 'i');
    IF m IS NOT NULL AND catalogue_month_num(m[2]) IS NOT NULL THEN
        r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_year', 'range without year');
        r.part_month := catalogue_month_num(m[2]);
        r.part_day := m[1]::int;
        RETURN r;
    END IF;

    -- ditto / "13th Do." / "1st ditto"
    m := regexp_match(s, '^(?:(\d{1,2})(?:st|nd|rd|th)?\s*)?(?:ditto|do)\.?,?$', 'i');
    IF m IS NOT NULL THEN
        r := catalogue_mk(NULL, NULL, NULL, NULL, 'ditto', 'ditto');
        r.part_day := m[1]::int;
        RETURN r;
    END IF;

    -- Not a date at all: column headers, editions, sizes, notes ...
    IF s !~ '\d' OR s ~* '(edition|\mvo\M|remark|ending|concluded|continued|\mr\.\s*8)' THEN
        RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'not_a_date', 'not a date');
    END IF;
    RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'unparsed');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


create or replace function parse_catalogue_date(
    p_text text, p_year_from int DEFAULT NULL, p_year_to int DEFAULT NULL
) returns parsed_catalogue_date as $$
DECLARE
    s text;
    m text[];
    r parsed_catalogue_date;
    outer_s text;
    inner_s text;
    lead_year int;
    lead_era text;
    era_txt text;
    default_year int;
    rlo int;
    rhi int;
    y1 int;
    y2 int;
BEGIN
    s := btrim(coalesce(p_text, ''));
    IF s = '' OR s ~ '^[\s_\[\]\(\)\.\-—–,]*$' THEN
        r.status := 'empty';
        RETURN r;
    END IF;

    s := regexp_replace(s, '^issued in\s+', '', 'i');
    s := regexp_replace(s, '\s*[—–]+\s*(continued|concluded)\.?$', '', 'i');
    s := btrim(regexp_replace(s, '\s+', ' ', 'g'));

    -- "<outer> [<inner>]" / "<outer> (<inner>)"; an empty outer means the whole value was bracketed
    m := regexp_match(s, '^(.*?)\s*[\(\[]\s*(.*?)\s*[\)\]]\.?$');
    IF m IS NOT NULL THEN
        outer_s := btrim(regexp_replace(m[1], '[\s.,;]+$', ''));
        inner_s := btrim(regexp_replace(m[2], '[\s.,;]+$', ''));
        IF outer_s = '' THEN
            r := catalogue_date_core(inner_s, NULL, p_year_from, p_year_to);
        ELSE
            m := regexp_match(outer_s, '^(\d{4})\s*([A-Za-z.]*)$');
            lead_year := CASE WHEN m IS NOT NULL THEN m[1]::int END;
            lead_era := CASE WHEN m IS NOT NULL THEN nullif(btrim(m[2]), '') END;
            default_year := CASE WHEN lead_year BETWEEN 1800 AND 1950 THEN lead_year END;
            era_txt := nullif(btrim(concat_ws(' ',
                           CASE WHEN lead_year IS NOT NULL AND default_year IS NULL THEN lead_year::text END,
                           lead_era)), '');

            IF inner_s ~* '^(\d{4}\s*)?(hijri|fasli|samvat|b\.?\s?e|b\.?\s?s|a\.?\s?h|h|s)\.?$' THEN
                -- era only in brackets: the date is the outer part, era is the inner
                IF lead_year IS NOT NULL AND default_year IS NULL THEN
                    r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'era date only', outer_s || ' ' || inner_s);
                ELSE
                    r := catalogue_date_core(outer_s, NULL, p_year_from, p_year_to);
                    r.era := inner_s;
                END IF;
            ELSE
                r := catalogue_date_core(inner_s, default_year, p_year_from, p_year_to);
                IF r.date_start IS NOT NULL THEN
                    r.era := era_txt;
                ELSE
                    r := catalogue_date_core(outer_s, NULL, p_year_from, p_year_to);
                END IF;
            END IF;
        END IF;
    ELSE
        s := btrim(regexp_replace(s, '[\s.,;]+$', ''));
        -- "1944 A.D." -> 1944
        s := regexp_replace(s, '^(\d{4})\s*a\.?d$', '\1', 'i');
        -- "1333 Hijri", "1268 B.E.": an era year, not a Gregorian date
        IF s ~* '^\d{4}\s*(hijri|fasli|samvat|b\.?\s?e|b\.?\s?s|a\.?\s?h|h|s)$' THEN
            RETURN catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review', 'era date only', s);
        END IF;
        r := catalogue_date_core(s, NULL, p_year_from, p_year_to);
    END IF;

    -- years must fall in the PDF's window (±1) or, with none, in the register's span
    IF r.date_start IS NOT NULL THEN
        y1 := extract(year FROM r.date_start)::int;
        y2 := extract(year FROM r.date_end)::int;
        rlo := coalesce(p_year_from - 1, 1867);
        rhi := coalesce(p_year_to + 1, 1946);
        IF y1 < rlo OR y2 > rhi THEN
            r := catalogue_mk(NULL, NULL, NULL, NULL, 'needs_review',
                              format('year %s outside %s-%s', y1, rlo, rhi), r.era);
        ELSIF r.note IN ('century guessed') THEN
            r.status := 'needs_review';
        END IF;
    END IF;
    RETURN r;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- "SV_412_44_1881-84" -> (1881, 1884); "..._1881" -> (1881, 1881); otherwise NULLs
create or replace function catalogue_pdf_year_window(p_src text)
returns table (y_from int, y_to int) as $$
DECLARE
    m text[];
    a int;
    b int;
BEGIN
    m := regexp_match(coalesce(p_src, ''), '(\d{4})\s*[-–_]\s*(\d{2,4})\D*$');
    IF m IS NOT NULL THEN
        a := m[1]::int;
        b := m[2]::int;
        IF b < 100 THEN
            b := (a / 100) * 100 + b;
            IF b < a THEN b := b + 100; END IF;
        END IF;
    ELSE
        m := regexp_match(coalesce(p_src, ''), '(\d{4})\D*$');
        IF m IS NOT NULL THEN a := m[1]::int; b := a; END IF;
    END IF;
    IF a BETWEEN 1800 AND 1950 AND b BETWEEN a AND 1950 THEN
        y_from := a; y_to := b;
    END IF;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
