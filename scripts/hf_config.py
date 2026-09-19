"""Single source of truth for the Hugging Face model default, shared
between extract_with_hf.py and list_hf_models.py so the two can't drift
out of sync the way a copy-pasted default in a workflow YAML file could
(a Copilot review finding on PR #57: list-hf-models.yml hardcoded its own
copy of extract-pages-hf.yml's default, silently going stale the next
time the pipeline's model changes).

Deliberately its own tiny, dependency-free module rather than a constant
inside extract_with_hf.py itself: that module does real work at import
time (reading HUGGING_FACE_API_KEY/SUPABASE_DB_URL from the environment,
connecting B2 credentials) that list_hf_models.py has no reason to need
and no configuration to satisfy -- same "duplicate rather than import"
rationale extract_with_hf.py's own docstring gives for not importing
extract_with_llm.py.

Qwen/Qwen2.5-VL-7B-Instruct (the original pick, from third-party
writeups) was replaced after scripts/list_hf_models.py's live test
(run 35446611937) empirically confirmed it fails with this account's key
("not supported by any provider you have enabled") and that
google/gemma-4-31B-it is one of the models that actually works -- the
only one of 6 passing candidates that also returned real, non-empty
message content under the diagnostic's minimal probe (the other 5 --
Qwen/Qwen3.6-27B, Qwen/Qwen3.6-35B-A3B, google/gemma-4-26B-A4B-it,
zai-org/GLM-5.3-Flash, moonshotai/Kimi-K3 -- returned a 200 but a null or
empty content field, likely a reasoning-model quirk under the probe's
5-token budget rather than a real failure; they're still offered as
alternate choices in extract-pages-hf.yml, just not the most-verified
pick for the default)."""
DEFAULT_HF_MODEL = "google/gemma-4-31B-it"
