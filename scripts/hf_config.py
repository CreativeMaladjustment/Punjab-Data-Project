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
"""
DEFAULT_HF_MODEL = "Qwen/Qwen2.5-VL-7B-Instruct"
