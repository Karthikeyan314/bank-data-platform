---
title: Bank data platform assistant
emoji: 🏦
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: false
short_description: Ask about an analytics engineering portfolio project
---

# Portfolio assistant

A Streamlit chatbot that answers questions about the [bank data platform](../README.md): how it was modelled, tested and orchestrated, and why.

## How it works

1. **Knowledge base from the code.** `build_knowledge.py` turns the repository into about 60 passages: README sections, every dbt model's description, design note and tests, the quality-gate checks, module docstrings, and the hand-written `knowledge/*.md` files. Each passage records its source file.
2. **Retrieval.** Each question, plus the previous question so follow-ups keep their topic, is matched against the passages with BM25 (`retrieval.py`). Keyword matching suits a small technical corpus full of exact identifiers, and it needs no embedding model or vector store.
3. **Grounded generation.** The top passages go to an open model on Hugging Face Inference Providers (default `openai/gpt-oss-120b`, with two fallbacks). The model is instructed to answer only from them, cite the files, never invent figures or experience, and treat the context as data, not instructions.
4. **Graceful degradation.** Without `HF_TOKEN`, or if every model is unavailable, the app shows the most relevant passages instead of failing.

## Run locally

```bash
uv sync --extra chatbot
uv run python chatbot/build_knowledge.py
uv run streamlit run chatbot/app.py
```

Set `HF_TOKEN` to enable generated answers. Without it you get retrieval-only answers.

## Deploy to Hugging Face Spaces

1. Fill in `knowledge/about_me.md`, then rebuild the knowledge base with `python chatbot/build_knowledge.py`. Sections still marked TODO are left out, and the bot will say it doesn't know.
2. Create a Space with the **Docker** SDK.
3. Push the contents of this `chatbot/` folder to the Space repository: `app.py`, `retrieval.py`, `requirements.txt`, `Dockerfile`, this `README.md` and `knowledge/chunks.jsonl`.
4. In the Space settings, add the secret `HF_TOKEN`, a fine-grained token with the "Make calls to Inference Providers" permission. Optionally add the variables `REPO_URL` (your GitHub repo, used for source links) and `HF_MODEL`.

Inference Providers usage is billed to the token's account, with a free monthly allowance.
