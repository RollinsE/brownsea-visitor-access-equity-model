# The AI assistant

The hosted app has an "Ask about the analysis" box. This page covers how the assistant works, how to configure it and how it's evaluated. For the project itself, see the [README](README.md).

## How it works

The assistant is an LLM agent built on Google's Gemini API. It doesn't answer from memory. For each question it picks from a small set of read-only tools, the tools look up the published results, and the model writes its answer from what comes back.

| Tool | Reads |
|---|---|
| `lookup_postcode` | the postcode lookup |
| `get_district`, `query_districts`, `aggregate_districts` | the district table |
| `get_model_performance` | the model comparison |
| `get_definitions` | the priority zone and need tier definitions |
| `search_documentation` | the help page, the model report notes and the README |

* Counts, totals and averages are worked out in Python by the tools, not by the model.
* The tools only read the published postcode and district outputs. The assistant never sees visitor or member records.
* Each answer lists which data it used.

## Searching the documentation

Questions about numbers go to the data tools. Questions about meaning or method, such as "how was the model tested?", are answered with retrieval-augmented generation (RAG): `search_documentation` finds the most relevant passages in the help page, the model report notes and parts of the README, and the assistant writes its answer from them.

The text is split into short passages, and each question is matched against them in two ways: by keyword (BM25) and by embedding similarity, using Gemini's embedding model. The two rankings are combined with reciprocal rank fusion. Keywords alone miss questions worded differently from the text, such as "tested" when the passage says "cross-validation". If the embedding call fails, search falls back to keywords.

## Settings

Gemini's free tier has low daily limits per model, so if one model is over its limit or busy, the assistant moves on to the next one in the list.

| Setting | Default | Purpose |
|---|---|---|
| `GEMINI_API_KEY` | none | Turns the assistant on |
| `BROWNSEA_AGENT_MODEL` | `gemini-3.8-flash` | First model to try |
| `BROWNSEA_AGENT_FALLBACK_MODELS` | `gemini-3.7-flash,gemini-3.6-flash,gemini-3.5-flash,gemini-3.5-flash-lite` | Models to fall back to, in order |
| `BROWNSEA_AGENT_RATE_PER_MIN` | `10` | Questions allowed per client per minute |
| `BROWNSEA_EMBEDDINGS` | `1` | Set to `0` to search the documentation by keyword only |
| `BROWNSEA_EMBEDDING_MODEL` | `gemini-embedding-001` | Embedding model for documentation search |
| `BROWNSEA_JUDGE_MODEL` | the first agent model | Model used as the judge in the evaluation |

## Evaluation

`scripts/eval_agent.py` asks the assistant 18 fixed questions: questions about the data, questions about terms and method, and questions it should decline or say it can't answer. Each answer is checked in two ways.

* **Exact checks.** Where a question has one right answer, such as a count or a district, the script works it out from the district table and looks for it in the reply.
* **LLM as a judge.** A second model lists every factual claim in the answer and checks each one against the assistant's tool results and the short background on the analysis it's given. Pass or fail is decided in code, and any unsupported claim is a fail. The judge also checks that off-topic questions are declined, and that questions the data can't answer (member numbers, past years) get "I can't answer that" rather than a guess.

A judge can get things wrong too, so I tested it before relying on its scores. `evals/judge_check.jsonl` holds 18 answers paired with real tool results. Fifteen I wrote by hand, about half with a planted mistake: a wrong number, a wrong district, an invented reason, or a made-up reply to an off-topic question. The other three are answers the assistant gave in evaluation runs, two wrong and one right. `--check-judge` reports how many bad answers the judge let through and how many good ones it failed. It also helps to use a different model as the judge (`BROWNSEA_JUDGE_MODEL`), since a model tends to go easy on its own writing.

Each run is saved with the tool results, so the answers can be judged again later with `--rejudge` without asking the assistant again.

```bash
python scripts/eval_agent.py                 # ask the questions, check and judge the answers
python scripts/eval_agent.py --check-judge   # test the judge against the labelled answers
```

**Latest results (27 November 2026)**

* The judge agreed with all 18 labelled answers in the check set.
* All 9 exact checks passed.
* None of the 18 answers contained an unsupported claim.
* The judge passed 17 of the 18. The one it failed was "What was the visit rate for BH1 in 2019?". The assistant said there are no figures by year and then gave BH1's current rate. That's a reasonable reply, but the judge didn't count it as declining the question.

Most of the judging was done by `gemini-3.5-flash-lite`, as the other free models were busy at the time.
