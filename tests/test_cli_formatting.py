import logging

from pipeline import _stage_done, _stage_start


def test_stage_output_uses_plain_separators_without_symbols(capsys):
    logger = logging.getLogger("test_cli_formatting")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    _stage_start(1, "Data pipeline", logger)
    _stage_done(1, "Data pipeline", logger, rows=10)

    output = capsys.readouterr().out
    assert "STAGE 1: DATA PIPELINE" in output
    assert "Stage 1 complete: Data pipeline: rows=10" in output
    for symbol in ("✓", "✗", "▶", "↻", "—"):
        assert symbol not in output



def test_model_ranking_table_restored_for_cli_output():
    source = __import__("pathlib").Path("src/model_training.py").read_text(encoding="utf-8")

    assert "def format_model_ranking_table" in source
    assert "Model ranking by Mean MAE" in source
    assert "ranking_table = format_model_ranking_table(results_df)" in source
    assert "print(ranking_table)" in source
