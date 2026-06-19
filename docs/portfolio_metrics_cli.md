# Portfolio Metrics Reference

Portfolio metrics are temporarily removed from the active CLI workflow while
the next metrics workflow is being designed. Do not run metrics from any
terminal command until the replacement workflow is ready.

The existing calculation reference remains in `src/portfolio_metrics.py` for
development and comparison work. Tests may continue to cover those pure
calculation helpers while the replacement workflow is being built.

## Reference Metrics

The reference module includes helpers for:

- Position and bucket weights.
- Allocation drift against policy targets.
- Unrealized gains.
- Total return, volatility, max drawdown, and Sharpe ratio.
- Policy limit warnings.
- Contribution allocation recommendations.
