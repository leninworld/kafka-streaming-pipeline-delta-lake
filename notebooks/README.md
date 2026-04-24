# Notebooks Problem Map

This file maps each real-world problem to the notebook that implements it, with current status and what is covered.

## 1) Uplift: Did campaign actually work?

- Problem: Which users convert because of campaign treatment?
- Notebook links:
  - Source: [telco_customer_churn_marketing.ipynb](./telco_customer_churn_marketing.ipynb)
  - Executed: [telco_customer_churn_marketing.executed.ipynb](./telco_customer_churn_marketing.executed.ipynb)
- Dataset used: Hillstrom Email Campaign (in notebook flow)
- Status: Done
- Implemented:
  - T-learner
  - X-learner
  - Qini curve
  - Decile analysis
- Why it matters: Measures incremental impact (not just conversion likelihood).

## 2) Signal Loss / Data Quality Impact: Why is measurement broken?

- Problem: How much performance drops when important signals are missing.
- Notebook links:
  - Primary source: [identity_resolution.ipynb](./identity_resolution.ipynb)
  - Primary executed: [identity_resolution.executed.ipynb](./identity_resolution.executed.ipynb)
  - Secondary source: [telco_customer_churn_marketing.ipynb](./telco_customer_churn_marketing.ipynb)
  - Secondary executed: [telco_customer_churn_marketing.executed.ipynb](./telco_customer_churn_marketing.executed.ipynb)
- Status: Done
- Implemented:
  - Artificially reduced signal completeness
  - Performance comparison under missing features
  - Feature completeness vs match-quality style analysis
- Why it matters: Mirrors privacy/signal-loss realities (cookie loss, ID constraints).

## 3) Budget Allocation Optimization: Where should I spend?

- Problem: Maximize ROI under budget constraint.
- Notebook links:
  - Source: [marketing_attribution.ipynb](./marketing_attribution.ipynb)
  - Executed: [marketing_attribution.executed.ipynb](./marketing_attribution.executed.ipynb)
- Status: Done
- Implemented:
  - Attribution-style channel contribution
  - ROI estimation by channel
  - Budget allocation recommendation
- Output: Channel spend recommendation based on constrained optimization logic.

## 4) Match Rate Optimization (Identity Quality): Why low match %?

- Problem: Low identity match rate and what drives it.
- Notebook links:
  - Source: [identity_resolution.ipynb](./identity_resolution.ipynb)
  - Executed: [identity_resolution.executed.ipynb](./identity_resolution.executed.ipynb)
- Dataset style: Synthetic identity-resolution records with controlled missingness
- Status: Done
- Implemented:
  - Similarity + classifier linkage flow
  - Match rate analysis
  - Missing-feature impact / completeness sensitivity
- Output: Match rate vs feature completeness behavior.

## 5) Cross-Channel User Stitching: How users move across channels?

- Problem: Same user appears across devices/channels and needs unified journey.
- Notebook links:
  - Source: [identity_resolution.ipynb](./identity_resolution.ipynb)
  - Executed: [identity_resolution.executed.ipynb](./identity_resolution.executed.ipynb)
- Status: Done (core stitching flow)
- Implemented:
  - Group records into resolved entities
  - Build unified user view across channels/devices
  - Journey-style outputs from stitched identities
- Output: Multi-touch journey and conversion-path style view from resolved users.

## Quick Notebook Index

- [telco_customer_churn_marketing.ipynb](./telco_customer_churn_marketing.ipynb) | [executed](./telco_customer_churn_marketing.executed.ipynb): churn + uplift + calibration + segmentation
- [marketing_attribution.ipynb](./marketing_attribution.ipynb) | [executed](./marketing_attribution.executed.ipynb): attribution + ROI + budget allocation
- [ctr_calibration.ipynb](./ctr_calibration.ipynb) | [executed](./ctr_calibration.executed.ipynb): CTR modeling + calibration + bidding impact simulation
- [identity_resolution.ipynb](./identity_resolution.ipynb) | [executed](./identity_resolution.executed.ipynb): record linkage + match rate + signal loss + user stitching
- [mlops.ipynb](./mlops.ipynb): experiment tracking, registry, serving, and monitoring concepts
