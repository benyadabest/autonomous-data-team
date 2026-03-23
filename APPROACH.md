### Summary

This doc aims to establish core principles for building a Machine Learning model. It isn’t meant to be a comprehensive guide to ML (and so it doesn’t have every evaluation metric listed) and it doesn’t focus on production aspects (how do we deploy & serve the model, monitor it, retrain it, etc). 

**TLDR**: Start simple, measure the right thing, and build end-to-end before going deep. Most gains come from data quality and feature engineering, not model complexity. Don’t introduce extra complexity if the results don’t require it.

### Principles

**1. Frame the problem before touching data**

Define the business problem being solved, what inputs you have, who or what might act on the output. Choose metrics that reflect the real cost structure of the problem. Accuracy alone is often misleading in real-world problems. Do you care about the cost of a False Positive or a False Negative? How many mistakes in predictions are you willing to tolerate? Precision/recall, AUC, calibration curves, or domain-specific metrics are usually more meaningful. Lock these before optimizing anything if you can.

**2. Try to define a "good enough" exit criterion upfront**

Without this, projects can expand indefinitely. What metric value is sufficient to ship? Agree on it before you start improving, so you know when to stop.

**3. Identify your ground truth anchor**

Supervised (some ground truth labels) problems are easier to evaluate than unsupervised (no ground truth) ones. Ideally you can find it early or readily. Understand its biases and how representative it is. In some cases, you may need to invest time in labeling data (e.g., to label abusive messages, to label good recommendations, etc).

**4. Build an understanding of the data before modeling**

Garbage in, garbage out. No amount of sophisticated modeling can save you from bad data quality. 

The best modeling results tend to arise from situations where the modeler understands the data distributions & quality, how the data is being created (and what biases that might entail) and how any system or human downstream of the model might use it. 

How to start characterizing the data: understand class imbalance, distributions, missingness, correlations. It informs baseline design and often reveals problems (e.g., label leakage, train/test distribution shift) that would otherwise blow up later.

**5. Remember that your model will be working on new observations.** 

To get any sense of how a trained model will perform on predicting things in the future, you will need to reserve some data for a “test set”. So at minimum you should shuffle your data and split into train & test sets before doing anything else. You may need to pay attention to how you split if you’re dealing with time (avoid data leakage from the future!), and you may want a validation set too if your modeling approach has many hyper-parameters to tune. 

**6. Establish a naive baseline**

Rule-based, threshold-based, or the simplest possible model. The goal isn't a great result, it’s to create a reference point. If your fancy model can't beat a count threshold, something is wrong (or the fancy model shouldn’t be used). This also forces you to interact with the raw data early, which always surfaces surprises.

**7. Build a thin E2E pipeline first**

Get something running end-to-end on a small sample before optimizing any step. That usually mean data ingestion, preprocessing, model, and some output. This surfaces integration bugs, format mismatches, and other surprises early when they're cheaper to fix. Resist the urge to perfect the model before the pipeline exists.

**8. Iterate on the middle, not the ends**

Once the pipeline is stable, improvement work follows a consistent order: first feature engineering and data quality (higher leverage, lower cost), then model architecture or algorithm selection, then hyperparameter tuning (higher cost, often lower leverage). Most real-world gains come from the first bucket, not the third. Earn the complexity.

**9. Run targeted experiments with one variable at a time**

Each experiment should test one hypothesis. Changing multiple things at once makes it impossible to attribute results. Keep a simple experiment log (even a spreadsheet to start, but ideally use tools like MLFlow) so you can compare cleanly. Track everything you can: code, preprocessing scripts, config files, model checkpoints, etc. If you can't reproduce a result, it didn't happen!

**10. Do error analysis, not just metric aggregation**

After each experiment, look at what's failing, not just the aggregate number. Segment errors by subgroup. This is usually where understanding the business problem, the data generating mechanisms, etc drive the next experiment idea. This also prevents you from over-optimizing on easy cases while ignoring a hard subpopulation.
