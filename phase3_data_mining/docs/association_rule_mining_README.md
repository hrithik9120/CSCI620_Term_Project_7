# Association Rule Mining - Reddit Comments Dataset

## Overview

This document describes the association rule mining implementation for the Reddit Comments May 2015 dataset. The program discovers interesting patterns and relationships in comment data using frequent itemset mining and association rule generation.

## 1. Transaction Definition

### How Transactions Are Defined

**Each transaction represents one Reddit comment with its characteristics.**

A transaction is a collection of items that describe a comment:
- **Subreddit**: The subreddit where the comment was posted (e.g., "subreddit:AskReddit", "subreddit:nfl")
- **Score Category**: Categorized based on the comment's score:
  - `very_high_score`: Score > 50 (top 1% of comments)
  - `high_score`: Score > 20 (top 5% of comments)
  - `medium_score`: Score > 5 (above average)
  - `low_score`: Score > 0 (positive but low)
  - `negative_score`: Score ≤ 0 (downvoted comments)
- **Status Flags**: Binary indicators for special comment characteristics:
  - `gilded`: Comment received Reddit gold
  - `controversial`: Comment marked as controversial
  - `edited`: Comment was edited after posting
  - `distinguished`: Comment distinguished by moderator/admin
  - `archived`: Post/comment is archived

### Example Transaction

```
["subreddit:AskReddit", "low_score", "edited"]
```

This transaction represents a comment in AskReddit subreddit with a low score that was edited.

## 2. Parameter Determination

### Minimum Support (`min_support`)

**Default: 0.01 (1%)**

**Rationale:**
- An itemset must appear in at least 1% of transactions to be considered frequent
- This threshold balances:
  - **Too low (< 0.5%)**: Finds too many rare combinations (noise)
  - **Too high (> 5%)**: Misses meaningful but less common patterns
- 1% ensures we find patterns that occur regularly enough to be meaningful
- For testing with smaller samples, we use 0.03 (3%) to get more results

**How it was determined:**
1. Started with standard threshold of 1% (common in literature)
2. Tested with sample data to see number of itemsets generated
3. Adjusted to 3% for sample runs to get reasonable number of results
4. For full dataset, 1% would be appropriate

### Minimum Confidence (`min_confidence`)

**Default: 0.5 (50%)**

**Rationale:**
- A rule must have at least 50% confidence to be considered useful
- Confidence = P(consequent | antecedent) = how often the consequent appears when antecedent is present
- 50% threshold ensures:
  - Rules have meaningful predictive power
  - Filters out weak associations
  - Focuses on rules that are likely to hold

**How it was determined:**
1. Standard threshold in association rule mining is 50-70%
2. Lower threshold (30-40%) produces too many weak rules
3. Higher threshold (80%+) produces too few rules
4. 50% provides good balance between coverage and quality

### Score Category Thresholds

**Rationale:**
- Based on analysis of Reddit's score distribution
- Most comments have low scores (0-5)
- High scores (>20) are relatively rare
- Categories chosen to capture meaningful differences in comment popularity

**Thresholds:**
- `very_high_score`: >50 (top 1% - exceptional comments)
- `high_score`: >20 (top 5% - highly upvoted)
- `medium_score`: >5 (above average)
- `low_score`: >0 (positive but low)
- `negative_score`: ≤0 (downvoted)

## 3. Results Explanation

### Frequent Itemsets

Frequent itemsets are combinations of items that appear together frequently in transactions.

**Example:**
- Itemset: `["subreddit:nfl", "low_score"]`
- Support: 0.0709 (7.09% of transactions)
- Meaning: 7.09% of comments are in NFL subreddit AND have low scores

### Association Rules

Association rules show IF-THEN relationships with the following metrics:

#### Support
- **Definition**: Frequency of the itemset (antecedent + consequent) in all transactions
- **Example**: Support = 0.0709 means the pattern appears in 7.09% of transactions
- **Interpretation**: Higher support = more common pattern

#### Confidence
- **Definition**: P(consequent | antecedent) = probability of consequent given antecedent
- **Example**: Confidence = 0.9232 means 92.32% of the time when antecedent is true, consequent is also true
- **Interpretation**: Higher confidence = stronger predictive power

#### Lift
- **Definition**: How much more likely the consequent is given the antecedent vs. overall
- **Formula**: Lift = P(consequent | antecedent) / P(consequent)
- **Interpretation**:
  - **Lift > 1.0**: Positive association (rule is useful)
  - **Lift = 1.0**: No association (independent)
  - **Lift < 1.0**: Negative association (less likely together)

#### Conviction
- **Definition**: Expected error of the rule
- **Interpretation**: Higher conviction = stronger rule (less likely to be wrong)

### Example Rule Interpretation

**Rule**: `IF subreddit:nfl THEN low_score`
- **Support**: 7.09% - This pattern appears in 7.09% of all comments
- **Confidence**: 92.32% - When a comment is in NFL subreddit, 92.32% of the time it has a low score
- **Lift**: 1.1639 - Comments in NFL subreddit are 16.39% more likely to have low scores than average
- **Interpretation**: There is a positive association between NFL subreddit and low scores

## 4. Applications of Results

The discovered association rules can be applied to:

### 1. Content Recommendation
- **Use**: Suggest subreddits to users based on their comment patterns
- **Example**: If a user frequently comments in subreddits with high scores, recommend similar subreddits

### 2. Moderation
- **Use**: Identify patterns that correlate with controversial or problematic content
- **Example**: Rules showing relationships between subreddits and controversial flags can help prioritize moderation efforts

### 3. User Engagement
- **Use**: Understand factors that lead to high-scoring comments
- **Example**: Rules showing which subreddits or characteristics lead to high scores can guide content strategy

### 4. Community Analysis
- **Use**: Discover relationships between subreddits and comment characteristics
- **Example**: Understanding which subreddits tend to have edited comments, gilded comments, etc.

### 5. Quality Prediction
- **Use**: Predict comment success based on subreddit and characteristics
- **Example**: Use rules to predict which comments are likely to receive high scores

## Program Steps Summary

1. **Load Data**: Read comments from SQLite database
2. **Create Transactions**: Transform each comment into a transaction with items
3. **Encode Transactions**: Convert to binary matrix format (required for Apriori)
4. **Mine Frequent Itemsets**: Apply Apriori algorithm to find frequent patterns
5. **Generate Rules**: Create association rules from frequent itemsets
6. **Calculate Metrics**: Compute support, confidence, lift, and conviction
7. **Filter & Rank**: Sort rules by confidence and lift
8. **Report Results**: Display top rules and save detailed results

## Usage

```bash
# Basic usage
python association_rule_mining.py --input database.sqlite

# With custom parameters
python association_rule_mining.py \
    --input database.sqlite \
    --sample 10000 \
    --min-support 0.03 \
    --min-confidence 0.5
```

## Output Files

- **Console Output**: Top association rules with metrics
- **association_rules_results.txt**: Detailed results including:
  - All frequent itemsets
  - All association rules with metrics
  - Applications section
  - Model comparison

