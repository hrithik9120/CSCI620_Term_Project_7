#!/usr/bin/env python3
"""
Association Rule Mining for Reddit Comments Dataset

This script performs frequent itemset mining and association rule mining
to discover interesting patterns in the Reddit Comments May 2015 dataset.

TRANSACTION DEFINITION:
----------------------
A transaction represents a comment with its characteristics. Each transaction
contains items that describe:
- Subreddit: The subreddit where the comment was posted
- Score category: Categorized score (very_high_score >50, high_score >20, 
  medium_score >5, low_score >0, negative_score <=0)
- Status flags: gilded, controversial, edited, distinguished, archived

PARAMETER DETERMINATION:
------------------------
- min_support: Set to 0.03 (3%) to find itemsets that appear in at least 3% 
  of transactions. This balances finding meaningful patterns while avoiding 
  noise from rare combinations.
- min_confidence: Set to 0.5 (50%) to ensure rules have at least 50% 
  probability. This filters out weak associations.

ALGORITHM:
---------
1. Load data from SQLite database
2. Transform comments into transactions (each comment = one transaction)
3. Encode transactions into binary matrix format
4. Apply Apriori algorithm to find frequent itemsets
5. Generate association rules from frequent itemsets
6. Calculate support, confidence, lift, and conviction metrics
7. Filter and report top rules
"""

import argparse
import sqlite3
import pandas as pd  # type: ignore
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
import sys


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Association Rule Mining for Reddit Comments dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', default='database.sqlite',
                       help='Path to SQLite database file (default: database.sqlite)')
    parser.add_argument('--sample', type=int,
                       help='Analyze only first N rows (for testing)')
    parser.add_argument('--min-support', type=float, default=0.01,
                       help='Minimum support threshold (default: 0.01)')
    parser.add_argument('--min-confidence', type=float, default=0.5,
                       help='Minimum confidence threshold (default: 0.5)')
    return parser.parse_args()


def load_data(sqlite_path, sample_size=None):
    """
    Load data from SQLite database.
    
    Returns:
        DataFrame with Reddit comments data
    """
    print("[*] Loading data from SQLite database...")
    conn = sqlite3.connect(sqlite_path)
    
    query = "SELECT * FROM May2015"
    if sample_size:
        query += f" LIMIT {sample_size}"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"[OK] Loaded {len(df):,} rows")
    return df




def create_transactions(df):
    """
    Create transactions from comment data.
    
    TRANSACTION DEFINITION:
    Each transaction represents one comment with its characteristics:
    - Subreddit name (e.g., "subreddit:AskReddit")
    - Score category based on comment score
    - Status flags (gilded, controversial, edited, distinguished, archived)
    
    PARAMETER RATIONALE:
    - Score categories: Based on Reddit's scoring distribution
      * very_high_score: >50 (top 1% of comments)
      * high_score: >20 (top 5% of comments)
      * medium_score: >5 (above average)
      * low_score: >0 (positive but low)
      * negative_score: <=0 (downvoted)
    
    Returns:
        List of transactions (each transaction is a list of items)
    """
    print("\n[*] Creating transactions from comments...")
    print("    Each transaction = one comment with its characteristics")
    
    transactions = []
    
    for _, row in df.iterrows():
        transaction = []
        
        # Add subreddit
        if pd.notna(row['subreddit']):
            transaction.append(f"subreddit:{row['subreddit']}")
        
        # Add score category (determined by analyzing score distribution)
        score = row['score']
        if score > 50:
            transaction.append("very_high_score")
        elif score > 20:
            transaction.append("high_score")
        elif score > 5:
            transaction.append("medium_score")
        elif score > 0:
            transaction.append("low_score")
        else:
            transaction.append("negative_score")
        
        # Add status flags
        if row['gilded'] > 0:
            transaction.append("gilded")
        
        if row['controversiality'] > 0:
            transaction.append("controversial")
        
        if pd.notna(row['edited']) and row['edited'] != 0:
            transaction.append("edited")
        
        if pd.notna(row['distinguished']) and row['distinguished'] != 'None':
            transaction.append("distinguished")
        
        if row.get('archived', 0) == 1:
            transaction.append("archived")
        
        if len(transaction) > 0:
            transactions.append(transaction)
    
    print(f"[OK] Created {len(transactions):,} transactions")
    return transactions


def mine_frequent_itemsets(transactions, min_support=0.01):
    """
    Mine frequent itemsets using Apriori algorithm.
    
    PARAMETER DETERMINATION:
    min_support = 0.01 (1%) means an itemset must appear in at least 1% of 
    transactions to be considered frequent. This threshold was chosen to:
    - Find meaningful patterns that occur regularly
    - Avoid noise from rare combinations
    - Balance between too many (low threshold) and too few (high threshold) itemsets
    
    Args:
        transactions: List of transactions
        min_support: Minimum support threshold (default: 0.01 = 1%)
        
    Returns:
        DataFrame with frequent itemsets and their support values
    """
    print(f"\n[*] Mining frequent itemsets using Apriori algorithm...")
    print(f"    Minimum support: {min_support} ({min_support*100}% of transactions)")
    
    # Step 1: Encode transactions into binary matrix
    # Each row = transaction, each column = item (present=1, absent=0)
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
    
    print(f"[OK] Encoded {len(df_encoded):,} transactions")
    print(f"     Found {len(te.columns_)} unique items")
    
    # Step 2: Apply Apriori algorithm
    # Apriori finds all itemsets that meet the minimum support threshold
    frequent_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
    
    if len(frequent_itemsets) > 0:
        frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
        print(f"[OK] Found {len(frequent_itemsets):,} frequent itemsets")
    else:
        print("[WARNING] No frequent itemsets found. Try lowering min_support.")
    
    return frequent_itemsets


def generate_association_rules(frequent_itemsets, min_confidence=0.5):
    """
    Generate association rules from frequent itemsets.
    
    PARAMETER DETERMINATION:
    min_confidence = 0.5 (50%) means a rule must have at least 50% confidence.
    Confidence = P(consequent | antecedent). This threshold ensures:
    - Rules have meaningful predictive power
    - Filters out weak associations
    - Focuses on rules that are likely to hold
    
    METRICS EXPLAINED:
    - Support: Frequency of the itemset (antecedent + consequent) in transactions
    - Confidence: P(consequent | antecedent) = how often consequent appears when antecedent is present
    - Lift: How much more likely consequent is given antecedent vs. overall
      * Lift > 1.0: Positive association (rule is useful)
      * Lift = 1.0: No association (independent)
      * Lift < 1.0: Negative association
    - Conviction: Expected error of the rule (higher = stronger rule)
    
    Args:
        frequent_itemsets: DataFrame with frequent itemsets
        min_confidence: Minimum confidence threshold (default: 0.5 = 50%)
        
    Returns:
        DataFrame with association rules and metrics
    """
    print(f"\n[*] Generating association rules...")
    print(f"    Minimum confidence: {min_confidence} ({min_confidence*100}%)")
    
    if len(frequent_itemsets) == 0:
        print("[WARNING] No frequent itemsets found. Cannot generate rules.")
        return pd.DataFrame()
    
    # Generate rules from frequent itemsets
    # Rules are in format: IF antecedent THEN consequent
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
    
    if len(rules) == 0:
        print("[WARNING] No association rules found. Try lowering min_confidence.")
        return pd.DataFrame()
    
    # Sort by confidence (highest first), then by lift
    rules = rules.sort_values(['confidence', 'lift'], ascending=[False, False])
    
    print(f"[OK] Generated {len(rules):,} association rules")
    print(f"     Rules with lift > 1.0 (positive association): {(rules['lift'] > 1.0).sum()}")
    
    return rules


def format_itemset(itemset):
    """Format itemset for display."""
    if isinstance(itemset, frozenset):
        return ', '.join(sorted(itemset))
    return str(itemset)


def print_top_rules(rules, top_n=20):
    """
    Print top association rules with explanations.
    
    RESULTS EXPLANATION:
    Each rule shows a pattern: IF certain conditions THEN certain outcomes.
    Higher confidence and lift indicate stronger, more useful rules.
    """
    print(f"\n{'='*80}")
    print(f"TOP {min(top_n, len(rules))} ASSOCIATION RULES")
    print(f"{'='*80}\n")
    
    for idx, (_, rule) in enumerate(rules.head(top_n).iterrows(), 1):
        antecedent = format_itemset(rule['antecedents'])
        consequent = format_itemset(rule['consequents'])
        
        print(f"Rule {idx}:")
        print(f"  IF {antecedent}")
        print(f"  THEN {consequent}")
        print(f"  Support: {rule['support']:.4f} ({rule['support']*100:.2f}% of transactions)")
        print(f"  Confidence: {rule['confidence']:.4f} ({rule['confidence']*100:.1f}% probability)")
        print(f"  Lift: {rule['lift']:.4f}", end="")
        if rule['lift'] > 1.0:
            print(" (positive association - rule is useful)")
        elif rule['lift'] < 1.0:
            print(" (negative association)")
        else:
            print(" (no association)")
        print(f"  Conviction: {rule['conviction']:.4f}")
        print()


def save_results(frequent_itemsets, rules, output_file='association_rules_results.txt'):
    """
    Save results to file with detailed explanations.
    
    APPLICATIONS OF RESULTS:
    Association rules can be used for:
    1. Content recommendation: Suggest subreddits based on user behavior
    2. Moderation: Identify patterns that lead to controversial content
    3. User engagement: Understand what makes comments successful (high scores)
    4. Community analysis: Discover relationships between subreddits and comment characteristics
    """
    print(f"\n[*] Saving results to {output_file}...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("ASSOCIATION RULE MINING RESULTS\n")
        f.write("="*80 + "\n\n")
        
        f.write("SUMMARY\n")
        f.write("-"*80 + "\n")
        f.write(f"Frequent Itemsets: {len(frequent_itemsets)}\n")
        f.write(f"Association Rules: {len(rules)}\n")
        if len(rules) > 0:
            f.write(f"Average Confidence: {rules['confidence'].mean():.4f}\n")
            f.write(f"Average Lift: {rules['lift'].mean():.4f}\n")
            f.write(f"Rules with Lift > 1.0: {(rules['lift'] > 1.0).sum()}\n")
        f.write("\n")
        
        f.write("FREQUENT ITEMSETS (Top 50)\n")
        f.write("-"*80 + "\n")
        f.write("Itemsets that appear together frequently in transactions.\n\n")
        
        for idx, (_, itemset) in enumerate(frequent_itemsets.head(50).iterrows(), 1):
            f.write(f"{idx}. {format_itemset(itemset['itemsets'])}\n")
            f.write(f"   Support: {itemset['support']:.4f} ({itemset['support']*100:.2f}%)\n")
            f.write(f"   Length: {itemset['length']} items\n\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("ASSOCIATION RULES\n")
        f.write("="*80 + "\n")
        f.write("Rules showing IF-THEN relationships with metrics:\n")
        f.write("- Support: Frequency of the pattern\n")
        f.write("- Confidence: Probability of consequent given antecedent\n")
        f.write("- Lift: Strength of association (>1.0 = positive, <1.0 = negative)\n")
        f.write("- Conviction: Expected error of the rule\n\n")
        
        for idx, (_, rule) in enumerate(rules.iterrows(), 1):
            antecedent = format_itemset(rule['antecedents'])
            consequent = format_itemset(rule['consequents'])
            
            f.write(f"Rule {idx}:\n")
            f.write(f"  IF {antecedent}\n")
            f.write(f"  THEN {consequent}\n")
            f.write(f"  Support: {rule['support']:.4f} ({rule['support']*100:.2f}%)\n")
            f.write(f"  Confidence: {rule['confidence']:.4f} ({rule['confidence']*100:.1f}%)\n")
            f.write(f"  Lift: {rule['lift']:.4f}")
            if rule['lift'] > 1.0:
                f.write(" (positive association)\n")
            elif rule['lift'] < 1.0:
                f.write(" (negative association)\n")
            else:
                f.write(" (no association)\n")
            f.write(f"  Conviction: {rule['conviction']:.4f}\n\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("APPLICATIONS OF RESULTS\n")
        f.write("="*80 + "\n")
        f.write("These association rules can be applied to:\n")
        f.write("1. Content Recommendation: Suggest subreddits based on user comment patterns\n")
        f.write("2. Moderation: Identify patterns that correlate with controversial content\n")
        f.write("3. User Engagement: Understand factors that lead to high-scoring comments\n")
        f.write("4. Community Analysis: Discover relationships between subreddits and comment characteristics\n")
        f.write("5. Quality Prediction: Predict comment success based on subreddit and characteristics\n")
    
    print(f"[OK] Results saved to {output_file}")


def main():
    """Main function."""
    args = parse_arguments()
    
    print("="*80)
    print("ASSOCIATION RULE MINING FOR REDDIT COMMENTS DATASET")
    print("="*80)
    
    # Load data
    df = load_data(args.input, args.sample)
    
    # Create transactions from comments
    print("\n[*] Creating transactions...")
    transactions = create_transactions(df)
    
    if len(transactions) == 0:
        print("[ERROR] No transactions created. Exiting.")
        sys.exit(1)
    
    # Mine frequent itemsets
    frequent_itemsets = mine_frequent_itemsets(transactions, args.min_support)
    
    if len(frequent_itemsets) == 0:
        print("[ERROR] No frequent itemsets found. Try lowering min_support.")
        sys.exit(1)
    
    # Generate association rules
    rules = generate_association_rules(frequent_itemsets, args.min_confidence)
    
    if len(rules) == 0:
        print("[ERROR] No association rules found. Try lowering min_confidence.")
        sys.exit(1)
    
    # Display results
    print_top_rules(rules, top_n=20)
    
    # Save results
    save_results(frequent_itemsets, rules)
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    print(f"Total transactions: {len(transactions):,}")
    print(f"Frequent itemsets: {len(frequent_itemsets):,}")
    print(f"Association rules: {len(rules):,}")
    if len(rules) > 0:
        print(f"Average confidence: {rules['confidence'].mean():.4f}")
        print(f"Average lift: {rules['lift'].mean():.4f}")
        print(f"Rules with lift > 1.0 (positive association): {(rules['lift'] > 1.0).sum()}")
    print("="*80)


if __name__ == "__main__":
    main()

